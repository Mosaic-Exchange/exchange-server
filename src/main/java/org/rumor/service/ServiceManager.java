package org.rumor.service;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.rumor.gossip.EndpointState;
import org.rumor.gossip.GossipService;
import org.rumor.gossip.NodeId;
import org.rumor.gossip.VersionedValue;
import org.rumor.transport.ConnectionManager;
import org.rumor.transport.MessageType;
import org.rumor.transport.RumorFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Manages service registration, request routing, node picking, streaming
 * handshake, and periodic state publishing for services annotated with
 * {@link MaintainState} and {@link StateKey}.
 *
 * <p>Handles two service modes:
 * <ul>
 *   <li>Default — request/response on the I/O thread, single-write</li>
 *   <li>{@link Streamable} — streaming on a dedicated thread, multi-write,
 *       at most one active stream per node</li>
 * </ul>
 *
 * <p>Also implements {@link ClusterView} so that services can query
 * cluster state without touching gossip internals.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ServiceManager implements ClusterView {

    private static final Logger log = LoggerFactory.getLogger(ServiceManager.class);
    private static final long STATE_PUBLISH_INTERVAL_MS = 2000;
    private static final long STREAM_HANDSHAKE_TIMEOUT_MS = 10_000;

    private final ConnectionManager connectionManager;
    private final GossipService gossipService;
    private final NodeId localId;

    private final Map<String, DistributedService> services = new ConcurrentHashMap<>();

    private final AtomicInteger requestIdGenerator = new AtomicInteger(1);
    private final ScheduledExecutorService timeoutScheduler;

    private volatile Consumer<Set<String>> onRegistrationChanged;

    private final long requestTimeoutMs;
    private final long requestIdleTimeoutMs;

    // Client-side: tracks pending outbound requests
    private final Map<Integer, PendingRequest> pendingRequests = new ConcurrentHashMap<>();

    // Server-side: streaming
    private final Map<Integer, PendingStream> pendingStreams = new ConcurrentHashMap<>();
    private final Map<Integer, Future<?>> activeStreams = new ConcurrentHashMap<>();
    // Server-side: handles for cooperative cancellation
    private final Map<Integer, ServiceHandle> serverSideHandles = new ConcurrentHashMap<>();
    private final ExecutorService streamExecutor;

    // State publishers scheduled for periodic updates
    private final Map<String, List<ScheduledFuture<?>>> publisherFutures = new ConcurrentHashMap<>();

    public ServiceManager(ConnectionManager connectionManager, GossipService gossipService,
                          NodeId localId,
                          long requestTimeoutMs, long requestIdleTimeoutMs) {
        this.connectionManager = connectionManager;
        this.gossipService = gossipService;
        this.localId = localId;
        this.requestTimeoutMs = requestTimeoutMs;
        this.requestIdleTimeoutMs = requestIdleTimeoutMs;
        this.timeoutScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "request-timeout");
            t.setDaemon(true);
            return t;
        });
        this.streamExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "stream-worker");
            t.setDaemon(true);
            return t;
        });
    }

    public void setOnRegistrationChanged(Consumer<Set<String>> callback) {
        this.onRegistrationChanged = callback;
    }

    // Global concurrency config

    private ThreadPoolExecutor globalRemoteExecutor;
    private ThreadPoolExecutor globalLocalExecutor;

    /**
     * Sets shared executor pools applied to all services without a per-service config.
     * Must be called before {@link #register(DistributedService)}.
     */
    public void setGlobalServiceConfig(DistributedService.Config config) {
        this.globalRemoteExecutor = buildGlobalExecutor("global-remote",
                config.remoteThreads(), config.remoteQueueCapacity());
        this.globalLocalExecutor  = buildGlobalExecutor("global-local",
                config.localThreads(), config.localQueueCapacity());
    }

    private ThreadPoolExecutor buildGlobalExecutor(String label, int threads, int queueCapacity) {
        AtomicInteger counter = new AtomicInteger(1);
        BlockingQueue<Runnable> queue = queueCapacity > 0
                ? new ArrayBlockingQueue<>(queueCapacity)
                : new SynchronousQueue<>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                threads, threads,
                60L, TimeUnit.SECONDS,
                queue,
                r -> {
                    Thread t = new Thread(r, label + "-" + counter.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.AbortPolicy()
        );
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    // Registration

    public void register(DistributedService service) {
        if (globalRemoteExecutor != null && !service.hasExecutors()) {
            service.setSharedExecutors(globalRemoteExecutor, globalLocalExecutor);
        }
        service.resolveCodecs();
        String name = service.serviceName();
        services.put(name, service);
        service.setManager(this);
        log.info("Registered {} service: {}", service.isStreamable() ? "streaming" : "request/response", name);

        if (service.getClass().isAnnotationPresent(MaintainState.class)) {
            scheduleStateKeyMethods(service);
        }
        fireRegistrationChanged();
    }

    /**
     * Registers a service with a per-service concurrency config.
     * Takes precedence over any global config set via {@link #setGlobalServiceConfig}.
     */
    public void register(DistributedService service, DistributedService.Config config) {
        service.initLocalExecutors(config);
        register(service);
    }

    public Set<String> getRegisteredNames() {
        return Collections.unmodifiableSet(new HashSet<>(services.keySet()));
    }

    /**
     * Returns only the names of services that are not in private mode.
     * This is what gets published to gossip so peers can discover available services.
     */
    public Set<String> getPublicServiceNames() {
        Set<String> publicNames = new HashSet<>();
        for (var entry : services.entrySet()) {
            if (!entry.getValue().isPrivate()) {
                publicNames.add(entry.getKey());
            }
        }
        return Collections.unmodifiableSet(publicNames);
    }

    /**
     * Re-publishes the set of public services to gossip.
     * Called by {@link DistributedService} when private/public mode changes.
     */
    public void republishServices() {
        fireRegistrationChanged();
    }

    private void fireRegistrationChanged() {
        if (onRegistrationChanged != null) {
            onRegistrationChanged.accept(getPublicServiceNames());
        }
    }

    // ClusterView implementation

    @Override
    public Map<NodeId, String> stateForKey(String key) {
        Map<NodeId, String> result = new HashMap<>();
        for (var entry : gossipService.getEndpointStates().entrySet()) {
            NodeId id = entry.getKey();
            if (id.equals(localId)) continue;

            EndpointState state = entry.getValue();
            VersionedValue statusVv = state.getAppState("STATUS");
            if (statusVv == null || !"ALIVE".equals(statusVv.value())) continue;

            VersionedValue vv = state.getAppState(key);
            if (vv != null && !vv.value().isEmpty()) {
                result.put(id, vv.value());
            }
        }
        return result;
    }

    @Override
    public List<NodeId> findPeers(String key, Predicate<String> match) {
        List<NodeId> result = new ArrayList<>();
        for (var entry : stateForKey(key).entrySet()) {
            if (match.test(entry.getValue())) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    @Override
    public NodeId localId() {
        return localId;
    }

    //  Client-side: sending requests

    /**
     * Send a request to a remote peer offering the named service.
     * Works for both default and {@link Streamable} services —
     * the server decides the protocol after receiving SERVICE_REQUEST.
     *
     * <p>The callback operates on raw {@code byte[]} events — the calling
     * {@link DistributedService} wraps it to deserialize typed responses.
     */
    public void sendRequest(String serviceName, byte[] request, OnStateChange<byte[]> onStateChange,
                            Predicate<Map<String, String>> peerFilter, ServiceHandle handle) {
        NodeId target = pickNode(serviceName, peerFilter);
        if (target == null) {
            onStateChange.accept(new RequestEvent.Failed<>("No live node found offering service '" + serviceName + "'"));
            log.warn("No live node found offering service '{}'", serviceName);
            return;
        }

        sendRequestTo(target, serviceName, request, onStateChange, handle);
    }

    /**
     * Send a request directly to a specific node, bypassing peer discovery.
     * Used for targeted dispatch when the caller knows exactly which node to use.
     */
    public void sendRequestToNode(String serviceName, byte[] request, NodeId targetNode,
                                   OnStateChange<byte[]> onStateChange, ServiceHandle handle) {
        sendRequestTo(targetNode, serviceName, request, onStateChange, handle);
    }

    private void sendRequestTo(NodeId target, String serviceName, byte[] request,
                                OnStateChange<byte[]> onStateChange, ServiceHandle handle) {
        try {
            Channel channel = connectionManager.getChannel(target);
            if (channel == null) {
                channel = connectionManager.connect(target);
            }

            int requestId = requestIdGenerator.getAndIncrement();
            PendingRequest pending = new PendingRequest(requestId, onStateChange, channel);
            pendingRequests.put(requestId, pending);

            pending.overallTimeout = timeoutScheduler.schedule(
                    () -> timeoutRequest(requestId, "Request timed out (exceeded " + requestTimeoutMs + "ms)"),
                    requestTimeoutMs, TimeUnit.MILLISECONDS);

            pending.resetIdleTimeout(timeoutScheduler, requestIdleTimeoutMs,
                    () -> timeoutRequest(requestId, "Idle timeout (no data received within " + requestIdleTimeoutMs + "ms)"));

            if (handle != null) {
                handle.onCancel(() -> cancelByHandle(requestId));
            }

            // Build SERVICE_REQUEST payload:
            //   [requestId: 4B][serviceNameLen: 2B][serviceName: NB][requestPayload: NB]
            byte[] nameBytes = serviceName.getBytes(StandardCharsets.UTF_8);
            byte[] payload = new byte[4 + 2 + nameBytes.length + request.length];
            ByteBuffer buf = ByteBuffer.wrap(payload);
            buf.putInt(requestId);
            buf.putShort((short) nameBytes.length);
            buf.put(nameBytes);
            buf.put(request);

            channel.writeAndFlush(new RumorFrame(MessageType.SERVICE_REQUEST, payload));
            onStateChange.accept(new RequestEvent.Processing<>());
            log.debug("Sent request {} for service '{}' to {}", requestId, serviceName, target);

        } catch (Exception e) {
            onStateChange.accept(new RequestEvent.Failed<>("Failed to send request: " + e.getMessage()));
            log.error("Failed to send request for service '{}' to {}", serviceName, target, e);
        }
    }

    private void timeoutRequest(int requestId, String reason) {
        PendingRequest pending = pendingRequests.remove(requestId);
        if (pending != null) {
            pending.cancelTimeouts();
            pending.onStateChange.accept(new RequestEvent.Failed<>(reason));
            log.warn("Request {} timed out: {}", requestId, reason);

            // Notify the server so it can stop work
            if (pending.channel.isActive()) {
                byte[] cancelPayload = new byte[4];
                ByteBuffer.wrap(cancelPayload).putInt(requestId);
                pending.channel.writeAndFlush(new RumorFrame(MessageType.SERVICE_CANCEL, cancelPayload));
            }
        }
    }

    /**
     * Cancels a pending request initiated by the caller via {@link ServiceHandle#cancel()}.
     * Removes the pending request, cancels timeouts, notifies the caller with a
     * {@link RequestEvent.Cancelled} event, and sends SERVICE_CANCEL to the server.
     */
    private void cancelByHandle(int requestId) {
        PendingRequest pending = pendingRequests.remove(requestId);
        if (pending != null) {
            pending.cancelTimeouts();
            pending.onStateChange.accept(new RequestEvent.Cancelled<>());
            log.info("Request {} cancelled by caller", requestId);

            if (pending.channel.isActive()) {
                byte[] cancelPayload = new byte[4];
                ByteBuffer.wrap(cancelPayload).putInt(requestId);
                pending.channel.writeAndFlush(new RumorFrame(MessageType.SERVICE_CANCEL, cancelPayload));
            }
        }
    }

    /**
     * Sends SERVICE_CANCEL for a request we no longer track — tells the server
     * to stop any work associated with this request id.
     */
    private void sendCancel(ChannelHandlerContext ctx, int requestId) {
        byte[] cancelPayload = new byte[4];
        ByteBuffer.wrap(cancelPayload).putInt(requestId);
        ctx.writeAndFlush(new RumorFrame(MessageType.SERVICE_CANCEL, cancelPayload));
    }

    //  Client-side: incoming response handlers

    /** Handles SERVICE_RESPONSE — single complete response for an RService request. */
    public void handleServiceResponse(ChannelHandlerContext ctx, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int requestId = buf.getInt();
        byte[] data = new byte[buf.remaining()];
        buf.get(data);

        PendingRequest pending = pendingRequests.remove(requestId);
        if (pending != null) {
            pending.cancelTimeouts();
            pending.onStateChange.accept(new RequestEvent.Succeeded<>(data));
            log.debug("Request {} completed with {} bytes", requestId, data.length);
        } else {
            log.warn("Received response for unknown request {}", requestId);
        }
    }

    /** Handles SERVICE_ERROR — error response for an RService request. */
    public void handleServiceError(ChannelHandlerContext ctx, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int requestId = buf.getInt();
        byte[] errorBytes = new byte[buf.remaining()];
        buf.get(errorBytes);
        String reason = new String(errorBytes, StandardCharsets.UTF_8);

        PendingRequest pending = pendingRequests.remove(requestId);
        if (pending != null) {
            pending.cancelTimeouts();
            pending.onStateChange.accept(new RequestEvent.Failed<>(reason));
            log.warn("Request {} failed remotely: {}", requestId, reason);
        } else {
            log.warn("Received error for unknown request {}", requestId);
        }
    }

    /**
     * Handles SERVICE_INIT_STREAM — server indicates this is a streaming response.
     * The client sends SERVICE_STREAM_START back to acknowledge readiness.
     */
    public void handleServiceInitStream(ChannelHandlerContext ctx, byte[] payload) {
        int requestId = ByteBuffer.wrap(payload).getInt();

        PendingRequest pending = pendingRequests.get(requestId);
        if (pending != null) {
            pending.streaming = true;
            // Cancel idle timeout during handshake — rely only on the overall
            // request timeout until the first STREAM_DATA actually arrives.
            if (pending.idleTimeout != null) {
                pending.idleTimeout.cancel(false);
                pending.idleTimeout = null;
            }

            // Acknowledge: ready to receive stream data
            byte[] startPayload = new byte[4];
            ByteBuffer.wrap(startPayload).putInt(requestId);
            ctx.writeAndFlush(new RumorFrame(MessageType.SERVICE_STREAM_START, startPayload));
            log.debug("Streaming initiated for request {}, sent STREAM_START", requestId);
        } else {
            log.warn("Received INIT_STREAM for unknown request {}, sending cancel", requestId);
            sendCancel(ctx, requestId);
        }
    }

    /** Handles SERVICE_STREAM_DATA — a chunk of streamed response data. */
    public void handleServiceStreamData(ChannelHandlerContext ctx, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int requestId = buf.getInt();
        byte[] data = new byte[buf.remaining()];
        buf.get(data);

        PendingRequest pending = pendingRequests.get(requestId);
        if (pending != null) {
            if (pending.overallTimeout != null) {
                pending.overallTimeout.cancel(false);
                pending.overallTimeout = null;
            }
            pending.resetIdleTimeout(timeoutScheduler, requestIdleTimeoutMs,
                    () -> timeoutRequest(requestId, "Idle timeout (no stream data received within " + requestIdleTimeoutMs + "ms)"));
            pending.onStateChange.accept(new RequestEvent.StreamData<>(data));
        } else {
            log.warn("Received stream data for unknown request {}, sending cancel", requestId);
            sendCancel(ctx, requestId);
        }
    }

    /** Handles SERVICE_STREAM_END — streaming completed successfully. */
    public void handleServiceStreamEnd(ChannelHandlerContext ctx, byte[] payload) {
        int requestId = ByteBuffer.wrap(payload).getInt();

        PendingRequest pending = pendingRequests.remove(requestId);
        if (pending != null) {
            pending.cancelTimeouts();
            pending.onStateChange.accept(new RequestEvent.Succeeded<>(null));
            log.debug("Stream {} completed successfully", requestId);
        } else {
            log.warn("Received stream end for unknown request {}", requestId);
        }
    }

    /** Handles SERVICE_STREAM_ERROR — streaming failed. */
    public void handleServiceStreamError(ChannelHandlerContext ctx, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int requestId = buf.getInt();
        byte[] errorBytes = new byte[buf.remaining()];
        buf.get(errorBytes);
        String reason = new String(errorBytes, StandardCharsets.UTF_8);

        PendingRequest pending = pendingRequests.remove(requestId);
        if (pending != null) {
            pending.cancelTimeouts();
            pending.onStateChange.accept(new RequestEvent.Failed<>(reason));
            log.warn("Stream {} failed remotely: {}", requestId, reason);
        } else {
            log.warn("Received stream error for unknown request {}", requestId);
            sendCancel(ctx, requestId);
        }
    }

    //  Server-side: cancellation

    /**
     * Handles SERVICE_CANCEL — client no longer needs the response.
     * Cancels the server-side handle for cooperative cancellation, then
     * cancels any pending handshake or active stream for the given request.
     */
    public void handleServiceCancel(ChannelHandlerContext ctx, byte[] payload) {
        int requestId = ByteBuffer.wrap(payload).getInt();

        // Cooperative cancellation — signals isCancelled() in write() checks
        ServiceHandle serverHandle = serverSideHandles.remove(requestId);
        if (serverHandle != null) serverHandle.cancel();

        // Cancel pending handshake if still waiting
        PendingStream pendingStream = pendingStreams.remove(requestId);
        if (pendingStream != null) {
            if (pendingStream.handshakeTimeout != null) {
                pendingStream.handshakeTimeout.cancel(false);
            }
            log.info("Cancelled pending stream handshake for request {}", requestId);
            return;
        }

        // Interrupt active stream thread
        Future<?> future = activeStreams.remove(requestId);
        if (future != null) {
            future.cancel(true);
            log.info("Cancelled active stream for request {}", requestId);
            return;
        }

        log.debug("Received cancel for unknown request {} (may have already completed)", requestId);
    }

    //  Server-side: incoming request handlers

    /** Handles SERVICE_REQUEST — dispatches to RService or initiates streaming handshake. */
    public void handleServiceRequest(ChannelHandlerContext ctx, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int requestId = buf.getInt();

        short nameLen = buf.getShort();
        byte[] nameBytes = new byte[nameLen];
        buf.get(nameBytes);
        String serviceName = new String(nameBytes, StandardCharsets.UTF_8);

        byte[] requestData = new byte[buf.remaining()];
        buf.get(requestData);

        log.info("Received SERVICE_REQUEST id={} service='{}' from={} ({} bytes)",
                requestId, serviceName, ctx.channel().remoteAddress(), requestData.length);

        DistributedService service = services.get(serviceName);
        if (service == null) {
            log.warn("No service registered for '{}', ignoring request {}", serviceName, requestId);
            sendError(ctx, requestId, "No service registered for '" + serviceName + "'");
            return;
        }

        if (!service.isStreamable()) {
            // Request/response: serve on the I/O thread (or remote executor), single-write
            ServiceHandle serverHandle = new ServiceHandle();
            serverSideHandles.put(requestId, serverHandle);
            ServiceRequest serviceRequest = new ServiceRequest(requestData, serverHandle, service.requestCodec);
            RemoteServiceResponse response = new RemoteServiceResponse(requestId, ctx.channel(), serverHandle, service.responseCodec);
            try {
                service.executeServe(serviceRequest, response);
                response.close();
            } catch (java.util.concurrent.CancellationException e) {
                log.info("Non-streaming request {} was cancelled", requestId);
            } catch (Exception e) {
                log.error("Error serving request {} (service='{}')", requestId, serviceName, e);
                String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                response.fail(msg.getBytes(StandardCharsets.UTF_8));
            } finally {
                serverSideHandles.remove(requestId);
            }
            return;
        }

        // Streaming: handshake then serve on dedicated thread
        ServiceHandle serverHandle = new ServiceHandle();
        serverSideHandles.put(requestId, serverHandle);
        PendingStream pending = new PendingStream(requestId, ctx, service, requestData, serverHandle);
        pendingStreams.put(requestId, pending);

        pending.handshakeTimeout = timeoutScheduler.schedule(() -> {
            PendingStream removed = pendingStreams.remove(requestId);
            if (removed != null) {
                serverSideHandles.remove(requestId);
                log.warn("Stream handshake timed out for request {}", requestId);
                sendError(ctx, requestId, "Stream handshake timed out");
            }
        }, STREAM_HANDSHAKE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        byte[] initPayload = new byte[4];
        ByteBuffer.wrap(initPayload).putInt(requestId);
        ctx.writeAndFlush(new RumorFrame(MessageType.SERVICE_INIT_STREAM, initPayload));
        log.debug("Sent INIT_STREAM for request {} (service='{}')", requestId, serviceName);
    }

    /**
     * Handles SERVICE_STREAM_START — client is ready, start streaming on a
     * dedicated thread.
     */
    public void handleServiceStreamStart(ChannelHandlerContext ctx, byte[] payload) {
        int requestId = ByteBuffer.wrap(payload).getInt();

        PendingStream pending = pendingStreams.remove(requestId);
        if (pending == null) {
            log.warn("Received STREAM_START for unknown stream {}", requestId);
            return;
        }

        if (pending.handshakeTimeout != null) {
            pending.handshakeTimeout.cancel(false);
        }

        log.debug("Client ready for stream {}, starting serve() on stream-worker", requestId);

        ServiceRequest serviceRequest = new ServiceRequest(pending.requestData, pending.handle, pending.service.requestCodec);
        RemoteStreamingServiceResponse response = new RemoteStreamingServiceResponse(
                requestId, pending.ctx.channel(), pending.handle, pending.service.responseCodec);

        // Register BEFORE executing: if submit() is used instead, a fast-finishing task
        // could run its finally-block (remove) before activeStreams.put(), leaving a
        // zombie entry that permanently inflates activeStreamsServer.
        FutureTask<Void> task = new FutureTask<>(() -> {
            try {
                pending.service.executeServe(serviceRequest, response);
                response.close();
            } catch (java.util.concurrent.CancellationException e) {
                log.info("Streaming request {} was cancelled", requestId);
            } catch (Exception e) {
                log.error("Error in streaming service for request {}", requestId, e);
                String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                response.fail(msg.getBytes(StandardCharsets.UTF_8));
            } finally {
                activeStreams.remove(requestId);
                serverSideHandles.remove(requestId);
            }
            return null;
        });
        activeStreams.put(requestId, task);
        try {
            streamExecutor.execute(task);
        } catch (RejectedExecutionException e) {
            activeStreams.remove(requestId);
            serverSideHandles.remove(requestId);
            log.error("Failed to start stream execution for request {}", requestId, e);
            sendError(ctx, requestId, "Server capacity exceeded for streams");
        }
    }

    //  Node picking

    private NodeId pickNode(String serviceName, Predicate<Map<String, String>> peerFilter) {
        List<NodeId> candidates = new ArrayList<>();

        for (var entry : gossipService.getEndpointStates().entrySet()) {
            NodeId id = entry.getKey();
            if (id.equals(localId)) continue;

            EndpointState state = entry.getValue();
            VersionedValue statusVv = state.getAppState("STATUS");
            if (statusVv == null || !"ALIVE".equals(statusVv.value())) continue;

            VersionedValue servicesVv = state.getAppState("SERVICES");
            if (servicesVv == null || servicesVv.value().isEmpty()) continue;

            if (!Set.of(servicesVv.value().split(",")).contains(serviceName)) continue;

            if (peerFilter != null) {
                Map<String, String> appState = toPlainMap(state);
                if (!peerFilter.test(appState)) continue;
            }

            candidates.add(id);
        }

        if (candidates.isEmpty()) return null;
        Collections.shuffle(candidates);
        return candidates.getFirst();
    }

    //  State publishing

    private void scheduleStateKeyMethods(DistributedService service) {
        String name = service.serviceName();
        List<ScheduledFuture<?>> existing = publisherFutures.remove(name);
        if (existing != null) {
            for (ScheduledFuture<?> f : existing) f.cancel(false);
        }

        List<ScheduledFuture<?>> newFutures = new ArrayList<>();
        String prefix = name + ".";
        for (Method method : service.getClass().getMethods()) {
            StateKey ann = method.getAnnotation(StateKey.class);
            if (ann == null) continue;

            String qualifiedKey = prefix + ann.value();
            ScheduledFuture<?> future = timeoutScheduler.scheduleAtFixedRate(() -> {
                try {
                    String value = (String) method.invoke(service);
                    gossipService.setLocalState(qualifiedKey, value);
                } catch (Exception e) {
                    log.error("Error computing state for key '{}'", qualifiedKey, e);
                }
            }, 0, STATE_PUBLISH_INTERVAL_MS, TimeUnit.MILLISECONDS);
            newFutures.add(future);
        }
        publisherFutures.put(name, newFutures);
    }

    //  Helpers

    private void sendError(ChannelHandlerContext ctx, int requestId, String message) {
        byte[] msgBytes = message.getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[4 + msgBytes.length];
        ByteBuffer.wrap(payload).putInt(requestId);
        System.arraycopy(msgBytes, 0, payload, 4, msgBytes.length);
        ctx.writeAndFlush(new RumorFrame(MessageType.SERVICE_ERROR, payload));
    }

    private static Map<String, String> toPlainMap(EndpointState state) {
        Map<String, String> map = new HashMap<>();
        for (var entry : state.appStates().entrySet()) {
            map.put(entry.getKey(), entry.getValue().value());
        }
        return map;
    }

    public void shutdown() {
        for (List<ScheduledFuture<?>> list : publisherFutures.values()) {
            for (ScheduledFuture<?> f : list) f.cancel(false);
        }
        publisherFutures.clear();
        timeoutScheduler.shutdown();
        streamExecutor.shutdown();

        for (PendingRequest pending : pendingRequests.values()) {
            pending.cancelTimeouts();
        }
        pendingRequests.clear();

        for (PendingStream pending : pendingStreams.values()) {
            if (pending.handshakeTimeout != null) pending.handshakeTimeout.cancel(false);
        }
        pendingStreams.clear();

        for (DistributedService service : services.values()) {
            service.shutdownExecutors();
        }
    }

    //  Inner classes

    //  Debug snapshot

    public record ExecutorSnapshot(int activeThreads, int poolSize, int maxPoolSize,
                                   int queueSize, int queueCapacity, long completedTasks) {}

    public record ExecutorPairSnapshot(ExecutorSnapshot remote, ExecutorSnapshot local) {}

    public record PendingRequestDetail(int requestId, boolean streaming, long elapsedMs) {}

    public record DebugSnapshot(int pendingOutbound, int activeStreamsServer, int pendingHandshakes,
                                List<PendingRequestDetail> pendingDetails,
                                Map<String, ExecutorPairSnapshot> executorStats) {}

    private static ExecutorSnapshot snap(java.util.concurrent.ThreadPoolExecutor ex) {
        BlockingQueue<Runnable> q = ex.getQueue();
        // size() + remainingCapacity() == total capacity for ArrayBlockingQueue;
        // both return 0 for SynchronousQueue, correctly representing fail-fast (no buffering).
        int capacity = q.size() + q.remainingCapacity();
        return new ExecutorSnapshot(ex.getActiveCount(), ex.getPoolSize(),
                ex.getMaximumPoolSize(), q.size(), capacity, ex.getCompletedTaskCount());
    }

    public DebugSnapshot debugSnapshot() {
        List<PendingRequestDetail> details = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (var pr : pendingRequests.values()) {
            details.add(new PendingRequestDetail(pr.requestId, pr.streaming,
                    now - pr.createdAt));
        }

        Map<String, ExecutorPairSnapshot> executorStats = new HashMap<>();
        // Show the shared global pool once under "(global)" rather than once per service.
        if (globalRemoteExecutor != null) {
            executorStats.put("(global)", new ExecutorPairSnapshot(
                    snap(globalRemoteExecutor), snap(globalLocalExecutor)));
        }
        // Only include services that own their own dedicated executor pools.
        for (var entry : services.entrySet()) {
            DistributedService svc = entry.getValue();
            if (svc.hasExecutors() && svc.ownsExecutors()) {
                executorStats.put(entry.getKey(),
                        new ExecutorPairSnapshot(snap(svc.remoteExecutor()), snap(svc.localExecutor())));
            }
        }

        return new DebugSnapshot(
                pendingRequests.size(),
                activeStreams.size(),
                pendingStreams.size(),
                details,
                executorStats
        );
    }

    /** Tracks a pending outbound request (client side). */
    private static class PendingRequest {
        final int requestId;
        final OnStateChange<byte[]> onStateChange;
        final Channel channel;
        final long createdAt;
        volatile ScheduledFuture<?> overallTimeout;
        volatile ScheduledFuture<?> idleTimeout;
        volatile boolean streaming;

        PendingRequest(int requestId, OnStateChange<byte[]> onStateChange, Channel channel) {
            this.requestId = requestId;
            this.onStateChange = onStateChange;
            this.channel = channel;
            this.createdAt = System.currentTimeMillis();
        }

        void resetIdleTimeout(ScheduledExecutorService scheduler, long idleMs, Runnable onTimeout) {
            ScheduledFuture<?> prev = this.idleTimeout;
            if (prev != null) prev.cancel(false);
            this.idleTimeout = scheduler.schedule(onTimeout, idleMs, TimeUnit.MILLISECONDS);
        }

        void cancelTimeouts() {
            if (overallTimeout != null) overallTimeout.cancel(false);
            if (idleTimeout != null) idleTimeout.cancel(false);
        }
    }

    /** Tracks a pending streaming handshake (server side). */
    private static class PendingStream {
        final int requestId;
        final ChannelHandlerContext ctx;
        final DistributedService service;
        final byte[] requestData;
        final ServiceHandle handle;
        volatile ScheduledFuture<?> handshakeTimeout;

        PendingStream(int requestId, ChannelHandlerContext ctx,
                      DistributedService service, byte[] requestData, ServiceHandle handle) {
            this.requestId = requestId;
            this.ctx = ctx;
            this.service = service;
            this.requestData = requestData;
            this.handle = handle;
        }
    }
}
