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
public class ServiceManager implements ClusterView {

    private static final Logger log = LoggerFactory.getLogger(ServiceManager.class);
    private static final long STATE_PUBLISH_INTERVAL_MS = 2000;
    private static final long STREAM_HANDSHAKE_TIMEOUT_MS = 10_000;

    private final ConnectionManager connectionManager;
    private final GossipService gossipService;
    private final NodeId localId;

    private final Map<String, RService> services = new ConcurrentHashMap<>();

    private final AtomicInteger requestIdGenerator = new AtomicInteger(1);
    private final ScheduledExecutorService timeoutScheduler;

    private volatile Consumer<Set<String>> onRegistrationChanged;

    private final long requestTimeoutMs;
    private final long requestIdleTimeoutMs;

    // --- Client-side: tracks pending outbound requests ---
    private final Map<Integer, PendingRequest> pendingRequests = new ConcurrentHashMap<>();

    // --- Server-side: streaming ---
    private final Map<Integer, PendingStream> pendingStreams = new ConcurrentHashMap<>();
    private final Map<Integer, Future<?>> activeStreams = new ConcurrentHashMap<>();
    private final ExecutorService streamExecutor;

    // State publishers scheduled for periodic updates
    private final List<ScheduledFuture<?>> publisherFutures = new ArrayList<>();

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

    // --- Registration ---

    public void register(RService service) {
        String name = service.serviceName();
        services.put(name, service);
        service.setManager(this);
        log.info("Registered {} service: {}", service.isStreamable() ? "streaming" : "request/response", name);

        if (service.getClass().isAnnotationPresent(MaintainState.class)) {
            scheduleStateKeyMethods(service);
        }
        fireRegistrationChanged();
    }

    public Set<String> getRegisteredNames() {
        return Collections.unmodifiableSet(new HashSet<>(services.keySet()));
    }

    private void fireRegistrationChanged() {
        if (onRegistrationChanged != null) {
            onRegistrationChanged.accept(getRegisteredNames());
        }
    }

    // --- ClusterView implementation ---

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

    // ====================================================================
    //  Client-side: sending requests
    // ====================================================================

    /**
     * Send a request to a remote peer offering the named service.
     * Works for both default and {@link Streamable} services —
     * the server decides the protocol after receiving SERVICE_REQUEST.
     */
    public void sendRequest(String serviceName, byte[] request, OnStateChange onStateChange,
                            Predicate<Map<String, String>> peerFilter, ServiceHandle handle) {
        NodeId target = pickNode(serviceName, peerFilter);
        if (target == null) {
            onStateChange.accept(new RequestEvent.Failed("No live node found offering service '" + serviceName + "'"));
            log.warn("No live node found offering service '{}'", serviceName);
            return;
        }

        sendRequestTo(target, serviceName, request, onStateChange, handle);
    }

    private void sendRequestTo(NodeId target, String serviceName, byte[] request,
                                OnStateChange onStateChange, ServiceHandle handle) {
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
            onStateChange.accept(new RequestEvent.Processing());
            log.debug("Sent request {} for service '{}' to {}", requestId, serviceName, target);

        } catch (Exception e) {
            onStateChange.accept(new RequestEvent.Failed("Failed to send request: " + e.getMessage()));
            log.error("Failed to send request for service '{}' to {}", serviceName, target, e);
        }
    }

    private void timeoutRequest(int requestId, String reason) {
        PendingRequest pending = pendingRequests.remove(requestId);
        if (pending != null) {
            pending.cancelTimeouts();
            pending.onStateChange.accept(new RequestEvent.Failed(reason));
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
     * {@link RequestEvent.Failed} event, and sends SERVICE_CANCEL to the server.
     */
    private void cancelByHandle(int requestId) {
        PendingRequest pending = pendingRequests.remove(requestId);
        if (pending != null) {
            pending.cancelTimeouts();
            pending.onStateChange.accept(new RequestEvent.Failed("Cancelled"));
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

    // ====================================================================
    //  Client-side: incoming response handlers
    // ====================================================================

    /** Handles SERVICE_RESPONSE — single complete response for an RService request. */
    public void handleServiceResponse(ChannelHandlerContext ctx, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int requestId = buf.getInt();
        byte[] data = new byte[buf.remaining()];
        buf.get(data);

        PendingRequest pending = pendingRequests.remove(requestId);
        if (pending != null) {
            pending.cancelTimeouts();
            pending.onStateChange.accept(new RequestEvent.Succeeded(data));
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
            pending.onStateChange.accept(new RequestEvent.Failed(reason));
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
            pending.resetIdleTimeout(timeoutScheduler, requestIdleTimeoutMs,
                    () -> timeoutRequest(requestId, "Idle timeout (no stream data received within " + requestIdleTimeoutMs + "ms)"));
            pending.onStateChange.accept(new RequestEvent.StreamData(data));
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
            pending.onStateChange.accept(new RequestEvent.Succeeded(null));
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
            pending.onStateChange.accept(new RequestEvent.Failed(reason));
            log.warn("Stream {} failed remotely: {}", requestId, reason);
        } else {
            log.warn("Received stream error for unknown request {}", requestId);
            sendCancel(ctx, requestId);
        }
    }

    // ====================================================================
    //  Server-side: cancellation
    // ====================================================================

    /**
     * Handles SERVICE_CANCEL — client no longer needs the response.
     * Cancels any pending handshake or active stream for the given request.
     */
    public void handleServiceCancel(ChannelHandlerContext ctx, byte[] payload) {
        int requestId = ByteBuffer.wrap(payload).getInt();

        // Cancel pending handshake if still waiting
        PendingStream pendingStream = pendingStreams.remove(requestId);
        if (pendingStream != null) {
            if (pendingStream.handshakeTimeout != null) {
                pendingStream.handshakeTimeout.cancel(false);
            }
            log.info("Cancelled pending stream handshake for request {}", requestId);
            return;
        }

        // Cancel active stream if running
        Future<?> future = activeStreams.remove(requestId);
        if (future != null) {
            future.cancel(true);
            log.info("Cancelled active stream for request {}", requestId);
            return;
        }

        log.debug("Received cancel for unknown request {} (may have already completed)", requestId);
    }

    // ====================================================================
    //  Server-side: incoming request handlers
    // ====================================================================

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

        RService service = services.get(serviceName);
        if (service == null) {
            log.warn("No service registered for '{}', ignoring request {}", serviceName, requestId);
            sendError(ctx, requestId, "No service registered for '" + serviceName + "'");
            return;
        }

        if (!service.isStreamable()) {
            // Request/response: serve on the I/O thread, single-write
            RemoteServiceResponse response = new RemoteServiceResponse(requestId, ctx.channel());
            try {
                service.serve(requestData, response);
                response.close();
            } catch (Exception e) {
                log.error("Error serving request {} (service='{}')", requestId, serviceName, e);
                String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                response.fail(msg.getBytes(StandardCharsets.UTF_8));
            }
            return;
        }

        // Streaming: handshake then serve on dedicated thread
        PendingStream pending = new PendingStream(requestId, ctx, service, requestData);
        pendingStreams.put(requestId, pending);

        pending.handshakeTimeout = timeoutScheduler.schedule(() -> {
            PendingStream removed = pendingStreams.remove(requestId);
            if (removed != null) {
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

        Future<?> future = streamExecutor.submit(() -> {
            RemoteStreamingServiceResponse response = new RemoteStreamingServiceResponse(
                    requestId, pending.ctx.channel(), () -> {
                        activeStreams.remove(requestId);
                    });
            try {
                pending.service.serve(pending.requestData, response);
                response.close();
            } catch (Exception e) {
                log.error("Error in streaming service for request {}", requestId, e);
                String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                response.fail(msg.getBytes(StandardCharsets.UTF_8));
            }
        });
        activeStreams.put(requestId, future);
    }

    // ====================================================================
    //  Node picking
    // ====================================================================

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

    // ====================================================================
    //  State publishing
    // ====================================================================

    private void scheduleStateKeyMethods(RService service) {
        String prefix = service.serviceName() + ".";
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
            publisherFutures.add(future);
        }
    }

    // ====================================================================
    //  Helpers
    // ====================================================================

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
        for (ScheduledFuture<?> f : publisherFutures) {
            f.cancel(false);
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
    }

    // ====================================================================
    //  Inner classes
    // ====================================================================

    // ====================================================================
    //  Debug snapshot
    // ====================================================================

    public record ExecutorSnapshot(int activeThreads, int poolSize, int maxPoolSize,
                                   int queueSize, long completedTasks) {}

    public record ExecutorPairSnapshot(ExecutorSnapshot remote, ExecutorSnapshot local) {}

    public record PendingRequestDetail(int requestId, boolean streaming, long elapsedMs) {}

    public record DebugSnapshot(int pendingOutbound, int activeStreamsServer, int pendingHandshakes,
                                List<PendingRequestDetail> pendingDetails,
                                Map<String, ExecutorPairSnapshot> executorStats) {}

    private static ExecutorSnapshot snap(java.util.concurrent.ThreadPoolExecutor ex) {
        return new ExecutorSnapshot(ex.getActiveCount(), ex.getPoolSize(),
                ex.getMaximumPoolSize(), ex.getQueue().size(), ex.getCompletedTaskCount());
    }

    public DebugSnapshot debugSnapshot() {
        List<PendingRequestDetail> details = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (var pr : pendingRequests.values()) {
            details.add(new PendingRequestDetail(pr.requestId, pr.streaming,
                    now - pr.createdAt));
        }

        Map<String, ExecutorPairSnapshot> executorStats = new HashMap<>();
        for (var entry : services.entrySet()) {
            if (entry.getValue() instanceof RPriorityService ps) {
                executorStats.put(entry.getKey(),
                        new ExecutorPairSnapshot(snap(ps.remoteExecutor()), snap(ps.localExecutor())));
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
        final OnStateChange onStateChange;
        final Channel channel;
        final long createdAt;
        volatile ScheduledFuture<?> overallTimeout;
        volatile ScheduledFuture<?> idleTimeout;
        volatile boolean streaming;

        PendingRequest(int requestId, OnStateChange onStateChange, Channel channel) {
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
        final RService service;
        final byte[] requestData;
        volatile ScheduledFuture<?> handshakeTimeout;

        PendingStream(int requestId, ChannelHandlerContext ctx,
                      RService service, byte[] requestData) {
            this.requestId = requestId;
            this.ctx = ctx;
            this.service = service;
            this.requestData = requestData;
        }
    }
}
