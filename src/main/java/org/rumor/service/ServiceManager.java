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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Manages service registration, request routing, node picking, streaming
 * handshake, and periodic state publishing for services that implement
 * {@link StatePublisher}.
 *
 * <p>Handles two service types:
 * <ul>
 *   <li>{@link RService} — request/response on the I/O thread, single-write</li>
 *   <li>{@link RStreamingService} — streaming on a dedicated thread, multi-write,
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
    private final Map<String, RStreamingService> streamingServices = new ConcurrentHashMap<>();

    private final AtomicInteger requestIdGenerator = new AtomicInteger(1);
    private final ScheduledExecutorService timeoutScheduler;

    private volatile Consumer<Set<String>> onRegistrationChanged;

    private final long requestTimeoutMs;
    private final long requestIdleTimeoutMs;

    // --- Client-side: tracks pending outbound requests ---
    private final Map<Integer, PendingRequest> pendingRequests = new ConcurrentHashMap<>();

    // --- Server-side: streaming ---
    private final AtomicBoolean streamingActive = new AtomicBoolean(false);
    private final Map<Integer, PendingStream> pendingStreams = new ConcurrentHashMap<>();
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
        this.streamExecutor = Executors.newSingleThreadExecutor(r -> {
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
        log.info("Registered service: {}", name);

        if (service instanceof StatePublisher publisher) {
            scheduleStatePublisher(publisher);
        }
        fireRegistrationChanged();
    }

    public void register(RStreamingService service) {
        String name = service.serviceName();
        streamingServices.put(name, service);
        service.setManager(this);
        log.info("Registered streaming service: {}", name);

        if (service instanceof StatePublisher publisher) {
            scheduleStatePublisher(publisher);
        }
        fireRegistrationChanged();
    }

    public Set<String> getRegisteredNames() {
        Set<String> names = new HashSet<>(services.keySet());
        names.addAll(streamingServices.keySet());
        return Collections.unmodifiableSet(names);
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
     * Works for both {@link RService} and {@link RStreamingService} —
     * the server decides the protocol after receiving SERVICE_REQUEST.
     */
    public void sendRequest(String serviceName, byte[] request, OnStateChange onStateChange,
                            Predicate<Map<String, String>> peerFilter) {
        NodeId target = pickNode(serviceName, peerFilter);
        if (target == null) {
            onStateChange.accept(new RequestEvent.Failed("No live node found offering service '" + serviceName + "'"));
            log.warn("No live node found offering service '{}'", serviceName);
            return;
        }

        sendRequestTo(target, serviceName, request, onStateChange);
    }

    private void sendRequestTo(NodeId target, String serviceName, byte[] request,
                                OnStateChange onStateChange) {
        try {
            Channel channel = connectionManager.getChannel(target);
            if (channel == null) {
                channel = connectionManager.connect(target);
            }

            int requestId = requestIdGenerator.getAndIncrement();
            PendingRequest pending = new PendingRequest(requestId, onStateChange);
            pendingRequests.put(requestId, pending);

            pending.overallTimeout = timeoutScheduler.schedule(
                    () -> timeoutRequest(requestId, "Request timed out (exceeded " + requestTimeoutMs + "ms)"),
                    requestTimeoutMs, TimeUnit.MILLISECONDS);

            pending.resetIdleTimeout(timeoutScheduler, requestIdleTimeoutMs,
                    () -> timeoutRequest(requestId, "Idle timeout (no data received within " + requestIdleTimeoutMs + "ms)"));

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
        }
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
            pending.resetIdleTimeout(timeoutScheduler, requestIdleTimeoutMs,
                    () -> timeoutRequest(requestId, "Idle timeout (no stream data received within " + requestIdleTimeoutMs + "ms)"));

            // Acknowledge: ready to receive stream data
            byte[] startPayload = new byte[4];
            ByteBuffer.wrap(startPayload).putInt(requestId);
            ctx.writeAndFlush(new RumorFrame(MessageType.SERVICE_STREAM_START, startPayload));
            log.debug("Streaming initiated for request {}, sent STREAM_START", requestId);
        } else {
            log.warn("Received INIT_STREAM for unknown request {}", requestId);
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
            log.warn("Received stream data for unknown request {}", requestId);
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
        }
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

        // Check non-streaming services first
        RService service = services.get(serviceName);
        if (service != null) {
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

        // Check streaming services
        RStreamingService streamingService = streamingServices.get(serviceName);
        if (streamingService != null) {
            if (!streamingActive.compareAndSet(false, true)) {
                log.warn("Rejecting streaming request {} — a stream is already active", requestId);
                sendError(ctx, requestId, "A streaming session is already active on this node");
                return;
            }

            // Store pending stream and wait for STREAM_START from client
            PendingStream pending = new PendingStream(requestId, ctx, streamingService, requestData);
            pendingStreams.put(requestId, pending);

            // Timeout if client never sends STREAM_START
            pending.handshakeTimeout = timeoutScheduler.schedule(() -> {
                PendingStream removed = pendingStreams.remove(requestId);
                if (removed != null) {
                    streamingActive.set(false);
                    log.warn("Stream handshake timed out for request {}", requestId);
                    sendError(ctx, requestId, "Stream handshake timed out");
                }
            }, STREAM_HANDSHAKE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            // Send INIT_STREAM to client
            byte[] initPayload = new byte[4];
            ByteBuffer.wrap(initPayload).putInt(requestId);
            ctx.writeAndFlush(new RumorFrame(MessageType.SERVICE_INIT_STREAM, initPayload));
            log.debug("Sent INIT_STREAM for request {} (service='{}')", requestId, serviceName);
            return;
        }

        // Unknown service
        log.warn("No service registered for '{}', ignoring request {}", serviceName, requestId);
        sendError(ctx, requestId, "No service registered for '" + serviceName + "'");
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

        streamExecutor.submit(() -> {
            RemoteStreamingServiceResponse response = new RemoteStreamingServiceResponse(
                    requestId, pending.ctx.channel(), () -> streamingActive.set(false));
            try {
                pending.service.serve(pending.requestData, response);
                response.close();
            } catch (Exception e) {
                log.error("Error in streaming service for request {}", requestId, e);
                String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                response.fail(msg.getBytes(StandardCharsets.UTF_8));
            }
        });
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

    private void scheduleStatePublisher(StatePublisher publisher) {
        ScheduledFuture<?> future = timeoutScheduler.scheduleAtFixedRate(() -> {
            try {
                String value = publisher.computeState();
                gossipService.setLocalState(publisher.stateKey(), value);
            } catch (Exception e) {
                log.error("Error computing state for key '{}'", publisher.stateKey(), e);
            }
        }, 0, STATE_PUBLISH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        publisherFutures.add(future);
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
        streamingActive.set(false);
    }

    // ====================================================================
    //  Inner classes
    // ====================================================================

    /** Tracks a pending outbound request (client side). */
    private static class PendingRequest {
        final int requestId;
        final OnStateChange onStateChange;
        volatile ScheduledFuture<?> overallTimeout;
        volatile ScheduledFuture<?> idleTimeout;
        volatile boolean streaming;

        PendingRequest(int requestId, OnStateChange onStateChange) {
            this.requestId = requestId;
            this.onStateChange = onStateChange;
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
        final RStreamingService service;
        final byte[] requestData;
        volatile ScheduledFuture<?> handshakeTimeout;

        PendingStream(int requestId, ChannelHandlerContext ctx,
                      RStreamingService service, byte[] requestData) {
            this.requestId = requestId;
            this.ctx = ctx;
            this.service = service;
            this.requestData = requestData;
        }
    }
}
