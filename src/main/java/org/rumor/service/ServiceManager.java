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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Manages service registration, request routing, node picking, and
 * dispatches incoming requests to the correct service on a configurable thread pool.
 */
public class ServiceManager {

    private static final Logger log = LoggerFactory.getLogger(ServiceManager.class);

    private final ConnectionManager connectionManager;
    private final GossipService gossipService;
    private final NodeId localId;
    private final Map<String, RService> services = new ConcurrentHashMap<>();
    private final AtomicInteger requestIdGenerator = new AtomicInteger(1);
    private final ExecutorService workerPool;
    private volatile Consumer<Set<String>> onRegistrationChanged;

    // Tracks pending outbound requests (client side)
    private final Map<Integer, PendingRequest> pendingRequests = new ConcurrentHashMap<>();

    public ServiceManager(ConnectionManager connectionManager, GossipService gossipService,
                          NodeId localId, int threadPoolSize) {
        this.connectionManager = connectionManager;
        this.gossipService = gossipService;
        this.localId = localId;
        this.workerPool = Executors.newFixedThreadPool(threadPoolSize, r -> {
            Thread t = new Thread(r, "service-worker");
            t.setDaemon(true);
            return t;
        });
    }

    public void setOnRegistrationChanged(Consumer<Set<String>> callback) {
        this.onRegistrationChanged = callback;
    }

    public void register(RService service) {
        String name = service.serviceName();
        services.put(name, service);
        service.setManager(this);
        log.info("Registered service: {}", name);
        if (onRegistrationChanged != null) {
            onRegistrationChanged.accept(getRegisteredNames());
        }
    }

    public Set<String> getRegisteredNames() {
        return Collections.unmodifiableSet(services.keySet());
    }

    /**
     * Send a request to a remote peer offering the named service.
     * Picks a live node automatically via gossip.
     */
    public void sendRequest(String serviceName, byte[] request,
                            OnReceive onReceive, OnStateChange onStateChange) {
        NodeId target = pickNode(serviceName);
        if (target == null) {
            onStateChange.accept(RequestState.FAILED);
            log.warn("No live node found offering service '{}'", serviceName);
            return;
        }

        try {
            Channel channel = connectionManager.getChannel(target);
            if (channel == null) {
                channel = connectionManager.connect(target);
            }

            int requestId = requestIdGenerator.getAndIncrement();
            pendingRequests.put(requestId, new PendingRequest(onReceive, onStateChange));

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
            onStateChange.accept(RequestState.PROCESSING);
            log.debug("Sent request {} for service '{}' to {}", requestId, serviceName, target);

        } catch (Exception e) {
            onStateChange.accept(RequestState.FAILED);
            log.error("Failed to send request for service '{}' to {}", serviceName, target, e);
        }
    }

    // Incoming frame handlers
    public void handleServiceRequest(ChannelHandlerContext ctx, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int requestId = buf.getInt();

        short nameLen = buf.getShort();
        byte[] nameBytes = new byte[nameLen];
        buf.get(nameBytes);
        String serviceName = new String(nameBytes, StandardCharsets.UTF_8);

        byte[] requestData = new byte[buf.remaining()];
        buf.get(requestData);

        log.debug("Received SERVICE_REQUEST id={} service='{}' ({} bytes)",
                requestId, serviceName, requestData.length);

        RService service = services.get(serviceName);
        if (service != null) {
            workerPool.submit(() -> {
                ServiceResponse response = new ServiceResponse(requestId, ctx.channel());
                try {
                    service.serve(requestData, response);
                    response.close();
                } catch (Exception e) {
                    log.error("Error serving request {} (service='{}')", requestId, serviceName, e);
                    response.closeWithError();
                }
            });
        } else {
            log.warn("No service registered for '{}', ignoring request {}", serviceName, requestId);
            // Send error end
            byte[] endPayload = new byte[5];
            ByteBuffer endBuf = ByteBuffer.wrap(endPayload);
            endBuf.putInt(requestId);
            endBuf.put((byte) 1);
            ctx.writeAndFlush(new RumorFrame(MessageType.SERVICE_END, endPayload));
        }
    }

    public void handleServiceData(ChannelHandlerContext ctx, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int requestId = buf.getInt();
        byte[] data = new byte[payload.length - 4];
        buf.get(data);

        PendingRequest pending = pendingRequests.get(requestId);
        if (pending != null) {
            pending.onReceive.accept(data);
        } else {
            log.warn("Received data for unknown request {}", requestId);
        }
    }

    public void handleServiceEnd(ChannelHandlerContext ctx, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int requestId = buf.getInt();
        byte status = buf.get();

        PendingRequest pending = pendingRequests.remove(requestId);
        if (pending != null) {
            if (status == 0) {
                pending.onStateChange.accept(RequestState.SUCCEEDED);
            } else {
                pending.onStateChange.accept(RequestState.FAILED);
            }
            log.debug("Request {} completed (status={})", requestId, status);
        } else {
            log.warn("Received end for unknown request {}", requestId);
        }
    }

    /**
     * Pick a random live node that offers the given service.
     */
    private NodeId pickNode(String serviceName) {
        List<NodeId> candidates = new ArrayList<>();

        for (var entry : gossipService.getEndpointStates().entrySet()) {
            NodeId id = entry.getKey();
            if (id.equals(localId)) continue;

            EndpointState state = entry.getValue();
            VersionedValue statusVv = state.getAppState("STATUS");
            if (statusVv == null || !"ALIVE".equals(statusVv.value())) continue;

            VersionedValue servicesVv = state.getAppState("SERVICES");
            if (servicesVv == null || servicesVv.value().isEmpty()) continue;

            if (Set.of(servicesVv.value().split(",")).contains(serviceName)) {
                candidates.add(id);
            }
        }

        if (candidates.isEmpty()) return null;
        Collections.shuffle(candidates);
        return candidates.getFirst();
    }

    public void shutdown() {
        workerPool.shutdown();
        pendingRequests.clear();
    }

    private record PendingRequest(OnReceive onReceive, OnStateChange onStateChange) {}
}
