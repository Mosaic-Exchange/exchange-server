package org.rumor.node;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.rumor.debug.DebugHttpServer;
import org.rumor.gossip.EndpointState;
import org.rumor.gossip.EvictionService;
import org.rumor.gossip.GossipService;
import org.rumor.gossip.NodeId;
import org.rumor.service.RService;
import org.rumor.service.ServiceManager;
import org.rumor.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * The main entry point for the Rumor framework.
 * Ties together the transport, gossip, and service layers.
 */
public class Rumor {

    private static final Logger log = LoggerFactory.getLogger(Rumor.class);

    private static final long EVICTION_CHECK_INTERVAL_MS = 2000;
    private static final long EVICTION_THRESHOLD_MS = 5000;

    private final RumorConfig config;
    private final NodeId localId;
    private final TransportServer server;
    private final ConnectionManager connectionManager;
    private final GossipService gossipService;
    private final ServiceManager serviceManager;
    private final EvictionService evictionService;
    private DebugHttpServer debugHttpServer;

    public Rumor(RumorConfig config) {
        this.config = config;
        this.localId = config.nodeId();

        this.server = new TransportServer(config.port(), this::onFrame, this::onInboundConnect);
        this.connectionManager = new ConnectionManager(server.workerGroup(), this::onFrame);
        this.gossipService = new GossipService(localId, config.seeds(),
                connectionManager, config.gossipIntervalMs());
        this.serviceManager = new ServiceManager(connectionManager, gossipService,
                localId, config.requestTimeoutMs(), config.requestIdleTimeoutMs());

        if (config.nodeType().isEvictor()) {
            this.evictionService = new EvictionService(localId, gossipService,
                    EVICTION_CHECK_INTERVAL_MS, EVICTION_THRESHOLD_MS);
        } else {
            this.evictionService = null;
        }
    }

    public void start() throws InterruptedException {
        server.start();
        gossipService.setLocalState("NODE_TYPE", config.nodeType().name());

        serviceManager.setOnRegistrationChanged(names -> {
            String csv = String.join(",", names);
            gossipService.setLocalState("SERVICES", csv);
        });
        // Publish any services that were registered before start()
        Set<String> existing = serviceManager.getRegisteredNames();
        if (!existing.isEmpty()) {
            gossipService.setLocalState("SERVICES", String.join(",", existing));
        }

        gossipService.start();
        if (evictionService != null) {
            evictionService.start();
        }

        if (config.debugPort() > 0) {
            try {
                debugHttpServer = new DebugHttpServer(config.debugPort(), serviceManager,
                        localId, this::getClusterState, System.currentTimeMillis());
                debugHttpServer.start();
            } catch (IOException e) {
                log.warn("Failed to start debug HTTP server on port {}: {}", config.debugPort(), e.getMessage());
            }
        }

        log.info("Rumor started: {} (type={})", localId, config.nodeType());
    }

    public void stop() {
        if (debugHttpServer != null) {
            debugHttpServer.stop();
        }
        if (evictionService != null) {
            evictionService.stop();
        }
        gossipService.stop();
        serviceManager.shutdown();
        connectionManager.closeAll();
        server.stop();
        log.info("Rumor stopped: {}", localId);
    }

    /**
     * Register a service. Streaming behavior is determined by the
     * {@link org.rumor.service.Streamable} annotation on the service class.
     */
    public void register(RService service) {
        serviceManager.register(service);
    }

    public void setAppState(String key, String value) {
        gossipService.setLocalState(key, value);
    }

    public Map<NodeId, EndpointState> getClusterState() {
        return gossipService.getEndpointStates();
    }

    public Set<NodeId> getLiveNodes() {
        return gossipService.getLiveNodes();
    }

    public NodeId localId() {
        return localId;
    }

    public RumorConfig config() {
        return config;
    }

    // --- Frame routing ---

    private void onFrame(ChannelHandlerContext ctx, RumorFrame frame) {
        switch (frame.type()) {
            case GOSSIP_DIGEST -> gossipService.handleGossipDigest(ctx, frame.payload());
            case GOSSIP_ACK    -> gossipService.handleGossipAck(ctx, frame.payload());
            case GOSSIP_ACK2   -> gossipService.handleGossipAck2(ctx, frame.payload());

            // RService (request/response)
            case SERVICE_REQUEST  -> serviceManager.handleServiceRequest(ctx, frame.payload());
            case SERVICE_RESPONSE -> serviceManager.handleServiceResponse(ctx, frame.payload());
            case SERVICE_ERROR    -> serviceManager.handleServiceError(ctx, frame.payload());

            // Streaming (handshake + streamed data)
            case SERVICE_INIT_STREAM  -> serviceManager.handleServiceInitStream(ctx, frame.payload());
            case SERVICE_STREAM_START -> serviceManager.handleServiceStreamStart(ctx, frame.payload());
            case SERVICE_STREAM_DATA  -> serviceManager.handleServiceStreamData(ctx, frame.payload());
            case SERVICE_STREAM_END   -> serviceManager.handleServiceStreamEnd(ctx, frame.payload());
            case SERVICE_STREAM_ERROR -> serviceManager.handleServiceStreamError(ctx, frame.payload());

            // Cancellation
            case SERVICE_CANCEL -> serviceManager.handleServiceCancel(ctx, frame.payload());
        }
    }

    private void onInboundConnect(Channel channel) {
        log.debug("Inbound connection from {}", channel.remoteAddress());
    }
}
