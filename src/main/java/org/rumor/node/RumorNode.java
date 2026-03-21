package org.rumor.node;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.rumor.exchange.Exchange;
import org.rumor.exchange.ExchangeHandler;
import org.rumor.exchange.ExchangeManager;
import org.rumor.gossip.EndpointState;
import org.rumor.gossip.EvictionService;
import org.rumor.gossip.GossipService;
import org.rumor.gossip.NodeId;
import org.rumor.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The main entry point for the Rumor framework.
 * Ties together the transport, gossip, and exchange layers.
 */
public class RumorNode {

    private static final Logger log = LoggerFactory.getLogger(RumorNode.class);

    private static final long EVICTION_CHECK_INTERVAL_MS = 2000;
    private static final long EVICTION_THRESHOLD_MS = 5000;

    private final NodeConfig config;
    private final NodeId localId;
    private final TransportServer server;
    private final ConnectionManager connectionManager;
    private final GossipService gossipService;
    private final ExchangeManager exchangeManager;
    private final EvictionService evictionService; // null if not an evictor

    public RumorNode(NodeConfig config) {
        this.config = config;
        this.localId = config.nodeId();

        // Create server with our frame dispatcher
        this.server = new TransportServer(config.port(), this::onFrame, this::onInboundConnect);
        this.connectionManager = new ConnectionManager(server.workerGroup(), this::onFrame);
        this.gossipService = new GossipService(localId, config.seeds(),
                connectionManager, config.gossipIntervalMs());
        this.exchangeManager = new ExchangeManager(connectionManager);

        if (config.nodeType().isEvictor()) {
            this.evictionService = new EvictionService(localId, gossipService,
                    EVICTION_CHECK_INTERVAL_MS, EVICTION_THRESHOLD_MS);
        } else {
            this.evictionService = null;
        }
    }

    /**
     * Start the node: begin listening, start gossip.
     */
    public void start() throws InterruptedException {
        server.start();
        gossipService.setLocalState("NODE_TYPE", config.nodeType().name());

        // Publish exchange types via gossip whenever registrations change
        exchangeManager.setOnRegistrationChanged(types -> {
            String csv = String.join(",", types);
            gossipService.setLocalState("EXCHANGES", csv);
        });

        gossipService.start();
        if (evictionService != null) {
            evictionService.start();
        }
        log.info("RumorNode started: {} (type={})", localId, config.nodeType());
    }

    /**
     * Stop the node gracefully.
     */
    public void stop() {
        if (evictionService != null) {
            evictionService.stop();
        }
        gossipService.stop();
        exchangeManager.shutdown();
        connectionManager.closeAll();
        server.stop();
        log.info("RumorNode stopped: {}", localId);
    }

    /**
     * Register a handler for a specific exchange type.
     * The exchange type is a global identifier (e.g., "hello", "file_transfer")
     * that is consistent across all nodes and propagated via gossip.
     *
     * @param exchangeType global identifier for the exchange
     * @param handler      callback invoked when a remote peer starts this exchange type
     */
    public void registerExchange(String exchangeType, ExchangeHandler handler) {
        exchangeManager.registerHandler(exchangeType, handler);
    }

    /**
     * Unregister a handler for a specific exchange type.
     */
    public void unregisterExchange(String exchangeType) {
        exchangeManager.unregisterHandler(exchangeType);
    }

    /**
     * Returns the set of exchange types registered on this node.
     */
    public Set<String> getRegisteredExchanges() {
        return exchangeManager.getRegisteredTypes();
    }

    /**
     * Returns the set of exchange types offered by a remote node (as seen via gossip).
     */
    public Set<String> getRemoteExchanges(NodeId peer) {
        EndpointState state = gossipService.getState(peer);
        if (state == null) return Set.of();
        var vv = state.getAppState("EXCHANGES");
        if (vv == null || vv.value().isEmpty()) return Set.of();
        return Set.of(vv.value().split(","));
    }

    /**
     * Find all live nodes that offer a specific exchange type.
     */
    public Set<NodeId> findNodesOffering(String exchangeType) {
        return gossipService.getEndpointStates().entrySet().stream()
                .filter(e -> {
                    var vv = e.getValue().getAppState("EXCHANGES");
                    if (vv == null || vv.value().isEmpty()) return false;
                    return Set.of(vv.value().split(",")).contains(exchangeType);
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Initiate an exchange with a remote peer.
     *
     * @param peer         the target node
     * @param exchangeType the global exchange type identifier
     * @param metadata     application-defined metadata
     */
    public Exchange startExchange(NodeId peer, String exchangeType, byte[] metadata)
            throws InterruptedException {
        return exchangeManager.startExchange(peer, exchangeType, metadata);
    }

    /**
     * Set an application state key (propagated via gossip).
     */
    public void setAppState(String key, String value) {
        gossipService.setLocalState(key, value);
    }

    /**
     * Get the cluster's view of all endpoint states.
     */
    public Map<NodeId, EndpointState> getClusterState() {
        return gossipService.getEndpointStates();
    }

    /**
     * Get all known live nodes.
     */
    public Set<NodeId> getLiveNodes() {
        return gossipService.getLiveNodes();
    }

    public NodeId localId() {
        return localId;
    }

    public NodeConfig config() {
        return config;
    }

    // frame routing 

    private void onFrame(ChannelHandlerContext ctx, RumorFrame frame) {
        switch (frame.type()) {
            case GOSSIP_DIGEST -> gossipService.handleGossipDigest(ctx, frame.payload());
            case GOSSIP_ACK    -> gossipService.handleGossipAck(ctx, frame.payload());
            case GOSSIP_ACK2   -> gossipService.handleGossipAck2(ctx, frame.payload());

            case EXCHANGE_START -> exchangeManager.handleExchangeStart(ctx, frame.payload());
            case EXCHANGE_DATA  -> exchangeManager.handleExchangeData(ctx, frame.payload());
            case EXCHANGE_END   -> exchangeManager.handleExchangeEnd(ctx, frame.payload());
        }
    }

    private void onInboundConnect(Channel channel) {
        log.debug("Inbound connection from {}", channel.remoteAddress());
    }
}
