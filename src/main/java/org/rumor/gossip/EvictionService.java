package org.rumor.gossip;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Monitors node liveness by tracking heartbeat progress.
 * If a node's heartbeat hasn't advanced within the eviction threshold,
 * the evictor sets its STATUS to "DOWN" — which propagates via gossip
 * to all other nodes in the cluster.
 *
 * Only runs on EVICTION or MASTER nodes.
 */
public class EvictionService {

    private static final Logger log = LoggerFactory.getLogger(EvictionService.class);

    private final NodeId localNode;
    private final GossipService gossipService;
    private final long checkIntervalMs;
    private final long evictionThresholdMs;

    // Tracks the last observed heartbeat version for each node
    private final Map<NodeId, HeartbeatSnapshot> lastSeen = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "eviction-scheduler");
        t.setDaemon(true);
        return t;
    });

    private record HeartbeatSnapshot(long heartbeatVersion, long observedAtMs) {}

    public EvictionService(NodeId localNode, GossipService gossipService,
                           long checkIntervalMs, long evictionThresholdMs) {
        this.localNode = localNode;
        this.gossipService = gossipService;
        this.checkIntervalMs = checkIntervalMs;
        this.evictionThresholdMs = evictionThresholdMs;
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(this::evictionCheck, checkIntervalMs, checkIntervalMs,
                TimeUnit.MILLISECONDS);
        log.info("Eviction service started (check={}ms, threshold={}ms)",
                checkIntervalMs, evictionThresholdMs);
    }

    public void stop() {
        scheduler.shutdown();
        log.info("Eviction service stopped");
    }

    private void evictionCheck() {
        try {
            long now = System.currentTimeMillis();

            for (var entry : gossipService.getEndpointStates().entrySet()) {
                NodeId nodeId = entry.getKey();
                EndpointState state = entry.getValue();

                // Don't evict ourselves
                if (nodeId.equals(localNode)) continue;

                // Only track the real heartbeat — this is incremented solely
                // by the node itself during gossip rounds. setRemoteState does
                // NOT touch heartbeatVersion, so it can't create false advances.
                long currentHb = state.heartbeatVersion();
                HeartbeatSnapshot prev = lastSeen.get(nodeId);

                if (prev == null || prev.heartbeatVersion < currentHb) {
                    // Heartbeat advanced — node is genuinely alive
                    lastSeen.put(nodeId, new HeartbeatSnapshot(currentHb, now));

                    // If node was DOWN, mark it back ALIVE
                    VersionedValue status = state.getAppState("STATUS");
                    if (status != null && "DOWN".equals(status.value())) {
                        gossipService.setRemoteState(nodeId, "STATUS", "ALIVE");
                        log.info("Node {} marked ALIVE (heartbeat advanced to {})", nodeId, currentHb);
                    }
                } else {
                    // Heartbeat has NOT advanced since last snapshot
                    long staleForMs = now - prev.observedAtMs;

                    if (staleForMs >= evictionThresholdMs) {
                        VersionedValue status = state.getAppState("STATUS");
                        if (status == null || !"DOWN".equals(status.value())) {
                            gossipService.setRemoteState(nodeId, "STATUS", "DOWN");
                            log.warn("Node {} marked DOWN (heartbeat stale for {}ms)", nodeId, staleForMs);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error during eviction check", e);
        }
    }
}
