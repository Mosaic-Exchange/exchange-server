package org.rumor.gossip;

import org.rumor.transport.ConnectionManager;
import org.rumor.transport.MessageType;
import org.rumor.transport.RumorFrame;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Implements the 3-way Push-Pull gossip protocol:
 *
 * 1. GOSSIP_DIGEST: "Here's what I know (as version summaries)."
 * 2. GOSSIP_ACK:    "Here's data you're missing + tell me what I'm missing."
 * 3. GOSSIP_ACK2:   "Here's the data you asked for."
 */
public class GossipService {

    private static final Logger log = LoggerFactory.getLogger(GossipService.class);

    private final NodeId localNode;
    private final ConnectionManager connectionManager;
    private final Map<NodeId, EndpointState> endpointStateMap = new ConcurrentHashMap<>();
    private final List<NodeId> seeds;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "gossip-scheduler");
        t.setDaemon(true);
        return t;
    });

    private final long gossipIntervalMs;

    public GossipService(NodeId localNode, List<NodeId> seeds,
                         ConnectionManager connectionManager, long gossipIntervalMs) {
        this.localNode = localNode;
        this.seeds = new ArrayList<>(seeds);
        this.connectionManager = connectionManager;
        this.gossipIntervalMs = gossipIntervalMs;

        // Initialize local state
        long generation = System.currentTimeMillis();
        EndpointState localState = new EndpointState(generation, 0);
        localState.putAppState("STATUS", "ALIVE");
        endpointStateMap.put(localNode, localState);
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(this::gossipRound, gossipIntervalMs, gossipIntervalMs,
                TimeUnit.MILLISECONDS);
        log.info("Gossip service started for {} (interval={}ms)", localNode, gossipIntervalMs);
    }

    public void stop() {
        scheduler.shutdown();
        log.info("Gossip service stopped");
    }

    /**
     * Set an application state key on the local node.
     */
    public void setLocalState(String key, String value) {
        EndpointState local = endpointStateMap.get(localNode);
        local.putAppState(key, value);
    }

    /**
     * Set an application state key on a remote node's local copy.
     * Used by the EvictionService to mark nodes DOWN/ALIVE.
     * The change propagates to other nodes via normal gossip.
     */
    public void setRemoteState(NodeId nodeId, String key, String value) {
        EndpointState state = endpointStateMap.get(nodeId);
        if (state != null) {
            // Use maxVersion+1 so gossip digests detect the change and propagate it.
            // We do NOT bump heartbeatVersion — only the node itself does that.
            state.putAppState(key, value, state.maxVersion() + 1);
        }
    }

    /**
     * Get the full cluster state view.
     */
    public Map<NodeId, EndpointState> getEndpointStates() {
        return Collections.unmodifiableMap(endpointStateMap);
    }

    public EndpointState getState(NodeId node) {
        return endpointStateMap.get(node);
    }

    public Set<NodeId> getLiveNodes() {
        return endpointStateMap.keySet();
    }

    

    private void gossipRound() {
        try {
            // Increment local heartbeat
            EndpointState local = endpointStateMap.get(localNode);
            local.incrementHeartbeat();

            // Pick a random peer to gossip with
            NodeId target = pickGossipTarget();
            if (target == null) {
                log.trace("No gossip target available");
                return;
            }

            // Ensure we're connected
            if (!connectionManager.isConnected(target)) {
                try {
                    connectionManager.connect(target);
                } catch (Exception e) {
                    log.debug("Could not connect to {}: {}", target, e.getMessage());
                    return;
                }
            }

            // Build and send digest
            byte[] digestPayload = buildDigestPayload();
            connectionManager.send(target, new RumorFrame(MessageType.GOSSIP_DIGEST, digestPayload));
            log.trace("Sent GOSSIP_DIGEST to {} ({} endpoints)", target, endpointStateMap.size());

        } catch (Exception e) {
            log.error("Error during gossip round", e);
        }
    }

    private NodeId pickGossipTarget() {
        List<NodeId> live = new ArrayList<>();
        List<NodeId> down = new ArrayList<>();

        for (var entry : endpointStateMap.entrySet()) {
            NodeId id = entry.getKey();
            if (id.equals(localNode)) continue;

            VersionedValue status = entry.getValue().getAppState("STATUS");
            if (status != null && "DOWN".equals(status.value())) {
                down.add(id);
            } else {
                live.add(id);
            }
        }

        // If we don't know anyone yet, bootstrap from a seed
        if (live.isEmpty() && down.isEmpty()) {
            if (seeds.isEmpty()) return null;
            List<NodeId> shuffled = new ArrayList<>(seeds);
            Collections.shuffle(shuffled);
            return shuffled.getFirst();
        }

        // 10% of the time still contact a seed for consistency
        if (!seeds.isEmpty() && ThreadLocalRandom.current().nextInt(10) == 0) {
            List<NodeId> shuffled = new ArrayList<>(seeds);
            Collections.shuffle(shuffled);
            return shuffled.getFirst();
        }

        // 10% of the time gossip with a DOWN node to detect recovery
        if (!down.isEmpty() && (live.isEmpty() || ThreadLocalRandom.current().nextInt(10) == 0)) {
            Collections.shuffle(down);
            return down.getFirst();
        }

        Collections.shuffle(live);
        return live.getFirst();
    }

    // Handle incoming gossip messages

    public void handleGossipDigest(ChannelHandlerContext ctx, byte[] payload) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));

            // Read sender's NodeId
            NodeId sender = NodeId.readFrom(in);

            // Read digests
            int count = in.readInt();
            List<GossipDigest> digests = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                digests.add(GossipDigest.readFrom(in));
            }

            log.debug("Received GOSSIP_DIGEST from {} with {} digests", sender, digests.size());

            // Register sender's channel so we can send back
            connectionManager.registerInbound(ctx.channel(), sender);

            // Build ACK response
            byte[] ackPayload = buildAckPayload(digests);
            ctx.writeAndFlush(new RumorFrame(MessageType.GOSSIP_ACK, ackPayload));

        } catch (IOException e) {
            log.error("Error handling GOSSIP_DIGEST", e);
        }
    }

    public void handleGossipAck(ChannelHandlerContext ctx, byte[] payload) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));

            // Part 1: updated states (data the sender is pushing to us)
            int updatedCount = in.readInt();
            for (int i = 0; i < updatedCount; i++) {
                NodeId nodeId = NodeId.readFrom(in);
                EndpointState remoteState = EndpointState.readFrom(in);
                applyRemoteState(nodeId, remoteState);
            }

            // Part 2: needed NodeIds (data the sender wants from us)
            int neededCount = in.readInt();
            List<NodeId> needed = new ArrayList<>(neededCount);
            for (int i = 0; i < neededCount; i++) {
                needed.add(NodeId.readFrom(in));
            }

            log.debug("Received GOSSIP_ACK: {} updates applied, {} states requested",
                    updatedCount, neededCount);

            // Respond with ACK2 containing the requested states
            if (!needed.isEmpty()) {
                byte[] ack2Payload = buildAck2Payload(needed);
                ctx.writeAndFlush(new RumorFrame(MessageType.GOSSIP_ACK2, ack2Payload));
            }

        } catch (IOException e) {
            log.error("Error handling GOSSIP_ACK", e);
        }
    }

    public void handleGossipAck2(ChannelHandlerContext ctx, byte[] payload) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                NodeId nodeId = NodeId.readFrom(in);
                EndpointState remoteState = EndpointState.readFrom(in);
                applyRemoteState(nodeId, remoteState);
            }
            log.debug("Received GOSSIP_ACK2: {} states applied", count);

        } catch (IOException e) {
            log.error("Error handling GOSSIP_ACK2", e);
        }
    }

    // Payload builders

    private byte[] buildDigestPayload() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        // Write our NodeId so the receiver can identify us
        localNode.writeTo(out);

        // Write digests for all known endpoints
        out.writeInt(endpointStateMap.size());
        for (var entry : endpointStateMap.entrySet()) {
            NodeId nodeId = entry.getKey();
            EndpointState state = entry.getValue();
            new GossipDigest(nodeId, state.generation(), state.maxVersion()).writeTo(out);
        }

        out.flush();
        return baos.toByteArray();
    }

    private byte[] buildAckPayload(List<GossipDigest> remoteDigests) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        // Determine what to push and what to request
        List<Map.Entry<NodeId, EndpointState>> toPush = new ArrayList<>();
        List<NodeId> toRequest = new ArrayList<>();

        Map<NodeId, GossipDigest> remoteDigestMap = new HashMap<>();
        for (GossipDigest gd : remoteDigests) {
            remoteDigestMap.put(gd.nodeId(), gd);
        }

        // Check what remote knows vs what we know
        for (var entry : endpointStateMap.entrySet()) {
            GossipDigest remoteDigest = remoteDigestMap.get(entry.getKey());
            if (remoteDigest == null) {
                // Remote doesn't know about this node at all — push it
                toPush.add(entry);
            } else if (entry.getValue().generation() > remoteDigest.generation()) {
                // We have a newer generation — push it
                toPush.add(entry);
            } else if (entry.getValue().generation() < remoteDigest.generation()) {
                // Remote has a newer generation — request it
                toRequest.add(entry.getKey());
            } else if (entry.getValue().maxVersion() > remoteDigest.maxVersion()) {
                // Same generation, but we have newer data — push it
                toPush.add(entry);
            } else if (entry.getValue().maxVersion() < remoteDigest.maxVersion()) {
                // Same generation, but remote has newer data — request it
                toRequest.add(entry.getKey());
            }
        }

        // Nodes remote knows about but we don't — request those too
        for (GossipDigest gd : remoteDigests) {
            if (!endpointStateMap.containsKey(gd.nodeId())) {
                toRequest.add(gd.nodeId());
            }
        }

        // Part 1: states we're pushing
        out.writeInt(toPush.size());
        for (var entry : toPush) {
            entry.getKey().writeTo(out);
            entry.getValue().writeTo(out);
        }

        // Part 2: NodeIds we need
        out.writeInt(toRequest.size());
        for (NodeId nodeId : toRequest) {
            nodeId.writeTo(out);
        }

        out.flush();
        return baos.toByteArray();
    }

    private byte[] buildAck2Payload(List<NodeId> requested) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        // Only send states we actually have
        List<Map.Entry<NodeId, EndpointState>> available = new ArrayList<>();
        for (NodeId nodeId : requested) {
            EndpointState state = endpointStateMap.get(nodeId);
            if (state != null) {
                available.add(Map.entry(nodeId, state));
            }
        }

        out.writeInt(available.size());
        for (var entry : available) {
            entry.getKey().writeTo(out);
            entry.getValue().writeTo(out);
        }

        out.flush();
        return baos.toByteArray();
    }

    // State application

    private void applyRemoteState(NodeId nodeId, EndpointState remoteState) {
        EndpointState local = endpointStateMap.get(nodeId);
        if (local == null) {
            endpointStateMap.put(nodeId, remoteState);
            log.info("Discovered new node: {} (gen={}, ver={})",
                    nodeId, remoteState.generation(), remoteState.maxVersion());
        } else {
            if (remoteState.generation() > local.generation()) {
                // New generation = node restarted, replace entirely
                endpointStateMap.put(nodeId, remoteState);
                log.info("Node {} restarted (new gen={})", nodeId, remoteState.generation());
            } else if (remoteState.generation() == local.generation()) {
                local.merge(remoteState);
            }
            // Ignore older generations
        }
    }
}
