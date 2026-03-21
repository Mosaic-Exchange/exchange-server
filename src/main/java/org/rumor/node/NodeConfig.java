package org.rumor.node;

import org.rumor.gossip.NodeId;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for a RumorNode.
 */
public class NodeConfig {

    private String host = "127.0.0.1";
    private int port = 7000;
    private NodeType nodeType = NodeType.BASIC;
    private List<NodeId> seeds = new ArrayList<>();
    private long gossipIntervalMs = 1000;

    public NodeConfig host(String host) {
        this.host = host;
        return this;
    }

    public NodeConfig port(int port) {
        this.port = port;
        return this;
    }

    public NodeConfig nodeType(NodeType nodeType) {
        this.nodeType = nodeType;
        return this;
    }

    public NodeConfig addSeed(String host, int port) {
        seeds.add(new NodeId(host, port));
        return this;
    }

    public NodeConfig gossipIntervalMs(long ms) {
        this.gossipIntervalMs = ms;
        return this;
    }

    public String host() { return host; }
    public int port() { return port; }
    public NodeType nodeType() { return nodeType; }
    public List<NodeId> seeds() { return seeds; }
    public long gossipIntervalMs() { return gossipIntervalMs; }

    public NodeId nodeId() {
        return new NodeId(host, port);
    }
}
