package org.rumor.node;

import org.rumor.gossip.NodeId;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for the Rumor framework.
 */
public class RumorConfig {

    private String host = "127.0.0.1";
    private int port = 7000;
    private NodeType nodeType = NodeType.BASIC;
    private List<NodeId> seeds = new ArrayList<>();
    private long gossipIntervalMs = 1000;
    private int maxRequestBytes = 1_048_576;   // 1 MB
    private int maxResponseBytes = 10_485_760; // 10 MB
    private long requestTimeoutMs = 30_000;    // overall max time for a request
    private long requestIdleTimeoutMs = 10_000; // max time between data messages

    public RumorConfig host(String host) {
        this.host = host;
        return this;
    }

    public RumorConfig port(int port) {
        this.port = port;
        return this;
    }

    public RumorConfig nodeType(NodeType nodeType) {
        this.nodeType = nodeType;
        return this;
    }

    public RumorConfig addSeed(String host, int port) {
        seeds.add(new NodeId(host, port));
        return this;
    }

    public RumorConfig gossipIntervalMs(long ms) {
        this.gossipIntervalMs = ms;
        return this;
    }

    public RumorConfig maxRequestBytes(int maxRequestBytes) {
        this.maxRequestBytes = maxRequestBytes;
        return this;
    }

    public RumorConfig maxResponseBytes(int maxResponseBytes) {
        this.maxResponseBytes = maxResponseBytes;
        return this;
    }

    public RumorConfig requestTimeoutMs(long ms) {
        this.requestTimeoutMs = ms;
        return this;
    }

    public RumorConfig requestIdleTimeoutMs(long ms) {
        this.requestIdleTimeoutMs = ms;
        return this;
    }

    public String host() { return host; }
    public int port() { return port; }
    public NodeType nodeType() { return nodeType; }
    public List<NodeId> seeds() { return seeds; }
    public long gossipIntervalMs() { return gossipIntervalMs; }
    public int maxRequestBytes() { return maxRequestBytes; }
    public int maxResponseBytes() { return maxResponseBytes; }
    public long requestTimeoutMs() { return requestTimeoutMs; }
    public long requestIdleTimeoutMs() { return requestIdleTimeoutMs; }

    public NodeId nodeId() {
        return new NodeId(host, port);
    }
}
