package org.rumor.gossip;

import java.io.*;

/**
 * A compact summary of a node's state: just the identity and version info.
 * Used in the first round of gossip to compare who has newer data.
 */
public record GossipDigest(NodeId nodeId, long generation, long maxVersion, long heartbeatVersion) {

    public void writeTo(DataOutputStream out) throws IOException {
        nodeId.writeTo(out);
        out.writeLong(generation);
        out.writeLong(maxVersion);
        out.writeLong(heartbeatVersion);
    }

    public static GossipDigest readFrom(DataInputStream in) throws IOException {
        NodeId nodeId = NodeId.readFrom(in);
        long gen = in.readLong();
        long maxVer = in.readLong();
        long hbVer = in.readLong();
        return new GossipDigest(nodeId, gen, maxVer, hbVer);
    }
}
