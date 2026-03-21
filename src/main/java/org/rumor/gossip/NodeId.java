package org.rumor.gossip;

import java.io.*;

/**
 * Unique identifier for a node in the cluster, based on its listen address.
 */
public record NodeId(String host, int port) implements Comparable<NodeId> {

    @Override
    public String toString() {
        return host + ":" + port;
    }

    @Override
    public int compareTo(NodeId other) {
        int cmp = this.host.compareTo(other.host);
        return cmp != 0 ? cmp : Integer.compare(this.port, other.port);
    }

    // Serialization helpers

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeUTF(host);
        out.writeInt(port);
    }

    public static NodeId readFrom(DataInputStream in) throws IOException {
        String host = in.readUTF();
        int port = in.readInt();
        return new NodeId(host, port);
    }
}
