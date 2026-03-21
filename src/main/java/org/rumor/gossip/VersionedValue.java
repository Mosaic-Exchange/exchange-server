package org.rumor.gossip;

import java.io.*;

/**
 * An application-defined state value tagged with a version number.
 * Higher version = newer value.
 */
public record VersionedValue(String value, long version) {

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeUTF(value);
        out.writeLong(version);
    }

    public static VersionedValue readFrom(DataInputStream in) throws IOException {
        String value = in.readUTF();
        long version = in.readLong();
        return new VersionedValue(value, version);
    }
}
