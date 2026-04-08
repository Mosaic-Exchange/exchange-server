package org.rumor.gossip;

import java.io.*;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Full state for a single node: heartbeat info + application-defined key-value pairs.
 *
 * generation: incremented on restart, distinguishes incarnations
 * heartbeatVersion: incremented each gossip round, proves liveness
 * appStates: arbitrary key-value pairs (e.g., "STATUS" -> "ALIVE", "VRAM" -> "8GB")
 */
public class EndpointState {

    private final long generation;
    private volatile long heartbeatVersion;
    private final Map<String, VersionedValue> appStates = new ConcurrentHashMap<>();

    public EndpointState(long generation, long heartbeatVersion) {
        this.generation = generation;
        this.heartbeatVersion = heartbeatVersion;
    }

    public long generation() {
        return generation;
    }

    public long heartbeatVersion() {
        return heartbeatVersion;
    }

    public void incrementHeartbeat() {
        heartbeatVersion++;
    }

    /**
     * Returns the maximum version across heartbeat and all app state values.
     */
    public long maxVersion() {
        long max = heartbeatVersion;
        for (VersionedValue vv : appStates.values()) {
            max = Math.max(max, vv.version());
        }
        return max;
    }

    public void putAppState(String key, String value) {
        appStates.put(key, new VersionedValue(value, heartbeatVersion));
    }

    public void putAppState(String key, String value, long version) {
        appStates.put(key, new VersionedValue(value, version));
    }

    public VersionedValue getAppState(String key) {
        return appStates.get(key);
    }

    public Map<String, VersionedValue> appStates() {
        return Collections.unmodifiableMap(appStates);
    }

    /**
     * Merge remote state into this one: keep values with higher versions.
     */
    public void merge(EndpointState remote) {
        for (var entry : remote.appStates.entrySet()) {
            VersionedValue local = appStates.get(entry.getKey());
            if (local == null || entry.getValue().version() > local.version()) {
                appStates.put(entry.getKey(), entry.getValue());
            }
        }
        if (remote.heartbeatVersion > this.heartbeatVersion) {
            this.heartbeatVersion = remote.heartbeatVersion;
        }
    }

    // Serialization

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeLong(generation);
        out.writeLong(heartbeatVersion);
        out.writeInt(appStates.size());
        for (var entry : appStates.entrySet()) {
            out.writeUTF(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    public static EndpointState readFrom(DataInputStream in) throws IOException {
        long gen = in.readLong();
        long hbv = in.readLong();
        EndpointState state = new EndpointState(gen, hbv);
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String key = in.readUTF();
            VersionedValue vv = VersionedValue.readFrom(in);
            state.appStates.put(key, vv);
        }
        return state;
    }
}
