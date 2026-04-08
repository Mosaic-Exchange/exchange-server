package org.rumor.node;

import org.rumor.gossip.EndpointState;
import org.rumor.gossip.NodeId;
import org.rumor.gossip.VersionedValue;
import org.rumor.service.ServiceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Periodically writes a human-readable debug snapshot to a file.
 * The file is overwritten (not appended) on each tick.
 */
class DebugFileWriter {

    private static final Logger log = LoggerFactory.getLogger(DebugFileWriter.class);

    private final Path path;
    private final NodeId localId;
    private final ServiceManager serviceManager;
    private final Supplier<Map<NodeId, EndpointState>> clusterState;
    private final long startTime;
    private final ScheduledExecutorService scheduler;

    DebugFileWriter(Path path, NodeId localId, ServiceManager serviceManager,
                    Supplier<Map<NodeId, EndpointState>> clusterState, long startTime,
                    long intervalMs) {
        this.path = path.toAbsolutePath().normalize();
        this.localId = localId;
        this.serviceManager = serviceManager;
        this.clusterState = clusterState;
        this.startTime = startTime;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "debug-file-writer");
            t.setDaemon(true);
            return t;
        });

        // Write immediately, then at fixed interval
        scheduler.scheduleAtFixedRate(this::write, 0, intervalMs, TimeUnit.MILLISECONDS);
    }

    void stop() {
        scheduler.shutdownNow();
    }

    private void write() {
        try {
            String content = buildSnapshot();
            Files.createDirectories(path.getParent());
            Files.writeString(path, content, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            log.warn("Failed to write debug file {}: {}", path, e.getMessage());
        } catch (Exception e) {
            log.warn("Error building debug snapshot: {}", e.getMessage());
        }
    }

    private String buildSnapshot() {
        ServiceManager.DebugSnapshot snap = serviceManager.debugSnapshot();
        Map<NodeId, EndpointState> cluster = clusterState.get();
        long uptimeMs = System.currentTimeMillis() - startTime;

        EndpointState localState = cluster.get(localId);
        String nodeType = localState != null ? appVal(localState, "NODE_TYPE", "?") : "?";

        StringBuilder sb = new StringBuilder(2048);
        sb.append("=== Rumor Debug Snapshot ===\n");
        sb.append("Generated: ").append(Instant.now()).append('\n');
        sb.append("Node:      ").append(localId).append('\n');
        sb.append("Type:      ").append(nodeType).append('\n');
        sb.append("Uptime:    ").append(formatDuration(uptimeMs)).append('\n');
        sb.append('\n');

        // Requests
        long activeStreamsClient = snap.pendingDetails().stream()
                .filter(ServiceManager.PendingRequestDetail::streaming).count();
        sb.append("--- Requests ---\n");
        sb.append("Active streams (clt):  ").append(activeStreamsClient).append('\n');
        sb.append("Active streams (svr):  ").append(snap.activeStreamsServer()).append('\n');

        if (!snap.pendingDetails().isEmpty()) {
            sb.append('\n');
            sb.append("Pending request details:\n");
            sb.append(String.format("  %-12s %-10s %s%n", "REQUEST_ID", "STREAMING", "ELAPSED_MS"));
            snap.pendingDetails().stream()
                    .sorted(java.util.Comparator.comparingInt(ServiceManager.PendingRequestDetail::requestId))
                    .forEach(d -> sb.append(String.format("  %-12d %-10s %d%n",
                            d.requestId(), d.streaming(), d.elapsedMs())));
        }
        sb.append('\n');

        // Executor stats
        if (!snap.executorStats().isEmpty()) {
            sb.append("--- Executors ---\n");
            new java.util.TreeMap<>(snap.executorStats()).forEach((name, pair) -> {
                sb.append("Service: ").append(name).append('\n');
                appendExecutor(sb, "  Remote", pair.remote());
                appendExecutor(sb, "  Local ", pair.local());
            });
            sb.append('\n');
        }

        // Cluster topology
        sb.append("--- Cluster (").append(cluster.size()).append(" nodes) ---\n");
        sb.append(String.format("%-25s %-8s %-10s %-10s %-14s %s%n",
                "NODE", "TYPE", "STATUS", "HEARTBEAT", "GENERATION", "APP STATE"));
        sb.append("-".repeat(90)).append('\n');

        for (var entry : new java.util.TreeMap<>(cluster).entrySet()) {
            NodeId id = entry.getKey();
            EndpointState state = entry.getValue();
            String type = appVal(state, "NODE_TYPE", "?");
            String status = appVal(state, "STATUS", "?");
            String self = id.equals(localId) ? " (self)" : "";

            StringBuilder extra = new StringBuilder();
            for (var app : new java.util.TreeMap<>(state.appStates()).entrySet()) {
                String key = app.getKey();
                if (!key.equals("NODE_TYPE") && !key.equals("STATUS") && !key.equals("SERVICES")) {
                    if (!extra.isEmpty()) extra.append(", ");
                    extra.append(key).append("=").append(app.getValue().value());
                }
            }

            sb.append(String.format("%-25s %-8s %-10s %-10d %-14d %s%s%n",
                    id, type, status, state.heartbeatVersion(), state.generation(), extra, self));
        }

        return sb.toString();
    }

    private void appendExecutor(StringBuilder sb, String label, ServiceManager.ExecutorSnapshot es) {
        sb.append(label).append(": active=").append(es.activeThreads())
                .append(" pool=").append(es.poolSize())
                .append("/").append(es.maxPoolSize())
                .append(" queue=").append(es.queueSize())
                .append("/").append(es.queueCapacity())
                .append(" completed=").append(es.completedTasks())
                .append('\n');
    }

    private static String appVal(EndpointState state, String key, String defaultVal) {
        VersionedValue vv = state.getAppState(key);
        return vv != null ? vv.value() : defaultVal;
    }

    private static String formatDuration(long ms) {
        long secs = ms / 1000;
        long mins = secs / 60;
        long hrs = mins / 60;
        return String.format("%dh %dm %ds", hrs, mins % 60, secs % 60);
    }
}
