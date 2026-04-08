package org.rumor.debug;

import com.sun.net.httpserver.HttpServer;
import org.rumor.gossip.EndpointState;
import org.rumor.gossip.NodeId;
import org.rumor.gossip.VersionedValue;
import org.rumor.service.ServiceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Lightweight HTTP server that exposes debug metrics as JSON.
 *
 * <p>Starts on a configurable port and serves {@code GET /debug}
 * with a snapshot of request, executor, and cluster state.
 */
public class DebugHttpServer {

    private static final Logger log = LoggerFactory.getLogger(DebugHttpServer.class);

    private final HttpServer server;

    public DebugHttpServer(int port, ServiceManager serviceManager, NodeId localId,
                           java.util.function.Supplier<Map<NodeId, EndpointState>> clusterState,
                           long startTime) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/debug", exchange -> {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            String json = buildJson(serviceManager, localId, clusterState.get(), startTime);
            byte[] body = json.getBytes(StandardCharsets.UTF_8);

            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });

        server.setExecutor(null); // default single-thread executor
    }

    public void start() {
        server.start();
        log.info("Debug HTTP server started on port {}", server.getAddress().getPort());
    }

    public void stop() {
        server.stop(0);
    }

    private String buildJson(ServiceManager serviceManager, NodeId localId,
                             Map<NodeId, EndpointState> cluster, long startTime) {
        ServiceManager.DebugSnapshot snap = serviceManager.debugSnapshot();

        StringBuilder sb = new StringBuilder(1024);
        sb.append("{");

        // Node info
        jsonStr(sb, "node", localId.toString());
        sb.append(",");
        jsonNum(sb, "uptimeMs", System.currentTimeMillis() - startTime);
        sb.append(",");

        // Request counts
        sb.append("\"requests\":{");
        jsonNum(sb, "pendingOutbound", snap.pendingOutbound());
        sb.append(",");
        jsonNum(sb, "activeStreamsServer", snap.activeStreamsServer());
        sb.append(",");
        jsonNum(sb, "pendingHandshakes", snap.pendingHandshakes());
        sb.append("},");

        // Pending request details
        sb.append("\"pendingDetails\":[");
        boolean first = true;
        for (var detail : snap.pendingDetails()) {
            if (!first) sb.append(",");
            first = false;
            sb.append("{");
            jsonNum(sb, "requestId", detail.requestId());
            sb.append(",");
            jsonBool(sb, "streaming", detail.streaming());
            sb.append(",");
            jsonNum(sb, "elapsedMs", detail.elapsedMs());
            sb.append("}");
        }
        sb.append("],");

        // Executor stats for RPriorityServices
        sb.append("\"executors\":{");
        first = true;
        for (var entry : snap.executorStats().entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append("\"").append(escJson(entry.getKey())).append("\":{");
            appendExecutorJson(sb, "remote", entry.getValue().remote());
            sb.append(",");
            appendExecutorJson(sb, "local", entry.getValue().local());
            sb.append("}");
        }
        sb.append("},");

        // Cluster
        sb.append("\"cluster\":[");
        first = true;
        for (var entry : cluster.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            NodeId id = entry.getKey();
            EndpointState state = entry.getValue();
            VersionedValue statusVv = state.getAppState("STATUS");
            VersionedValue typeVv = state.getAppState("NODE_TYPE");
            VersionedValue servicesVv = state.getAppState("SERVICES");

            sb.append("{");
            jsonStr(sb, "id", id.toString());
            sb.append(",");
            jsonStr(sb, "type", typeVv != null ? typeVv.value() : "?");
            sb.append(",");
            jsonStr(sb, "status", statusVv != null ? statusVv.value() : "?");
            sb.append(",");
            jsonStr(sb, "services", servicesVv != null ? servicesVv.value() : "");
            sb.append(",");
            jsonNum(sb, "heartbeat", state.heartbeatVersion());
            sb.append(",");
            jsonBool(sb, "self", id.equals(localId));
            sb.append("}");
        }
        sb.append("]");

        sb.append("}");
        return sb.toString();
    }

    private void appendExecutorJson(StringBuilder sb, String label, ServiceManager.ExecutorSnapshot es) {
        sb.append("\"").append(label).append("\":{");
        jsonNum(sb, "activeThreads", es.activeThreads());
        sb.append(",");
        jsonNum(sb, "poolSize", es.poolSize());
        sb.append(",");
        jsonNum(sb, "maxPoolSize", es.maxPoolSize());
        sb.append(",");
        jsonNum(sb, "queueSize", es.queueSize());
        sb.append(",");
        jsonNum(sb, "completedTasks", es.completedTasks());
        sb.append("}");
    }

    private static void jsonStr(StringBuilder sb, String key, String val) {
        sb.append("\"").append(key).append("\":\"").append(escJson(val)).append("\"");
    }

    private static void jsonNum(StringBuilder sb, String key, long val) {
        sb.append("\"").append(key).append("\":").append(val);
    }

    private static void jsonBool(StringBuilder sb, String key, boolean val) {
        sb.append("\"").append(key).append("\":").append(val);
    }

    private static String escJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
}
