package org.rumor.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.rumor.app.FileDownloadService;
import org.rumor.app.InferenceRequest;
import org.rumor.app.InferenceService;
import org.rumor.gossip.EndpointState;
import org.rumor.gossip.NodeId;
import org.rumor.gossip.VersionedValue;
import org.rumor.service.OnStateChange;
import org.rumor.service.RequestEvent;
import org.rumor.service.ServiceHandle;
import org.rumor.service.ServiceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Full HTTP server replacing DebugHttpServer.
 * Provides REST+SSE endpoints for debug metrics, inference, and model downloads.
 */
public class MosaicHttpServer {

    private static final Logger log = LoggerFactory.getLogger(MosaicHttpServer.class);

    private final HttpServer server;
    private final ServiceManager serviceManager;
    private final NodeId localId;
    private final Supplier<Map<NodeId, EndpointState>> clusterState;
    private final long startTime;
    private final InferenceService inferenceService;
    private final FileDownloadService fileDownloadService;
    private final Path sharedDir;

    // Active operation tracking
    private final AtomicReference<ServiceHandle> activeInference = new AtomicReference<>();
    private final AtomicReference<ServiceHandle> activeDownload = new AtomicReference<>();
    private final AtomicReference<DownloadProgress> downloadProgress = new AtomicReference<>();

    // SSE subscribers for download progress
    private final CopyOnWriteArrayList<OutputStream> downloadSubscribers = new CopyOnWriteArrayList<>();

    public MosaicHttpServer(int port, ServiceManager serviceManager, NodeId localId,
                            Supplier<Map<NodeId, EndpointState>> clusterState, long startTime,
                            InferenceService inferenceService,
                            FileDownloadService fileDownloadService,
                            Path sharedDir) throws IOException {
        this.serviceManager = serviceManager;
        this.localId = localId;
        this.clusterState = clusterState;
        this.startTime = startTime;
        this.inferenceService = inferenceService;
        this.fileDownloadService = fileDownloadService;
        this.sharedDir = sharedDir;

        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(8, r -> {
            Thread t = new Thread(r, "http-server");
            t.setDaemon(true);
            return t;
        }));

        server.createContext("/api/debug", this::handleDebug);
        server.createContext("/api/inference", this::handleInference);
        server.createContext("/api/models/download/progress", this::handleDownloadProgress);
        server.createContext("/api/models/download", this::handleDownload);
        server.createContext("/api/models", this::handleModels);
    }

    public void start() {
        server.start();
        log.info("Mosaic HTTP server started on port {}", server.getAddress().getPort());
    }

    public void stop() {
        server.stop(0);
    }

    // ====================================================================
    //  CORS + routing helpers
    // ====================================================================

    private void setCors(HttpExchange ex) {
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        ex.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
        ex.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
    }

    private boolean handlePreflight(HttpExchange ex) throws IOException {
        if ("OPTIONS".equals(ex.getRequestMethod())) {
            setCors(ex);
            ex.sendResponseHeaders(204, -1);
            ex.close();
            return true;
        }
        return false;
    }

    private void sendJson(HttpExchange ex, int status, String json) throws IOException {
        setCors(ex);
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }

    private void sendEmpty(HttpExchange ex, int status) throws IOException {
        setCors(ex);
        ex.sendResponseHeaders(status, -1);
        ex.close();
    }

    private String readBody(HttpExchange ex) throws IOException {
        try (InputStream is = ex.getRequestBody()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    // ====================================================================
    //  GET /api/debug
    // ====================================================================

    private void handleDebug(HttpExchange ex) throws IOException {
        if (handlePreflight(ex)) return;
        if (!"GET".equals(ex.getRequestMethod())) { sendEmpty(ex, 405); return; }

        String json = buildDebugJson();
        sendJson(ex, 200, json);
    }

    private String buildDebugJson() {
        ServiceManager.DebugSnapshot snap = serviceManager.debugSnapshot();
        Map<NodeId, EndpointState> cluster = clusterState.get();

        StringBuilder sb = new StringBuilder(2048);
        sb.append("{");

        jsonStr(sb, "node", localId.toString()); sb.append(",");
        jsonNum(sb, "uptimeMs", System.currentTimeMillis() - startTime); sb.append(",");

        // Request counts
        sb.append("\"requests\":{");
        jsonNum(sb, "pendingOutbound", snap.pendingOutbound()); sb.append(",");
        jsonNum(sb, "activeStreamsServer", snap.activeStreamsServer()); sb.append(",");
        jsonNum(sb, "pendingHandshakes", snap.pendingHandshakes());
        sb.append("},");

        // Pending request details
        sb.append("\"pendingDetails\":[");
        boolean first = true;
        for (var detail : snap.pendingDetails()) {
            if (!first) sb.append(",");
            first = false;
            sb.append("{");
            jsonNum(sb, "requestId", detail.requestId()); sb.append(",");
            jsonBool(sb, "streaming", detail.streaming()); sb.append(",");
            jsonNum(sb, "elapsedMs", detail.elapsedMs());
            sb.append("}");
        }
        sb.append("],");

        // Executor stats per service
        sb.append("\"executors\":{");
        first = true;
        for (var entry : snap.executorStats().entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append("\"").append(escJson(entry.getKey())).append("\":{");
            appendExecutor(sb, "remote", entry.getValue().remote()); sb.append(",");
            appendExecutor(sb, "local", entry.getValue().local());
            sb.append("}");
        }
        sb.append("},");

        // Active operations
        sb.append("\"activeOperations\":{");
        jsonBool(sb, "inferenceRunning", activeInference.get() != null); sb.append(",");
        DownloadProgress dp = downloadProgress.get();
        jsonBool(sb, "downloadRunning", activeDownload.get() != null);
        if (dp != null) {
            sb.append(",\"download\":{");
            jsonStr(sb, "fileName", dp.fileName); sb.append(",");
            jsonNum(sb, "bytesReceived", dp.bytesReceived.get()); sb.append(",");
            jsonNum(sb, "totalBytes", dp.totalBytes); sb.append(",");
            jsonStr(sb, "status", dp.status);
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
            jsonStr(sb, "id", id.toString()); sb.append(",");
            jsonStr(sb, "type", typeVv != null ? typeVv.value() : "?"); sb.append(",");
            jsonStr(sb, "status", statusVv != null ? statusVv.value() : "?"); sb.append(",");
            jsonStr(sb, "services", servicesVv != null ? servicesVv.value() : ""); sb.append(",");
            jsonNum(sb, "heartbeat", state.heartbeatVersion()); sb.append(",");
            jsonBool(sb, "self", id.equals(localId));

            // All app states
            sb.append(",\"appStates\":{");
            boolean firstApp = true;
            for (var app : state.appStates().entrySet()) {
                if (!firstApp) sb.append(",");
                firstApp = false;
                jsonStr(sb, app.getKey(), app.getValue().value());
            }
            sb.append("}");

            sb.append("}");
        }
        sb.append("]");

        sb.append("}");
        return sb.toString();
    }

    private void appendExecutor(StringBuilder sb, String label, ServiceManager.ExecutorSnapshot es) {
        sb.append("\"").append(label).append("\":{");
        jsonNum(sb, "activeThreads", es.activeThreads()); sb.append(",");
        jsonNum(sb, "poolSize", es.poolSize()); sb.append(",");
        jsonNum(sb, "maxPoolSize", es.maxPoolSize()); sb.append(",");
        jsonNum(sb, "queueSize", es.queueSize()); sb.append(",");
        jsonNum(sb, "completedTasks", es.completedTasks());
        sb.append("}");
    }

    // ====================================================================
    //  POST /api/inference  (SSE stream)
    //  DELETE /api/inference (cancel)
    // ====================================================================

    private void handleInference(HttpExchange ex) throws IOException {
        if (handlePreflight(ex)) return;

        switch (ex.getRequestMethod()) {
            case "POST" -> handleInferenceStart(ex);
            case "DELETE" -> handleInferenceCancel(ex);
            default -> sendEmpty(ex, 405);
        }
    }

    private void handleInferenceStart(HttpExchange ex) throws IOException {
        String body = readBody(ex);
        String prompt = extractJsonString(body, "prompt");
        boolean local = extractJsonBool(body, "local", false);

        if (prompt == null || prompt.isEmpty()) {
            sendJson(ex, 400, "{\"error\":\"prompt is required\"}");
            return;
        }

        // Cancel any existing inference
        ServiceHandle prev = activeInference.getAndSet(null);
        if (prev != null) prev.cancel();

        // Set up SSE response
        setCors(ex);
        ex.getResponseHeaders().set("Content-Type", "text/event-stream");
        ex.getResponseHeaders().set("Cache-Control", "no-cache");
        ex.getResponseHeaders().set("Connection", "keep-alive");
        ex.sendResponseHeaders(200, 0);
        OutputStream out = ex.getResponseBody();

        String model = extractJsonString(body, "model");
        String effort = extractJsonString(body, "effort");
        int maxTokens = extractJsonInt(body, "max_output_tokens", 0);
        double temperature = extractJsonDouble(body, "temperature", 0);

        InferenceRequest request = new InferenceRequest(prompt, model, effort, maxTokens, temperature);
        CountDownLatch done = new CountDownLatch(1);

        OnStateChange callback = event -> {
            try {
                switch (event) {
                    case RequestEvent.Processing p ->
                            writeSse(out, "status", "processing");
                    case RequestEvent.StreamData d -> {
                        writeSse(out, "token", new String(d.raw(), StandardCharsets.UTF_8));
                    }
                    case RequestEvent.Succeeded s -> {
                        if (s.data() != null) {
                            writeSse(out, "token", new String(s.raw(), StandardCharsets.UTF_8));
                        }
                        writeSse(out, "done", "");
                        done.countDown();
                    }
                    case RequestEvent.Failed f -> {
                        writeSse(out, "error", f.reason());
                        done.countDown();
                    }
                    default -> {}
                }
            } catch (IOException e) {
                log.debug("SSE write failed (client disconnected?): {}", e.getMessage());
                done.countDown();
            }
        };

        ServiceHandle handle;
        if (local) {
            handle = inferenceService.request(request, callback);
        } else {
            handle = inferenceService.dispatch(request, callback);
        }
        activeInference.set(handle);

        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            activeInference.compareAndSet(handle, null);
            try { out.close(); } catch (IOException ignored) {}
            ex.close();
        }
    }

    private void handleInferenceCancel(HttpExchange ex) throws IOException {
        ServiceHandle h = activeInference.getAndSet(null);
        if (h != null) {
            h.cancel();
            sendJson(ex, 200, "{\"status\":\"cancelled\"}");
        } else {
            sendJson(ex, 200, "{\"status\":\"no_active_inference\"}");
        }
    }

    // ====================================================================
    //  GET /api/models
    // ====================================================================

    private void handleModels(HttpExchange ex) throws IOException {
        if (handlePreflight(ex)) return;
        if (!"GET".equals(ex.getRequestMethod())) { sendEmpty(ex, 405); return; }

        Map<NodeId, String> peerFiles = fileDownloadService.discoverFiles();

        StringBuilder sb = new StringBuilder(512);
        sb.append("{\"peers\":[");
        boolean first = true;
        for (var entry : peerFiles.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append("{");
            jsonStr(sb, "nodeId", entry.getKey().toString());
            sb.append(",\"files\":[");

            boolean firstFile = true;
            for (String fileEntry : entry.getValue().split(",")) {
                if (fileEntry.isEmpty()) continue;
                int colon = fileEntry.lastIndexOf(':');
                String name;
                long size = 0;
                if (colon > 0) {
                    name = fileEntry.substring(0, colon);
                    try { size = Long.parseLong(fileEntry.substring(colon + 1)); } catch (NumberFormatException ignored) {}
                } else {
                    name = fileEntry;
                }
                if (!firstFile) sb.append(",");
                firstFile = false;
                sb.append("{");
                jsonStr(sb, "name", name); sb.append(",");
                jsonNum(sb, "size", size);
                sb.append("}");
            }
            sb.append("]}");
        }
        sb.append("]}");
        sendJson(ex, 200, sb.toString());
    }

    // ====================================================================
    //  POST /api/models/download   (start download)
    //  DELETE /api/models/download  (cancel download)
    // ====================================================================

    private void handleDownload(HttpExchange ex) throws IOException {
        if (handlePreflight(ex)) return;

        switch (ex.getRequestMethod()) {
            case "POST" -> handleDownloadStart(ex);
            case "DELETE" -> handleDownloadCancel(ex);
            default -> sendEmpty(ex, 405);
        }
    }

    private void handleDownloadStart(HttpExchange ex) throws IOException {
        String body = readBody(ex);
        String fileName = extractJsonString(body, "fileName");

        if (fileName == null || fileName.isEmpty()) {
            sendJson(ex, 400, "{\"error\":\"fileName is required\"}");
            return;
        }

        // Cancel any existing download
        ServiceHandle prev = activeDownload.getAndSet(null);
        if (prev != null) prev.cancel();

        // Determine total size from peer files
        long totalBytes = 0;
        Map<NodeId, String> peerFiles = fileDownloadService.discoverFiles();
        outer:
        for (String listing : peerFiles.values()) {
            for (String entry : listing.split(",")) {
                int colon = entry.lastIndexOf(':');
                if (colon > 0) {
                    String name = entry.substring(0, colon);
                    if (name.equals(fileName)) {
                        try { totalBytes = Long.parseLong(entry.substring(colon + 1)); } catch (NumberFormatException ignored) {}
                        break outer;
                    }
                }
            }
        }

        Path outputPath = uniquePath(sharedDir.resolve(Path.of(fileName).getFileName()).toAbsolutePath());
        Files.createDirectories(outputPath.getParent());

        DownloadProgress progress = new DownloadProgress(fileName, totalBytes);
        downloadProgress.set(progress);

        try {
            FileOutputStream fos = new FileOutputStream(outputPath.toFile());

            ServiceHandle handle = fileDownloadService.downloadFrom(fileName, event -> {
                switch (event) {
                    case RequestEvent.Processing p -> {
                        progress.status = "downloading";
                        notifyDownloadSubscribers(progress);
                    }
                    case RequestEvent.StreamData d -> {
                        try {
                            byte[] data = d.raw();
                            fos.write(data);
                            progress.bytesReceived.addAndGet(data.length);
                            notifyDownloadSubscribers(progress);
                        } catch (IOException e) {
                            log.error("Write error during download: {}", e.getMessage());
                        }
                    }
                    case RequestEvent.Succeeded s -> {
                        try { fos.close(); } catch (IOException ignored) {}
                        progress.status = "completed";
                        notifyDownloadSubscribers(progress);
                        activeDownload.set(null);
                    }
                    case RequestEvent.Failed f -> {
                        try { fos.close(); } catch (IOException ignored) {}
                        progress.status = "failed:" + f.reason();
                        notifyDownloadSubscribers(progress);
                        activeDownload.set(null);
                    }
                    default -> {}
                }
            });

            activeDownload.set(handle);
            sendJson(ex, 200, "{\"status\":\"started\",\"outputPath\":\"" + escJson(outputPath.toString()) + "\"}");

        } catch (IOException e) {
            progress.status = "failed:" + e.getMessage();
            downloadProgress.set(null);
            sendJson(ex, 500, "{\"error\":\"" + escJson(e.getMessage()) + "\"}");
        }
    }

    private void handleDownloadCancel(HttpExchange ex) throws IOException {
        ServiceHandle h = activeDownload.getAndSet(null);
        if (h != null) {
            h.cancel();
            DownloadProgress dp = downloadProgress.get();
            if (dp != null) {
                dp.status = "cancelled";
                notifyDownloadSubscribers(dp);
            }
            sendJson(ex, 200, "{\"status\":\"cancelled\"}");
        } else {
            sendJson(ex, 200, "{\"status\":\"no_active_download\"}");
        }
    }

    // ====================================================================
    //  GET /api/models/download/progress  (SSE)
    // ====================================================================

    private void handleDownloadProgress(HttpExchange ex) throws IOException {
        if (handlePreflight(ex)) return;
        if (!"GET".equals(ex.getRequestMethod())) { sendEmpty(ex, 405); return; }

        setCors(ex);
        ex.getResponseHeaders().set("Content-Type", "text/event-stream");
        ex.getResponseHeaders().set("Cache-Control", "no-cache");
        ex.getResponseHeaders().set("Connection", "keep-alive");
        ex.sendResponseHeaders(200, 0);
        OutputStream out = ex.getResponseBody();

        // Send current state immediately
        DownloadProgress dp = downloadProgress.get();
        if (dp != null) {
            try {
                writeSse(out, "progress", buildProgressJson(dp));
            } catch (IOException e) {
                ex.close();
                return;
            }
        }

        downloadSubscribers.add(out);
        try {
            // Keep connection open until client disconnects or download finishes
            // Poll to detect client disconnect
            while (true) {
                Thread.sleep(2000);
                DownloadProgress current = downloadProgress.get();
                if (current != null) {
                    String status = current.status;
                    if (status.startsWith("completed") || status.startsWith("failed") || status.equals("cancelled")) {
                        // Send final update and close
                        try { writeSse(out, "progress", buildProgressJson(current)); } catch (IOException ignored) {}
                        break;
                    }
                }
                // Heartbeat to detect disconnect
                try {
                    writeSse(out, "heartbeat", "");
                } catch (IOException e) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            downloadSubscribers.remove(out);
            try { out.close(); } catch (IOException ignored) {}
            ex.close();
        }
    }

    private void notifyDownloadSubscribers(DownloadProgress progress) {
        String json = buildProgressJson(progress);
        for (OutputStream out : downloadSubscribers) {
            try {
                writeSse(out, "progress", json);
            } catch (IOException e) {
                downloadSubscribers.remove(out);
            }
        }
    }

    private String buildProgressJson(DownloadProgress dp) {
        StringBuilder sb = new StringBuilder(128);
        sb.append("{");
        jsonStr(sb, "fileName", dp.fileName); sb.append(",");
        jsonNum(sb, "bytesReceived", dp.bytesReceived.get()); sb.append(",");
        jsonNum(sb, "totalBytes", dp.totalBytes); sb.append(",");
        jsonStr(sb, "status", dp.status);
        sb.append("}");
        return sb.toString();
    }

    // ====================================================================
    //  SSE + JSON helpers
    // ====================================================================

    private static void writeSse(OutputStream out, String event, String data) throws IOException {
        String msg = "event: " + event + "\ndata: " + data.replace("\n", "\ndata: ") + "\n\n";
        out.write(msg.getBytes(StandardCharsets.UTF_8));
        out.flush();
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
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    private static String extractJsonString(String json, String field) {
        String key = "\"" + field + "\":\"";
        int start = json.indexOf(key);
        if (start < 0) return null;
        start += key.length();
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '\\' && i + 1 < json.length()) {
                sb.append(json.charAt(++i));
            } else if (c == '"') {
                break;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static boolean extractJsonBool(String json, String field, boolean defaultVal) {
        String key = "\"" + field + "\":";
        int start = json.indexOf(key);
        if (start < 0) return defaultVal;
        start += key.length();
        String rest = json.substring(start).trim();
        if (rest.startsWith("true")) return true;
        if (rest.startsWith("false")) return false;
        return defaultVal;
    }

    private static int extractJsonInt(String json, String field, int defaultVal) {
        String key = "\"" + field + "\":";
        int start = json.indexOf(key);
        if (start < 0) return defaultVal;
        start += key.length();
        String rest = json.substring(start).trim();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rest.length(); i++) {
            char c = rest.charAt(i);
            if (Character.isDigit(c) || (c == '-' && i == 0)) sb.append(c);
            else break;
        }
        if (sb.isEmpty()) return defaultVal;
        try { return Integer.parseInt(sb.toString()); } catch (NumberFormatException e) { return defaultVal; }
    }

    private static double extractJsonDouble(String json, String field, double defaultVal) {
        String key = "\"" + field + "\":";
        int start = json.indexOf(key);
        if (start < 0) return defaultVal;
        start += key.length();
        String rest = json.substring(start).trim();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rest.length(); i++) {
            char c = rest.charAt(i);
            if (Character.isDigit(c) || c == '.' || (c == '-' && i == 0)) sb.append(c);
            else break;
        }
        if (sb.isEmpty()) return defaultVal;
        try { return Double.parseDouble(sb.toString()); } catch (NumberFormatException e) { return defaultVal; }
    }

    private static Path uniquePath(Path path) {
        if (!Files.exists(path)) return path;
        String name = path.getFileName().toString();
        String base, ext;
        int dot = name.lastIndexOf('.');
        if (dot > 0) { base = name.substring(0, dot); ext = name.substring(dot); }
        else { base = name; ext = ""; }
        Path parent = path.getParent();
        int i = 1;
        Path candidate;
        do { candidate = parent.resolve(base + "_" + i + ext); i++; } while (Files.exists(candidate));
        return candidate;
    }

    // ====================================================================
    //  Download progress state
    // ====================================================================

    static class DownloadProgress {
        final String fileName;
        final long totalBytes;
        final AtomicLong bytesReceived = new AtomicLong(0);
        volatile String status = "pending";

        DownloadProgress(String fileName, long totalBytes) {
            this.fileName = fileName;
            this.totalBytes = totalBytes;
        }
    }
}
