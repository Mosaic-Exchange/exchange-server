package org.rumor;

import org.rumor.app.FileDownloadService;
import org.rumor.app.InferenceRequest;
import org.rumor.app.InferenceService;
import org.rumor.gossip.EndpointState;
import org.rumor.gossip.NodeId;
import org.rumor.gossip.VersionedValue;
import org.rumor.http.MosaicHttpServer;
import org.rumor.node.NodeType;
import org.rumor.node.Rumor;
import org.rumor.node.RumorConfig;
import org.rumor.service.OnStateChange;
import org.rumor.service.RequestEvent;
import org.rumor.service.RService;
import org.rumor.service.ServiceHandle;
import org.rumor.service.ServiceRequest;
import org.rumor.service.ServiceResponse;
import org.rumor.service.RService.Config;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

import sun.misc.Signal;

/**
 * 
 *  mvn exec:java -Dexec.args="--port 7001 --type master --host 172.20.10.5"
 *   # Start a master(seed+eviction) node on port 7001
 *   mvn exec:java -Dexec.args="--port 7001 --type master"
 *
 *   # Start a basic node on port 7002, connecting to the seed
 *   mvn exec:java -Dexec.args="--port 7002 --type basic --seed 127.0.0.1:7001"
 *
 * Console commands:
 *   ls       - Show network topology (all known nodes, heartbeat, state)
 *   hello    - Send a hello request to the best available node
 *   a        - Run LLM inference locally (alias: ask-local)
 *   ask-remote - Run LLM inference on a remote peer
 *   files    - Discover files available on a remote peer
 *   download - Download a file from a remote peer
 *   quit     - Shut down this node
 */
public class Main {

    static class HelloService extends RService {

        @Override
        public void serve(ServiceRequest request, ServiceResponse response) {
            String message = new String(request.raw(), StandardCharsets.UTF_8);
            System.out.println();
            System.out.println(">> Incoming hello request: " + message);

            String reply = "Hello back! Got your message: " + message;
            response.write(reply.getBytes(StandardCharsets.UTF_8));
            System.out.println(">> Sent response");
            System.out.print("rumor> ");
        }
    }

    private static Path sharedDir = Path.of(System.getProperty("user.home"), "mosaic-shared");
    private static volatile ServiceHandle activeHandle;
    private static MosaicHttpServer httpServer;

    public static void main(String[] args) throws Exception {
        RumorConfig config = parseArgs(args);
        Rumor rumor = new Rumor(config);

        Signal.handle(new Signal("INT"), signal -> {
            ServiceHandle h = activeHandle;
            if (h != null) {
                System.out.println("\nCancelling...");
                h.cancel();
            } else {
                rumor.stop();
                System.out.println("\nGoodbye.");
                System.exit(0);
            }
        });

        Path shared = sharedDir.toAbsolutePath().normalize();

        HelloService helloService = new HelloService();
        InferenceService inferenceService = new InferenceService();
        FileDownloadService fileDownloadService = new FileDownloadService(shared);

        rumor.register(helloService);
        rumor.register(inferenceService, new Config()
                .remoteThreads(2)
                .remoteQueueCapacity(2)
                .localThreads(2)
                .localQueueCapacity(2));
        rumor.register(fileDownloadService, new Config()
                .remoteThreads(2)
                .remoteQueueCapacity(2));

        rumor.start();

        // Start HTTP server if configured
        if (config.httpPort() > 0) {
            try {
                httpServer = new MosaicHttpServer(
                        config.httpPort(), rumor.serviceManager(), rumor.localId(),
                        rumor::getClusterState, System.currentTimeMillis(),
                        inferenceService, fileDownloadService, shared);
                httpServer.start();
                System.out.println("HTTP server running on port " + config.httpPort());
            } catch (IOException e) {
                System.err.println("Failed to start HTTP server: " + e.getMessage());
            }
        }

        System.out.println();
        System.out.println("Node " + rumor.localId() + " (" + config.nodeType() + ") is running.");
        System.out.println("Shared directory: " + shared);
        System.out.println("Commands: ls, hello, a (ask-local), ask-remote, files, download, quit");
        System.out.println("Press Ctrl+C to cancel a running service.");
        System.out.println();

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("rumor> ");
            if (!scanner.hasNextLine()) break;
            String line = scanner.nextLine().trim();

            switch (line) {
                case "" -> {}
                case "ls" -> printTopology(rumor);
                case "hello" -> sendHello(rumor, helloService);
                case "ask-local", "a" -> sendInference(scanner, inferenceService, true);
                case "ask-remote" -> sendInference(scanner, inferenceService, false);
                case "files" -> discoverFiles(fileDownloadService);
                case "download" -> downloadFile(scanner, fileDownloadService, shared);
                case "quit", "exit" -> {
                    if (httpServer != null) httpServer.stop();
                    rumor.stop();
                    System.out.println("Goodbye.");
                    return;
                }
                default -> System.out.println("Unknown command: " + line
                        + " (try: ls, hello, a, ask-remote, files, download, quit)");
            }
        }
    }

    private static void sendHello(Rumor rumor, HelloService helloService) {
        byte[] request = ("Hello from " + rumor.localId() + "!").getBytes(StandardCharsets.UTF_8);
        CountDownLatch done = new CountDownLatch(1);
        ServiceHandle handle = helloService.dispatch(request, event -> {
            switch (event) {
                case RequestEvent.Processing p ->
                        System.out.println("Request sent, waiting for response...");
                case RequestEvent.Succeeded s -> {
                    byte[] data = s.raw();
                    String reply = data != null
                            ? new String(data, StandardCharsets.UTF_8) : "";
                    System.out.println("Response: " + reply);
                    done.countDown();
                }
                case RequestEvent.Failed f -> {
                    System.out.println("Request failed: " + f.reason());
                    done.countDown();
                }
                default -> {}
            }
        });

        activeHandle = handle;
        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            activeHandle = null;
        }
    }

    private static void printTopology(Rumor rumor) {
        Map<NodeId, EndpointState> states = rumor.getClusterState();
        System.out.println();
        System.out.printf("%-25s %-8s %-10s %-10s %s%n",
                "NODE", "TYPE", "STATUS", "HEARTBEAT", "APP STATE");
        System.out.println("-".repeat(80));

        for (var entry : states.entrySet()) {
            NodeId id = entry.getKey();
            EndpointState state = entry.getValue();

            String type = getAppValue(state, "NODE_TYPE", "?");
            String status = getAppValue(state, "STATUS", "?");
            String self = id.equals(rumor.localId()) ? " (self)" : "";

            StringBuilder extra = new StringBuilder();
            for (var app : state.appStates().entrySet()) {
                String key = app.getKey();
                if (!key.equals("NODE_TYPE") && !key.equals("STATUS") && !key.equals("SERVICES")) {
                    if (!extra.isEmpty()) extra.append(", ");
                    extra.append(key).append("=").append(app.getValue().value());
                }
            }

            VersionedValue servicesVv = state.getAppState("SERVICES");
            String services = (servicesVv != null && !servicesVv.value().isEmpty())
                    ? servicesVv.value() : "-";
            if (!extra.isEmpty()) extra.append(", ");
            extra.append("services=[").append(services).append("]");

            System.out.printf("%-25s %-8s %-10s %-10d %s%s%n",
                    id, type, status, state.heartbeatVersion(), extra, self);
        }
        System.out.println();
    }

    private static String getAppValue(EndpointState state, String key, String defaultVal) {
        VersionedValue vv = state.getAppState(key);
        return vv != null ? vv.value() : defaultVal;
    }

    // --- Inference command ---

    private static void sendInference(Scanner scanner, InferenceService inferenceService, boolean local) {
        System.out.print("prompt> ");
        if (!scanner.hasNextLine()) return;
        String prompt = scanner.nextLine().trim();
        if (prompt.isEmpty()) {
            System.out.println("Empty prompt, skipping.");
            return;
        }

        InferenceRequest request = new InferenceRequest(prompt);
        CountDownLatch done = new CountDownLatch(1);

        OnStateChange callback = event -> {
            switch (event) {
                case RequestEvent.Processing p ->
                        System.out.println((local ? "Running locally..." : "Sending to remote peer..."));
                case RequestEvent.StreamData d -> {
                    System.out.print(new String(d.raw(), StandardCharsets.UTF_8));
                }
                case RequestEvent.Succeeded s -> {
                    System.out.println();
                    done.countDown();
                }
                case RequestEvent.Failed f -> {
                    System.out.println("\nInference failed: " + f.reason());
                    done.countDown();
                }
                case RequestEvent.Cancelled c -> {
                    System.out.println("\nInference cancelled.");
                    done.countDown();
                }
            }
        };

        ServiceHandle handle;
        if (local) {
            handle = inferenceService.request(request, callback);
        } else {
            handle = inferenceService.dispatch(request, callback);
        }

        activeHandle = handle;
        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            activeHandle = null;
        }
    }

    // --- File discovery command (reads gossip state, no network request) ---

    private static void discoverFiles(FileDownloadService fileDownloadService) {
        Map<NodeId, String> peerFiles = fileDownloadService.discoverFiles();
        if (peerFiles.isEmpty()) {
            System.out.println("\nNo remote files found (no peers with shared files).");
            return;
        }

        System.out.println("\nRemote files:");
        for (var entry : peerFiles.entrySet()) {
            System.out.println("  " + entry.getKey() + ":");
            for (String fileEntry : entry.getValue().split(",")) {
                if (fileEntry.isEmpty()) continue;
                int colon = fileEntry.lastIndexOf(':');
                if (colon > 0) {
                    String name = fileEntry.substring(0, colon);
                    String size = fileEntry.substring(colon + 1);
                    System.out.printf("    %-30s %s bytes%n", name, size);
                } else {
                    System.out.println("    " + fileEntry);
                }
            }
        }
        System.out.println();
    }

    private static Path uniquePath(Path path) {
        if (!Files.exists(path)) return path;

        String name = path.getFileName().toString();
        String base;
        String ext;
        int dot = name.lastIndexOf('.');
        if (dot > 0) {
            base = name.substring(0, dot);
            ext = name.substring(dot);
        } else {
            base = name;
            ext = "";
        }

        Path parent = path.getParent();
        int i = 1;
        Path candidate;
        do {
            candidate = parent.resolve(base + "_" + i + ext);
            i++;
        } while (Files.exists(candidate));
        return candidate;
    }

    // --- File download command ---

    private static void downloadFile(Scanner scanner, FileDownloadService fileDownloadService, Path sharedDir) {
        System.out.print("remote file path> ");
        if (!scanner.hasNextLine()) return;
        String remotePath = scanner.nextLine().trim();
        if (remotePath.isEmpty()) {
            System.out.println("No file specified, skipping.");
            return;
        }

        Path outputPath = uniquePath(sharedDir.resolve(Path.of(remotePath).getFileName()).toAbsolutePath());
        CountDownLatch done = new CountDownLatch(1);

        try {
            Files.createDirectories(outputPath.getParent());
            FileOutputStream fos = new FileOutputStream(outputPath.toFile());

            ServiceHandle handle = fileDownloadService.downloadFrom(remotePath, event -> {
                switch (event) {
                    case RequestEvent.Processing p ->
                            System.out.println("Downloading...");
                    case RequestEvent.StreamData d -> {
                        try {
                            fos.write(d.raw());
                        } catch (IOException e) {
                            System.out.println("Write error: " + e.getMessage());
                        }
                    }
                    case RequestEvent.Succeeded s -> {
                        try { fos.close(); } catch (IOException ignored) {}
                        System.out.println("Download complete: " + outputPath);
                        done.countDown();
                    }
                    case RequestEvent.Failed f -> {
                        try { fos.close(); } catch (IOException ignored) {}
                        System.out.println("Download failed: " + f.reason());
                        done.countDown();
                    }
                    case RequestEvent.Cancelled c -> {
                        try { fos.close(); } catch (IOException ignored) {}
                        System.out.println("Download cancelled.");
                        done.countDown();
                    }
                }
            });

            activeHandle = handle;
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            System.out.println("Could not open output file: " + e.getMessage());
        } finally {
            activeHandle = null;
        }
    }

    // --- Argument parsing ---

    private static RumorConfig parseArgs(String[] args) {
        RumorConfig config = new RumorConfig();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port", "-p" -> config.port(Integer.parseInt(args[++i]));
                case "--host", "-h" -> config.host(args[++i]);
                case "--type", "-t" -> config.nodeType(NodeType.fromString(args[++i]));
                case "--seed", "-s" -> {
                    String[] parts = args[++i].split(":");
                    config.addSeed(parts[0], Integer.parseInt(parts[1]));
                }
                case "--request-timeout" -> config.requestTimeoutMs(Long.parseLong(args[++i]));
                case "--idle-timeout" -> config.requestIdleTimeoutMs(Long.parseLong(args[++i]));
                case "--shared-dir" -> {
                    String dir = args[++i];
                    if (dir.startsWith("~")) dir = System.getProperty("user.home") + dir.substring(1);
                    sharedDir = Path.of(dir);
                }
                case "--debug-port" -> config.debugPort(Integer.parseInt(args[++i]));
                case "--http-port" -> config.httpPort(Integer.parseInt(args[++i]));

                case "--help" -> {
                    printUsage();
                    System.exit(0);
                }
                default -> {
                    System.err.println("Unknown argument: " + args[i]);
                    printUsage();
                    System.exit(1);
                }
            }
        }

        return config;
    }

    private static void printUsage() {
        System.out.println("""
                Usage: rumor [options]

                Options:
                  --port, -p <port>                          Listen port (default: 7000)
                  --host, -h <host>                          Listen host (default: 127.0.0.1)
                  --type, -t <seed|basic|eviction|master>    Node type (default: basic)
                  --seed, -s <host:port>                     Seed node address (repeatable)
                  --request-timeout <ms>                     Overall request timeout in ms (default: 30000)
                  --idle-timeout <ms>                        Idle timeout between data messages in ms (default: 10000)
                  --shared-dir <path>                        Shared file directory (default: ~/mosaic-shared)
                  --debug-port <port>                        Debug HTTP server port (disabled if not set)
                  --http-port <port>                         Mosaic web UI + API port (disabled if not set)

                  --help                                     Show this help

                Node types:
                  seed       Bootstrap node — other nodes connect here first
                  basic      Standard participant node
                  eviction   Monitors heartbeats and marks unresponsive nodes DOWN
                  master     Combined seed + eviction node

                Examples:
                  # Start a master node (seed + evictor)
                  --port 7001 --type master

                  # Start a basic node connecting to the master
                  --port 7002 --type basic --seed 127.0.0.1:7001
                """);
    }
}
