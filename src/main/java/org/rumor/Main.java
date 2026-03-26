package org.rumor;

import org.rumor.gossip.EndpointState;
import org.rumor.gossip.NodeId;
import org.rumor.gossip.VersionedValue;
import org.rumor.node.NodeType;
import org.rumor.node.Rumor;
import org.rumor.node.RumorConfig;
import org.rumor.service.RService;
import org.rumor.service.RequestEvent;
import org.rumor.service.ServiceResponse;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

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
 *   quit     - Shut down this node
 */
public class Main {

    static class HelloService extends RService {
        
        
        @Override
        public void serve(byte[] request, ServiceResponse response) {
            String message = new String(request, StandardCharsets.UTF_8);
            System.out.println();
            System.out.println(">> Incoming hello request: " + message);

            String reply = "Hello back! Got your message: " + message;
            response.write(reply.getBytes(StandardCharsets.UTF_8));
            System.out.println(">> Sent response");
            System.out.print("rumor> ");
        }
    }

    public static void main(String[] args) throws Exception {
        RumorConfig config = parseArgs(args);
        Rumor rumor = new Rumor(config);

        HelloService helloService = new HelloService();
        rumor.register(helloService);

        rumor.start();

        System.out.println();
        System.out.println("Node " + rumor.localId() + " (" + config.nodeType() + ") is running.");
        System.out.println("Commands: ls, hello, quit");
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
                case "quit", "exit" -> {
                    rumor.stop();
                    System.out.println("Goodbye.");
                    return;
                }
                default -> System.out.println("Unknown command: " + line
                        + " (try: ls, hello, quit)");
            }
        }
    }

    private static void sendHello(Rumor rumor, HelloService helloService) {
        byte[] request = ("Hello from " + rumor.localId() + "!").getBytes(StandardCharsets.UTF_8);
        StringBuilder responseAccumulator = new StringBuilder();
        CountDownLatch done = new CountDownLatch(1);
        //This can be ran on a separate thread from ui so that
        //ui does not freez as this can take longer
        helloService.dispatch(
                request,
                (event) -> {
                    switch (event) {
                        case RequestEvent.Processing p ->
                                System.out.println("Request sent, waiting for response...");
                        case RequestEvent.StreamData d ->
                                responseAccumulator.append(new String(d.data(), StandardCharsets.UTF_8));
                        case RequestEvent.Succeeded s -> {
                            System.out.println("Response: " + responseAccumulator);
                            done.countDown();
                        }
                        case RequestEvent.Failed f -> {
                            System.out.println("Request failed: " + f.reason());
                            done.countDown();
                        }
                    }
                }
        );

        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
