package org.rumor;

import org.rumor.exchange.Exchange;
import org.rumor.gossip.EndpointState;
import org.rumor.gossip.NodeId;
import org.rumor.gossip.VersionedValue;
import org.rumor.node.NodeConfig;
import org.rumor.node.NodeType;
import org.rumor.node.RumorNode;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Scanner;

/**
 * TODO: move node picking logic inside messafe handlers itself
 * 
 */
/**
 *
 *   # Start a master(seed+eviction) node on port 7001
 *   mvn exec:java -Dexec.args="--port 7001 --type master"
 *
 *   # Start a basic node on port 7002, connecting to the seed
 *   mvn exec:java -Dexec.args="--port 7002 --type basic --seed 127.0.0.1:7001"
 *
 * Console commands:
 *   ls       - Show network topology (all known nodes, heartbeat, state)
 *   hello    - Send a hello exchange to the best available node
 *   quit     - Shut down this node
 */
public class Main {

    public static void main(String[] args) throws Exception {
        NodeConfig config = parseArgs(args);
        RumorNode node = new RumorNode(config);

        // Register the "hello" exchange type — all nodes will discover and offer this
        node.registerExchange("hello", (metadata, exchange) -> {
            String request = new String(metadata, StandardCharsets.UTF_8);
            System.out.println();
            System.out.println(">> Incoming 'hello' exchange from remote peer: " + request);

            try {
                InputStream in = exchange.getInputStream();
                byte[] data = in.readAllBytes();
                String message = new String(data, StandardCharsets.UTF_8);
                System.out.println(">> Received: " + message);

                OutputStream out = exchange.getOutputStream();
                String response = "Hello back from " + node.localId() + "! Got your message.";
                out.write(response.getBytes(StandardCharsets.UTF_8));
                out.close();

                System.out.println(">> Sent response back");
                System.out.print("rumor> ");
            } catch (Exception e) {
                System.err.println(">> Exchange error: " + e.getMessage());
            }
        });

        node.start();

        System.out.println();
        System.out.println("Node " + node.localId() + " (" + config.nodeType() + ") is running.");
        System.out.println("Commands: ls, hello, quit");
        System.out.println();

        // Interactive console loop
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("rumor> ");
            if (!scanner.hasNextLine()) break;
            String line = scanner.nextLine().trim();

            switch (line) {
                case "" -> {}
                case "ls" -> printTopology(node);
                case "hello" -> sendHello(node);
                case "quit", "exit" -> {
                    node.stop();
                    System.out.println("Goodbye.");
                    return;
                }
                default -> System.out.println("Unknown command: " + line
                        + " (try: ls, hello, quit)");
            }
        }
    }

    private static void printTopology(RumorNode node) {
        Map<NodeId, EndpointState> states = node.getClusterState();
        System.out.println();
        System.out.printf("%-25s %-8s %-10s %-10s %s%n",
                "NODE", "TYPE", "STATUS", "HEARTBEAT", "APP STATE");
        System.out.println("-".repeat(80));

        for (var entry : states.entrySet()) {
            NodeId id = entry.getKey();
            EndpointState state = entry.getValue();

            String type = getAppValue(state, "NODE_TYPE", "?");
            String status = getAppValue(state, "STATUS", "?");
            String self = id.equals(node.localId()) ? " (self)" : "";

            // Collect extra app state keys (skip NODE_TYPE, STATUS, and EXCHANGES)
            StringBuilder extra = new StringBuilder();
            for (var app : state.appStates().entrySet()) {
                String key = app.getKey();
                if (!key.equals("NODE_TYPE") && !key.equals("STATUS") && !key.equals("EXCHANGES")) {
                    if (!extra.isEmpty()) extra.append(", ");
                    extra.append(key).append("=").append(app.getValue().value());
                }
            }

            // Show exchange types offered by this node
            VersionedValue exchangesVv = state.getAppState("EXCHANGES");
            String exchanges = (exchangesVv != null && !exchangesVv.value().isEmpty())
                    ? exchangesVv.value() : "-";
            if (!extra.isEmpty()) extra.append(", ");
            extra.append("exchanges=[").append(exchanges).append("]");

            System.out.printf("%-25s %-8s %-10s %-10d %s%s%n",
                    id, type, status, state.heartbeatVersion(), extra, self);
        }
        System.out.println();
    }

    private static final Set<String> INFRA_TYPES = Set.of("SEED", "MASTER", "EVICTION");

    private static void sendHello(RumorNode node) {
        // Pick the best node: prefer BASIC nodes over infrastructure (seed/master/eviction),
        // then highest heartbeat (most recently active), exclude self
        NodeId bestPeer = null;
        long bestHeartbeat = -1;
        boolean bestIsBasic = false;

        for (var entry : node.getClusterState().entrySet()) {
            NodeId id = entry.getKey();
            if (id.equals(node.localId())) continue;

            EndpointState state = entry.getValue();
            String status = getAppValue(state, "STATUS", "");
            if (!"ALIVE".equals(status)) continue;

            String nodeType = getAppValue(state, "NODE_TYPE", "");
            boolean isBasic = !INFRA_TYPES.contains(nodeType);

            // Prefer basic nodes; among same tier, pick highest heartbeat
            if (isBasic && !bestIsBasic) {
                bestPeer = id;
                bestHeartbeat = state.heartbeatVersion();
                bestIsBasic = true;
            } else if (isBasic == bestIsBasic && state.heartbeatVersion() > bestHeartbeat) {
                bestPeer = id;
                bestHeartbeat = state.heartbeatVersion();
            }
        }

        if (bestPeer == null) {
            System.out.println("No active peers available. Wait for gossip to discover nodes.");
            return;
        }

        System.out.println("Sending hello exchange to " + bestPeer + "...");

        final NodeId target = bestPeer;
        try {
            byte[] metadata = ("HELLO from " + node.localId()).getBytes(StandardCharsets.UTF_8);
            Exchange exchange = node.startExchange(target, "hello", metadata);

            OutputStream out = exchange.getOutputStream();
            String message = "Hello World from " + node.localId() + "!";
            out.write(message.getBytes(StandardCharsets.UTF_8));
            out.close();

            InputStream in = exchange.getInputStream();
            byte[] responseBytes = in.readAllBytes();
            String response = new String(responseBytes, StandardCharsets.UTF_8);
            System.out.println("Response from " + target + ": " + response);

        } catch (Exception e) {
            System.out.println("Exchange failed: " + e.getMessage());
        }
    }

    private static String getAppValue(EndpointState state, String key, String defaultVal) {
        VersionedValue vv = state.getAppState(key);
        return vv != null ? vv.value() : defaultVal;
    }

    // --- Argument parsing ---

    private static NodeConfig parseArgs(String[] args) {
        NodeConfig config = new NodeConfig();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port", "-p" -> config.port(Integer.parseInt(args[++i]));
                case "--host", "-h" -> config.host(args[++i]);
                case "--type", "-t" -> config.nodeType(NodeType.fromString(args[++i]));
                case "--seed", "-s" -> {
                    String[] parts = args[++i].split(":");
                    config.addSeed(parts[0], Integer.parseInt(parts[1]));
                }
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
