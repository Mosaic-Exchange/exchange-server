# Rumor

A distributed service framework built on gossip-based cluster membership and Netty transport.

## Quick Start

```java
// Bootstrap a node
RumorConfig config = new RumorConfig();
config.port(7001).addSeed("127.0.0.1", 7000);

Rumor rumor = new Rumor(config);
rumor.start();
rumor.register(new MyService());
```

## Writing a Service

Extend `RService` and implement `serve()`:

```java
public class EchoService extends RService {

    @Override
    public void serve(byte[] request, ServiceResponse response) {
        response.write(request);  // echo back
        response.close();         // signal completion
    }
}
```

### Calling a Service

**Remote** -- dispatches to a random healthy peer offering this service:

```java
echoService.dispatch("hello".getBytes(), event -> {
    switch (event) {
        case RequestEvent.Processing p   -> System.out.println("sent");
        case RequestEvent.StreamData d   -> System.out.println("chunk: " + d.data().length + " bytes");
        case RequestEvent.Succeeded s    -> System.out.println("done");
        case RequestEvent.Failed f       -> System.err.println("error: " + f.reason());
    }
});
```

**Local** -- calls `serve()` on the caller's thread:

```java
echoService.request("hello".getBytes(), event -> { /* same callback */ });
```

**Filtered** -- dispatch only to peers matching a predicate over their gossip app state:

```java
echoService.dispatch(request, callback, appState ->
    "true".equals(appState.get("HAS_GPU"))
);
```

## Event Model

Every outbound request (local or remote) emits events through `OnStateChange`:

| Event | Meaning |
|---|---|
| `Processing` | Request sent, awaiting response |
| `StreamData(byte[])` | A chunk of response data arrived |
| `Succeeded` | Request completed successfully |
| `Failed(String)` | Request failed (reason included) |

Events always follow: `Processing -> StreamData* -> Succeeded | Failed`.

## Publishing Application State

Implement `StatePublisher` to gossip custom key-value state across the cluster:

```java
public class GpuService extends RService implements StatePublisher {

    @Override
    public String stateKey() { return "GPU_INFO"; }

    @Override
    public String computeState() { return "A100:80GB"; }

    // ...
}
```

`computeState()` is called periodically on a background thread. Other nodes query it via `ClusterView`:

```java
Map<NodeId, String> gpuInfo = clusterView().stateForKey("GPU_INFO");
List<NodeId> gpuNodes = clusterView().findPeers("GPU_INFO", v -> v.contains("A100"));
```

## Threading Model

| Context | Thread | Rule |
|---|---|---|
| `serve()` | Netty I/O event loop | **Never block.** No `Thread.sleep`, no blocking I/O, no long-held locks. |
| `computeState()` | Framework background thread | Safe to do light work; avoid heavy computation. |
| `OnStateChange` callbacks (remote) | Netty I/O event loop | Same as `serve()` -- don't block. |
| `OnStateChange` callbacks (local) | Caller's thread | Blocking is your problem. |

For blocking or long-running work inside `serve()`, offload to your own executor:

```java
@Override
public void serve(byte[] request, ServiceResponse response) {
    myExecutor.submit(() -> {
        byte[] result = heavyComputation(request);
        response.write(result);
        response.close();
    });
}
```

### Backpressure

`ServiceResponse.write()` respects Netty channel writability. If the remote consumer is slow, writes from a non-event-loop thread will block until the channel is writable again. This is another reason to offload streaming writes to a dedicated thread.

## Configuration

```java
RumorConfig config = new RumorConfig();
config.host("0.0.0.0")              // listen address (default: 127.0.0.1)
      .port(7000)                    // listen port (default: 7000)
      .nodeType(NodeType.SEED)       // SEED | BASIC | EVICTION | MASTER
      .addSeed("10.0.0.1", 7000)    // bootstrap peer(s)
      .gossipIntervalMs(1000)        // gossip round interval
      .requestTimeoutMs(300_000)     // overall request timeout
      .requestIdleTimeoutMs(60_000)  // idle timeout between chunks
      .maxRequestBytes(1_048_576)    // max inbound request (1 MB)
      .maxResponseBytes(268_435_456);// max response (256 MB)
```

### Node Types

- **SEED** -- bootstrap node; other nodes connect here first
- **BASIC** -- standard participant
- **EVICTION** -- monitors heartbeats, marks unresponsive nodes DOWN
- **MASTER** -- SEED + EVICTION combined

## Best Practices

1. **Don't block the event loop.** This is the single most important rule. Offload anything that touches disk, network, or takes more than a few milliseconds.
2. **Close your responses.** Always call `response.close()` -- even in error paths. Without it, the remote caller hangs until timeout.
3. **Use `StatePublisher` for discovery.** Publish metadata (available files, model names, capabilities) via gossip instead of making discovery RPCs.
4. **Filter dispatches.** Use the `Predicate<Map<String,String>>` overload of `dispatch()` to route requests to the right peer based on their published state, rather than discovering first then dispatching.
5. **Stream large responses.** Call `response.write()` in chunks rather than buffering the entire response in memory.
6. **Handle all event types.** Your `OnStateChange` callback should handle `Failed` -- network partitions and timeouts happen.
