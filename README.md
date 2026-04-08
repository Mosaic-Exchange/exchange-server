# Rumor

A distributed service framework built on gossip-based cluster membership and Netty transport.

## Quick Start

```bash
# Start a master node (seed + evictor) on port 7001
mvn exec:java -Dexec.args="--port 7001 --type master"

# Start a basic node connecting to the master
mvn exec:java -Dexec.args="--port 7002 --type basic --seed 127.0.0.1:7001"

# Start a basic node with the web UI
mvn exec:java -Dexec.args="--port 7002 --type basic --seed 127.0.0.1:7001 --http-port 8080"
```

### CLI Options

| Flag | Description | Default |
|---|---|---|
| `--port`, `-p` | Listen port | `7000` |
| `--host`, `-h` | Listen address | `127.0.0.1` |
| `--type`, `-t` | Node type: `seed`, `basic`, `eviction`, `master` | `basic` |
| `--seed`, `-s` | Seed node `host:port` (repeatable) | — |
| `--request-timeout` | Overall request timeout (ms) | `30000` |
| `--idle-timeout` | Idle timeout between data chunks (ms) | `10000` |
| `--shared-dir` | Shared file directory | `~/mosaic-shared` |
| `--http-port` | Web UI + API port (disabled if omitted) | — |
| `--debug-port` | Debug HTTP server port (disabled if omitted) | — |

### Node Types

- **seed** — bootstrap node; other nodes connect here first
- **basic** — standard participant
- **eviction** — monitors heartbeats, marks unresponsive nodes DOWN
- **master** — seed + eviction combined

## Programmatic Setup

```java
RumorConfig config = new RumorConfig();
config.port(7001).addSeed("127.0.0.1", 7000);

Rumor rumor = new Rumor(config);
rumor.register(new MyService());
rumor.start();
```

## Writing a Service

Extend `RService` and implement `serve()`. For the common byte[] case, use raw types — no
generic parameters needed on the class:

```java
public class EchoService extends RService {

    @Override
    public void serve(ServiceRequest request, ServiceResponse response) {
        byte[] data = request.raw();   // raw() returns byte[] — no cast needed
        response.write(data);          // echo back
        response.close();              // signal completion
    }
}
```

### Custom Request/Response Types

For custom types, parameterize `RService` and the `serve()` method:

```java
public class GreetService extends RService<GreetRequest, GreetResponse> {

    @Override
    public void serve(ServiceRequest<GreetRequest> request,
                      ServiceResponse<GreetResponse> response) {
        GreetRequest req = request.data();       // typed — no cast needed
        GreetResponse resp = new GreetResponse("Hello, " + req.name());
        response.write(resp);                    // typed — no cast needed
        response.close();
    }
}
```

The framework discovers types from the method signature at registration time and handles
serialization/deserialization transparently.

**Serialization rules:**
- `byte[]` — identity passthrough (zero cost, the default)
- Any `Serializable` type — Java serialization (when no custom codec is specified)
- Custom codec — via `@Codec` annotation (see below)
- **Local requests bypass serialization entirely** — the object is passed directly

### Custom Codecs

If you want control over serialization, implement `ServiceCodec<T>` and reference it with
`@Codec` on the `serve()` parameters:

```java
public class GreetService extends RService<GreetRequest, GreetResponse> {

    static class ReqCodec implements ServiceCodec<GreetRequest> {
        @Override public byte[] encode(GreetRequest value) { /* your logic */ }
        @Override public GreetRequest decode(byte[] data)  { /* your logic */ }
    }

    static class RespCodec implements ServiceCodec<GreetResponse> {
        @Override public byte[] encode(GreetResponse value) { /* your logic */ }
        @Override public GreetResponse decode(byte[] data)  { /* your logic */ }
    }

    @Override
    public void serve(@Codec(ReqCodec.class) ServiceRequest<GreetRequest> request,
                      @Codec(RespCodec.class) ServiceResponse<GreetResponse> response) {
        // ...
    }
}
```

Request and response codecs are independent — you can use a custom codec for one and the
default for the other.

### Convenience `raw()` Methods — No Casts for byte[] Services

For the default byte[] case, convenience methods eliminate all casts:

**In `serve()`** — use `request.raw()` instead of `(byte[]) request.data()`:

```java
public void serve(ServiceRequest request, ServiceResponse response) {
    byte[] data = request.raw();    // returns byte[] directly
    response.write(data);           // write() accepts Object — works as-is
    response.close();
}
```

You can also use `response.writeRaw(bytes)` for an explicit raw-byte write that bypasses
the codec (equivalent to `write()` for byte[] services, but makes the intent clear for
typed services that want to write pre-encoded bytes).

**In callbacks** — use `d.raw()` and `s.raw()` instead of `(byte[]) d.data()`:

```java
echoService.dispatch("hello".getBytes(), event -> {
    switch (event) {
        case RequestEvent.StreamData d -> {
            byte[] chunk = d.raw();   // no cast needed
        }
        case RequestEvent.Succeeded s -> {
            byte[] result = s.raw();  // no cast needed
        }
        // ...
    }
});
```

For **typed services**, use `data()` for typed access (cast needed in the callback since
it returns `Object` through raw types):

```java
greetService.dispatch(new GreetRequest("Alice"), event -> {
    switch (event) {
        case RequestEvent.Succeeded s -> {
            GreetResponse resp = (GreetResponse) s.data();   // cast needed
            System.out.println(resp.message());
        }
        // ...
    }
});
```

**Summary:**

| Context | byte[] services | Typed services |
|---|---|---|
| `serve()` — reading request | `request.raw()` | `request.data()` — typed, no cast |
| `serve()` — writing response | `response.write(bytes)` | `response.write(typedObj)` — typed |
| Callback — `StreamData` | `d.raw()` | `(MyResp) d.data()` — cast needed |
| Callback — `Succeeded` | `s.raw()` | `(MyResp) s.data()` — cast needed |
| Callback — `Failed` | `f.reason()` — always `String` | Same |

### Streaming Services

Annotate with `@Streamable` to enable multi-write streaming. The framework runs `serve()` on a
dedicated thread — blocking is safe:
c
```java
@Streamable
public class GenerateService extends RService<String, String> {

    @Override
    public void serve(ServiceRequest<String> request, ServiceResponse<String> response) {
        for (String token : generateTokens(request.data())) {
            response.write(token);    // each write sends a typed chunk
        }
        response.close();
    }
}
```

**Stream data is fully typed.** Each `response.write(T)` call is individually serialized
(for remote calls) or passed directly (for local calls). On the caller side, each
`StreamData.data()` carries the decoded typed object:

```java
generateService.dispatch("tell me a joke", event -> {
    switch (event) {
        case RequestEvent.StreamData d -> {
            String token = (String) d.data();   // each chunk is typed
            System.out.print(token);
        }
        case RequestEvent.Succeeded s ->
            System.out.println("\n[done]");
        // ...
    }
});
```

For byte[] streaming services (like file downloads), cast `d.data()` to `byte[]`.

### Calling a Service

`RService` gives you two calling methods: `request` for local execution and `dispatch` for
remote.

**Remote** — dispatches a request to a random healthy peer offering this service. Returns
immediately with a `ServiceHandle`; events arrive asynchronously via the callback:

```java
ServiceHandle handle = echoService.dispatch("hello".getBytes(), event -> {
    switch (event) {
        case RequestEvent.Processing p -> System.out.println("sent");
        case RequestEvent.StreamData d -> {
            byte[] chunk = (byte[]) d.data();
            System.out.println("chunk: " + chunk.length + " bytes");
        }
        case RequestEvent.Succeeded s  -> System.out.println("done");
        case RequestEvent.Failed f     -> System.err.println("error: " + f.reason());
    }
});
```

The callback runs on Rumor's network I/O thread — do not block inside it. Delegate any
blocking work to a separate thread.

**Local** — invokes `serve()` on this node. If a local executor pool is configured
(per-service or globally), the call is submitted asynchronously and returns immediately.
Without a pool it runs on the caller's thread synchronously:

```java
ServiceHandle handle = echoService.request("hello".getBytes(), event -> { /* same callback */ });
```

The same no-blocking rule applies to the callback.

**Filtered** — dispatch only to peers matching a predicate over their gossip app state:

```java
ServiceHandle handle = echoService.dispatch(request, callback, appState ->
    "true".equals(appState.get("HAS_GPU"))
);
```

### Cancellation

Every dispatch and request returns a `ServiceHandle`. Call `cancel()` to abort:

```java
ServiceHandle handle = inferenceService.dispatch(request, callback);

// Later (e.g. on Ctrl+C):
handle.cancel();
```

Cancellation sends `SERVICE_CANCEL` to the server, which interrupts any active streaming
thread.

## Event Model

Every outbound request emits events through `OnStateChange` in this order:

```
Processing -> StreamData* -> Succeeded | Failed
```

| Event | Meaning |
|---|---|
| `Processing` | Request sent (or started locally), awaiting response |
| `StreamData(data)` | A chunk of response data arrived (typed per your service's response type) |
| `Succeeded(data)` | Request completed successfully (data is the final response, or `null` for streaming) |
| `Failed(String)` | Request failed (reason included) |

## Publishing Application State

Annotate the class with `@MaintainState` and mark methods with `@StateKey` to gossip custom
key-value state across the cluster. The framework calls each method periodically and publishes
the result. Keys are automatically qualified as `ServiceClassName.KEY`:

```java
@MaintainState
public class GpuService extends RService {

    @StateKey("GPU_INFO")
    public String gpuInfo() { return "A100:80GB"; }

    // ...
}
```

Query state from other services via `ClusterView`. Use `qualifiedKey()` to build the correct
prefixed key:

```java
// Inside GpuService (or any service with access to clusterView()):
Map<NodeId, String> gpuInfo = clusterView().stateForKey(qualifiedKey("GPU_INFO"));
List<NodeId> gpuNodes = clusterView().findPeers(qualifiedKey("GPU_INFO"), v -> v.contains("A100"));
```

## Concurrency Control

Configure bounded thread pools per-service or globally to prevent one service from starving
others:

```java
// Per-service config (takes precedence over global)
rumor.register(inferenceService, new RService.Config()
        .remoteThreads(2)          // threads for inbound (remote) requests
        .remoteQueueCapacity(2)    // queue depth; 0 = fail-fast, no queuing
        .localThreads(2)           // threads for local requests
        .localQueueCapacity(2));

// Global config — shared by all services without a per-service config
rumor.globalServiceConfig(new RService.Config()
        .remoteThreads(4)
        .localThreads(4));
```

When a pool is at capacity, the request is rejected immediately with
`RequestEvent.Failed("Service at capacity")`. Remote and local pools are separate, so a flood
of inbound requests cannot starve local callers.

## Private / Public Mode

By default, every registered service is **public** — its name is gossiped to the cluster so
remote peers can discover and invoke it. You can toggle a service into **private mode** to stop
advertising it. The service remains registered locally and can still handle local `request()`
calls, but remote peers will no longer see it in the `SERVICES` gossip key.

```java
// Stop advertising to the cluster
inferenceService.enablePrivateMode();

// Use the service locally only
inferenceService.request(myRequest, callback);

// Re-advertise when ready
inferenceService.enablePublicMode();
```

Check current visibility:

```java
inferenceService.isPrivate();   // true if in private mode
```

The change takes effect immediately — the next gossip round will reflect the updated service
list. Peers that already have an open connection can still send requests to a private service
(the framework does not reject them), but new peer lookups via `dispatch()` on other nodes
will no longer route to this node for that service.

## Threading Model

| Context | Thread | Blocking? |
|---|---|---|
| `serve()` — default service | Netty I/O event loop | **Never block.** |
| `serve()` — `@Streamable` service | Dedicated stream-worker thread | Safe to block. |
| `serve()` — with `Config` remote pool | Service's remote executor thread | Safe to block. |
| `request()` — with `Config` local pool | Service's local executor thread | Safe to block. |
| `request()` — no pool | Caller's thread | Your responsibility. |
| `@StateKey` method | Framework background thread | Avoid heavy computation. |
| `OnStateChange` callbacks (remote) | Netty I/O event loop | **Never block.** |
| `OnStateChange` callbacks (local) | Same thread that called `request()` | Your responsibility. |

## Configuration

```java
RumorConfig config = new RumorConfig();
config.host("0.0.0.0")               // listen address (default: 127.0.0.1)
      .port(7000)                     // listen port (default: 7000)
      .nodeType(NodeType.SEED)        // SEED | BASIC | EVICTION | MASTER
      .addSeed("10.0.0.1", 7000)     // bootstrap peer(s)
      .gossipIntervalMs(1000)         // gossip round interval
      .requestTimeoutMs(30_000)       // overall request timeout
      .requestIdleTimeoutMs(10_000)   // idle timeout between chunks
      .httpPort(8080)                 // web UI port (0 = disabled)
      .debugPort(9090);               // debug HTTP server (0 = disabled)
```

## File-Based Debug Listener

Register a debug file to get a periodically refreshed snapshot of this node's metrics,
services, and cluster topology — no HTTP server required:

```java
// Write debug snapshot to a file every 2 seconds (default)
rumor.registerDebug(Path.of("/tmp/rumor-debug.txt"));

// Custom refresh interval (in milliseconds)
rumor.registerDebug(Path.of("/tmp/rumor-debug.txt"), 5000);
```

The file is **overwritten** (not appended) on each tick and includes:
- Node identity and uptime
- Pending outbound requests, active server streams, pending handshakes
- Per-request details (request ID, streaming flag, elapsed time)
- Per-service executor stats (active threads, pool size, queue depth, completed tasks)
- Full cluster topology as seen by this node (type, status, heartbeat, all app states)

Calling `registerDebug` again replaces the previous listener. The writer is automatically
stopped on `rumor.stop()`.

## Best Practices

1. **Don't block network I/O threads.** Configure `RService.Config` executor pools for any
   service that touches disk, network, or runs for more than a few milliseconds.
2. **Close your responses.** Always call `response.close()` in the success path. On errors, call
   `response.fail()` — do not call both.
3. **Use `@MaintainState` for discovery.** Publish metadata (available files, model names,
   capabilities) via gossip rather than making separate discovery RPCs.
4. **Filter dispatches.** Use the `Predicate<Map<String,String>>` overload of `dispatch()` to
   route to the right peer based on published gossip state, rather than discovering then
   dispatching separately.
5. **Stream large responses.** Call `response.write()` in chunks rather than buffering the entire
   response in memory.
6. **Handle all event types.** Always handle `Failed` in your `OnStateChange` callback —
   network partitions and timeouts happen.
7. **Qualify state keys.** When querying `ClusterView`, use `qualifiedKey("MY_KEY")` or the
   full `ServiceName.MY_KEY` string — raw keys will not match what the framework publishes.
8. **Prefer typed `serve()` parameters.** Use `ServiceRequest<MyType>` on the `serve()` method
   to get compile-time type safety inside the handler, even though the caller-side callback
   requires casts.
