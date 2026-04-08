# Rumor Example: Distributed LLM Inference & File Sharing

A sample application built on the Rumor framework that demonstrates:

- **LLM inference** — stream tokens from an Ollama model, locally or on a remote peer
- **File sharing** — publish a shared directory via gossip and download files from peers
- **Service discovery** — use gossip state to find which peers have GPUs, files, etc.

## Prerequisites

- **Java 21+**
- **Maven 3.9+**
- **Ollama** — local LLM runtime (required for inference commands)

## Ollama Setup

1. Install Ollama:

   ```bash
   # Linux
   curl -fsSL https://ollama.com/install.sh | sh

   # macOS
   brew install ollama
   ```

2. Start the Ollama server:

   ```bash
   ollama serve
   ```

3. Pull a model (the example defaults to `llama3.2`):

   ```bash
   ollama pull llama3.2
   ```

   You can use any model — pass `--model <name>` or change the default in `InferenceService.java`.

4. Verify it works:

   ```bash
   ollama run llama3.2 "Hello"
   ```

## Project Setup

These example files live outside the framework's Maven source tree. To run them,
copy them into your own project that depends on the Rumor JAR, or temporarily place
them under the framework's `src/main/java/` directory:

```bash
# From the repository root
mkdir -p src/main/java/example
cp example/*.java src/main/java/example/
```

Then add a `package example;` declaration to each file and update the `pom.xml`
exec plugin to point at `example.ExampleApp`:

```xml
<configuration>
    <mainClass>example.ExampleApp</mainClass>
</configuration>
```

Build:

```bash
mvn compile
```

## Running

### Start a master node (seed + eviction)

```bash
mvn exec:java -Dexec.args="--port 7001 --type master"
```

### Start a basic node connecting to the master

```bash
mvn exec:java -Dexec.args="--port 7002 --type basic --seed 127.0.0.1:7001"
```

### Multi-host setup

Bind to a routable address so other machines can connect:

```bash
mvn exec:java -Dexec.args="--port 7001 --type master --host 192.168.1.10"
```

On the other machine:

```bash
mvn exec:java -Dexec.args="--port 7001 --type basic --seed 192.168.1.10:7001"
```

## CLI Options

| Flag | Description | Default |
|---|---|---|
| `--port`, `-p` | Listen port | `7000` |
| `--host`, `-h` | Listen address | `127.0.0.1` |
| `--type`, `-t` | Node type: `seed`, `basic`, `eviction`, `master` | `basic` |
| `--seed`, `-s` | Seed node `host:port` (repeatable) | -- |
| `--request-timeout` | Overall request timeout (ms) | `30000` |
| `--idle-timeout` | Idle timeout between data chunks (ms) | `10000` |
| `--shared-dir` | Shared file directory | `~/mosaic-shared` |

## Console Commands

Once a node is running, you get an interactive prompt:

| Command | Description |
|---|---|
| `ls` | Show cluster topology (all known nodes, heartbeat, state) |
| `hello` | Send a hello request to a random remote peer |
| `a` / `ask-local` | Run LLM inference locally via Ollama |
| `ask-remote` | Run LLM inference on a remote peer |
| `files` | Discover files available on remote peers (reads gossip state) |
| `download` | Download a file from a remote peer |
| `quit` | Shut down this node |

Press **Ctrl+C** during a running service call to cancel it.

## Example Files

| File | Description |
|---|---|
| `ExampleApp.java` | Entry point — CLI parsing, node setup, interactive console |
| `InferenceService.java` | Streaming service that calls the Ollama HTTP API |
| `InferenceRequest.java` | Request record for the inference service |
| `FileDownloadService.java` | Streaming file server + gossip-based file discovery |

## How It Works

1. **`InferenceService`** is a `@Streamable` service. When `serve()` is called, it sends
   the prompt to Ollama's `/api/generate` endpoint and streams each token back via
   `response.write()`. The service is registered with a bounded executor pool (2 threads,
   queue of 2) to prevent overloading.

2. **`FileDownloadService`** uses `@MaintainState` and `@StateKey("SHARED_FILES")` to
   periodically publish the list of files in the shared directory into gossip state.
   Remote peers can read this state without making a network request. When a download
   is requested, the framework automatically routes to a peer that has the file using
   a `dispatch()` filter predicate.

3. **`HelloService`** (defined inline in `ExampleApp.java`) is a minimal request/response
   service that echoes back a message — useful for verifying cluster connectivity.
