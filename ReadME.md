# Sluice

A crash-safe log pipeline in Java: **agent → ingestion server → indexed storage**.

Sluice is a learning project that rebuilds the core ideas behind log shippers like
Filebeat, Fluent Bit and the Datadog Agent: tailing log files, batching lines, and
delivering them reliably to a central server where they can be searched.

> 🚧 **Status:** work in progress. The agent tails and batches log lines and streams
> them to the server, which decodes them. Durable storage and acknowledgements are next.

## Architecture

```
            AGENT                                              SERVER
┌─────────────────────────────────────────────┐      ┌──────────────────────────────┐
│ app.log ─► LogTailer ─► LineBatcher ─► BatchSender ──TCP──► accept loop          │
│            (lines)      (size/time)    (JSON lines)  │      └─► one virtual thread │
└─────────────────────────────────────────────┘      │          per client         │
                                                     │          decodes LogBatch   │
                                                     └──────────────────────────────┘
```

Agent stages are connected through `java.util.function.Consumer`, so each stage only
knows about the next one, and stages can be added or swapped without changing the others.

## What works today

### Agent
- **`LogTailer`** follows a log file like `tail -f` and emits only **complete lines**.
  If a writer has only written half a line, the tailer waits for the newline instead
  of splitting one event into two. Handles `\n` and `\r\n` line endings.
- **`LineBatcher`** flushes a batch when it reaches **N lines** (throughput) **or**
  after **T ms** (latency), whichever comes first. Thread-safe: the tailer thread and
  the timer thread share one lock.
- **`BatchSender`** wraps each batch in a `LogBatch` with the agent ID and an
  increasing sequence number, and sends it to the server as one line of JSON.

### Server
- Accepts many agents at once, with **one virtual thread per connection**.
- Decodes each JSON line back into a `LogBatch`.
- Errors are handled at the right level: a malformed message is logged and skipped,
  a broken connection ends only that client, and a failed `accept()` doesn't stop
  the server.

## Wire format

Each batch is sent as one line of JSON ([JSON Lines](https://jsonlines.org/)):

```json
{"agentId":"agent-1","seq":3,"lines":["2026-09-28 10:00:21 INFO  [cache] cache miss key=user:4821", "..."]}
```

The newline at the end of each line marks where one message ends and the next
begins. JSON escapes any special characters inside log lines, so they can't break
the framing.

## Build and run

Requires **Java 21+** and **Maven**. Both programs are built as self-contained jars.

```bash
mvn package

# terminal 1
java -jar server/target/server-0.1.0-SNAPSHOT.jar

# terminal 2
java -jar agent/target/agent-0.1.0-SNAPSHOT.jar app.log
```

Append lines to `app.log` from another terminal and watch them arrive at the server
in batches.

To send a message by hand (on Windows PowerShell use `curl.exe`):

```bash
curl telnet://localhost:9000
{"agentId":"manual","seq":1,"lines":["typed by hand"]}
```

## Project layout

```
sluice/
├── common/   LogBatch: the message shared by agent and server
├── agent/    LogTailer, LineBatcher, BatchSender, Agent (entry point)
└── server/   Server (entry point)
```

## Roadmap

- [x] Tail a log file, emitting only complete lines
- [x] Batch lines by size and time
- [x] Multi-client TCP server (virtual thread per connection)
- [x] Send batches to the server as JSON lines
- [ ] Write-ahead log with fsync before acknowledging
- [ ] Acknowledgements, retries and de-duplication (at-least-once delivery)
- [ ] Backpressure: bounded queue + token-bucket rate limiting
- [ ] Inverted index and search
- [ ] Compressed storage segments and background compaction
- [ ] Benchmarks

## Design notes

- **Partial lines:** `BufferedReader.readLine()` treats end-of-file as a line ending,
  which splits lines that are still being written. The tailer reads character by
  character and only emits a line once it sees `\n`.
- **Size + time batching:** size keeps throughput high and bounds batch size under
  load; the timer bounds latency when traffic is quiet. This is the same approach as
  Kafka's `batch.size` and `linger.ms`.
- **Framing:** TCP is a byte stream with no message boundaries. Sending one JSON
  document per line gives each batch a clear boundary that `readLine()` can find.
- **Virtual threads:** each connection spends most of its time waiting for data.
  Virtual threads make one-thread-per-connection cheap enough for thousands of agents.