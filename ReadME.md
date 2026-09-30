# Sluice

A crash-safe log pipeline in Java: **agent → ingestion server → indexed storage**.

Sluice is a learning project that rebuilds the core ideas behind log shippers like
Filebeat, Fluent Bit and the Datadog Agent: tailing log files, batching lines, and
delivering them reliably to a central server where they can be searched.

> 🚧 **Status:** work in progress. The agent can tail files and batch lines; the server is next.

## Architecture

```
app.log ──► LogTailer ──(line)──► LineBatcher ──(batch)──► [next: send to server]
             tails the file       size + time batching
```

Each stage hands its output to the next through a `java.util.function.Consumer`,
so stages can be added or swapped without changing the others.

## What works today

**`LogTailer`** follows a log file like `tail -f`:
- Emits only **complete lines**. If the writer has only written half a line, the
  tailer waits for the newline instead of splitting one event into two.
- Handles Windows (`\r\n`) and Unix (`\n`) line endings.
- Polls every 500 ms when idle. When lines are arriving it reads continuously, so
  polling adds no delay under load.
- Opens the file without blocking the application that writes to it.

**`LineBatcher`** groups lines into batches:
- Flushes when a batch reaches **N lines** (throughput) **or** after **T ms**
  (latency), whichever comes first.
- Thread-safe: the tailer thread and the timer thread share one lock, and full
  batches are handed off by swapping in a fresh list, so a sent batch is never modified.

## Build and run

Requires **Java 21+** and **Maven**.

```bash
mvn package
java -cp agent/target/agent-0.1.0-SNAPSHOT.jar io.github.mathewlobo.sluice.agent.Agent app.log
```

Then append lines to `app.log` from another terminal and watch them arrive in batches.

## Project layout

```
sluice/
├── common/   shared types (upcoming)
├── agent/    LogTailer, LineBatcher, Agent (entry point)
└── server/   ingestion server (upcoming)
```

## Roadmap

- [x] Tail a log file, emitting only complete lines
- [x] Batch lines by size and time
- [ ] Send batches to the server over TCP
- [ ] Acknowledgements, retries and de-duplication (at-least-once delivery)
- [ ] Write-ahead log with fsync before acknowledging
- [ ] Backpressure: bounded queue + token-bucket rate limiting
- [ ] Inverted index and search
- [ ] Compressed storage segments and background compaction
- [ ] Benchmarks

## Design notes

- **Partial lines:** `BufferedReader.readLine()` treats end-of-file as a line ending,
  which splits lines that are still being written. The tailer reads character by
  character and only emits a line once it sees `\n`.
- **Size + time batching:** size keeps throughput high and bounds batch size under load;
  the timer bounds latency when traffic is quiet. This is the same approach as
  Kafka's `batch.size` and `linger.ms`.