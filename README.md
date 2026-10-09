# Sluice

A crash-safe log pipeline in Java: **agent → ingestion server → indexed storage**.

Sluice is a learning project that rebuilds the core ideas behind log shippers like
Filebeat, Fluent Bit and the Datadog Agent: tailing log files, batching lines, and
delivering them reliably to a central server where they can be searched.

> 🚧 **Status:** work in progress. The agent tails, batches and sends log lines; the
> server stores each batch durably, acknowledges it, and skips duplicates. Next: the
> agent waits for acknowledgements and resends on failure.

## Architecture

```
            AGENT                                              SERVER
┌──────────────────────────────────────────────┐      ┌───────────────────────────────┐
│ app.log ─► LogTailer ─► LineBatcher ─► BatchSender ──TCP──► accept loop             │
│            (lines)      (size/time)    (JSON lines) ◄─ACK──  └─► virtual thread     │
└──────────────────────────────────────────────┘      │            per client         │
                                                      │            └─► LogStore       │
                                                      │                logs.dat       │
                                                      └───────────────────────────────┘
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
- **Durable storage:** `LogStore` appends each batch to `logs.dat` with a single
  write, then calls `force()` (fsync) so the data is on disk, not just in the
  operating system's cache.
- **Acknowledgements:** the server replies `ACK <seq>` **only after** the batch is
  safely on disk. An ACK is a durability guarantee, not just a receipt.
- **De-duplication:** the server remembers the highest sequence number stored per
  agent. A batch at or below it is a duplicate: it is not stored again, but is still
  acknowledged, so a resend after a lost ACK doesn't create duplicate lines.
- Errors are handled at the right level: a malformed message is logged and skipped,
  a broken connection ends only that client, a failed disk write means no ACK, and a
  failed `accept()` doesn't stop the server.

## Protocol

**Agent → server:** one batch per line of JSON ([JSON Lines](https://jsonlines.org/)):

```json
{"agentId":"agent-1","seq":3,"lines":["2026-09-28 10:00:21 INFO  [cache] cache miss key=user:4821", "..."]}
```

**Server → agent:** one acknowledgement per line, sent after the batch is on disk:

```
ACK 3
```

The newline at the end of each message marks where it ends. JSON escapes any special
characters inside log lines, so they can't break the framing.

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
and in `logs.dat`.

### Testing the server by hand (Windows PowerShell)

```powershell
$client = New-Object System.Net.Sockets.TcpClient("localhost", 9000)
$stream = $client.GetStream()
$writer = New-Object System.IO.StreamWriter($stream); $writer.AutoFlush = $true
$reader = New-Object System.IO.StreamReader($stream)

$writer.WriteLine('{"agentId":"test","seq":1,"lines":["hello"]}'); $reader.ReadLine()   # ACK 1
$writer.WriteLine('{"agentId":"test","seq":1,"lines":["hello"]}'); $reader.ReadLine()   # ACK 1 (duplicate, not stored again)
$client.Close()
```

## Project layout

```
sluice/
├── common/   LogBatch: the message shared by agent and server
├── agent/    LogTailer, LineBatcher, BatchSender, Agent (entry point)
└── server/   Server (entry point), LogStore (durable storage + de-duplication)
```

## Roadmap

- [x] Tail a log file, emitting only complete lines
- [x] Batch lines by size and time
- [x] Multi-client TCP server (virtual thread per connection)
- [x] Send batches to the server as JSON lines
- [x] Durable storage: write + fsync before acknowledging
- [x] Server acknowledgements and de-duplication by sequence number
- [ ] Agent waits for ACKs, reconnects with backoff and resends (at-least-once delivery)
- [ ] Rebuild de-duplication state from disk after a server restart
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
  document per line gives each message a clear boundary that `readLine()` can find.
- **Virtual threads:** each connection spends most of its time waiting for data.
  Virtual threads make one-thread-per-connection cheap enough for thousands of agents.
- **Durable before ACK:** `write()` only hands data to the operating system, which
  may keep it in memory. The server calls `force()` before acknowledging, so an ACK
  always means the batch survives a crash.
- **De-duplication by sequence number:** batches arrive in order (one in flight at a
  time), so the server only needs the highest stored `seq` per agent, constant memory,
  instead of remembering every batch. The check, the write and the update happen
  inside one `synchronized` method, so two connections can't both store the same batch.

## Known limitations

- The agent does not yet wait for ACKs or resend, so batches can still be lost if the
  server goes down. This is the next step.
- De-duplication state lives in memory and is lost when the server restarts.