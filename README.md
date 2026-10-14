# Sluice

A crash-safe log pipeline in Java: **agent → ingestion server → indexed storage**.

Sluice is a learning project that rebuilds the core ideas behind log shippers like
Filebeat, Fluent Bit and the Datadog Agent: tailing log files, batching lines, and
delivering them reliably to a central server, with no lost or duplicated lines even
when either side crashes.

> 🚧 **Status:** work in progress. Reliable delivery is complete end to end
> (at-least-once with de-duplication, surviving server and agent restarts).
> Next: backpressure, search, and an automated crash test.

## Architecture

```
            AGENT                                                SERVER
┌──────────────────────────────────────────────────┐   ┌────────────────────────────────┐
│ app.log ─► LogTailer ─► LineBatcher ─► BatchSender ─TCP─► accept loop                 │
│            (lines)      (size/time)    (JSON lines) ◄ACK─  └─► virtual thread/client  │
│                                            │       │   │        └─► LogStore           │
│                              agent-checkpoint.json │   │            logs.dat           │
└──────────────────────────────────────────────────┘   └────────────────────────────────┘
```

Agent stages are connected through `java.util.function.Consumer`, so each stage only
knows about the next one, and stages can be added or swapped without changing the others.

## Delivery guarantees

| Failure | What happens | Result |
|---|---|---|
| Server crashes or restarts | Agent keeps the unacknowledged batch, reconnects with exponential backoff and resends it with the **same** sequence number. Server rebuilds its de-duplication state from `logs.dat` on startup | No loss, no duplicates |
| ACK lost on the network | Agent times out and resends; server recognises the sequence number, skips storing, and ACKs again | No duplicates |
| Agent restarts | Agent loads its checkpoint, skips lines already shipped and continues the sequence | No loss, no duplicates |
| Crash while saving the checkpoint | Checkpoint is written to a temp file and atomically renamed, so the old one stays intact | No corruption |
| Server down at agent startup | Agent keeps retrying the first batch until the server appears | No loss |

In short: **at-least-once delivery with de-duplication**, so each batch is stored
effectively once.

## How it works

### Agent
- **`LogTailer`** follows a log file like `tail -f` and emits only **complete lines**.
  If a writer has only written half a line, it waits for the newline instead of
  splitting one event into two. On startup it skips the lines already shipped.
- **`LineBatcher`** flushes a batch when it reaches **N lines** (throughput) **or**
  after **T ms** (latency), whichever comes first. Thread-safe: the tailer thread and
  the timer thread share one lock.
- **`BatchSender`** numbers each batch, sends it as one JSON line, and **waits for
  `ACK <seq>`** (5 s timeout). On any failure it closes the connection, waits
  (1 s, 2 s, 4 s… up to 20 s) and resends the same batch. It never gives up on a batch.
- **`CheckpointStore`** saves `{linesShipped, lastSeq}` after every ACK, using a temp
  file plus an atomic rename, so a restart picks up exactly where it left off.

### Server
- Accepts many agents at once, with **one virtual thread per connection**.
- **`LogStore`** appends each batch to `logs.dat` as one JSON record, then calls
  `force()` (fsync) so it is on disk, not just in the operating system's cache.
- Replies `ACK <seq>` **only after** the batch is safely on disk. An ACK is a
  durability guarantee, not just a receipt.
- **De-duplication:** remembers the highest sequence number stored per agent. Batches
  at or below it are not stored again, but are still acknowledged.
- On startup, **rebuilds** that per-agent state by reading `logs.dat`, before accepting
  any connections. A half-written last record from a crash is skipped safely.
- Errors are handled at the right level: a malformed message is skipped, a broken
  connection ends only that client, a failed disk write means no ACK, and a failed
  `accept()` doesn't stop the server.

## Protocol

**Agent → server:** one batch per line of JSON ([JSON Lines](https://jsonlines.org/)):

```json
{"agentId":"agent-1","seq":3,"lines":["2026-09-28 10:00:21 INFO  [cache] cache miss key=user:4821", "..."]}
```

**Server → agent:** one acknowledgement per line, sent after the batch is on disk:

```
ACK 3
```

The same JSON records are what the server stores in `logs.dat`, so each stored batch
carries its agent ID and sequence number.

## Build and run

Requires **Java 21+** and **Maven**. Both programs are built as self-contained jars.

```bash
mvn package

# terminal 1
java -jar server/target/server-0.1.0-SNAPSHOT.jar

# terminal 2
java -jar agent/target/agent-0.1.0-SNAPSHOT.jar app.log
```

Append lines to `app.log` and watch them arrive in `logs.dat`. Try stopping and
restarting either program: nothing is lost or duplicated.

Runtime files (ignored by Git): `logs.dat` (server storage) and
`agent-checkpoint.json` (agent progress). Delete both for a clean start.

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
├── agent/    LogTailer, LineBatcher, BatchSender, Checkpoint, CheckpointStore, Agent (entry point)
└── server/   Server (entry point), LogStore (durable storage + de-duplication)
```

## Roadmap

- [x] Tail a log file, emitting only complete lines
- [x] Batch lines by size and time
- [x] Multi-client TCP server (virtual thread per connection)
- [x] Send batches to the server as JSON lines
- [x] Durable storage: write + fsync before acknowledging
- [x] Server acknowledgements and de-duplication by sequence number
- [x] Agent waits for ACKs, reconnects with backoff and resends
- [x] De-duplication state rebuilt from disk after a server restart
- [x] Agent checkpoint: resume after restart without loss or duplicates
- [ ] Backpressure: bounded queue + token-bucket rate limiting
- [ ] Inverted index and search
- [ ] Automated crash test (kill server and agent under load, verify every line exactly once)
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
  document per line gives each message a clear boundary.
- **Virtual threads:** each connection spends most of its time waiting for data, so
  one virtual thread per connection is cheap enough for thousands of agents.
- **Durable before ACK:** `write()` only hands data to the operating system. The server
  calls `force()` before acknowledging, so an ACK always means the batch survives a crash.
- **Same sequence number on retry:** a resent batch must look exactly like the original,
  or the server would store it twice.
- **Fresh connection on every retry:** a dead connection can't be revived, and reusing it
  after a timeout could pair a late ACK with the wrong batch.
- **Exponential backoff:** retrying 1 s, 2 s, 4 s… avoids hammering a server that is
  restarting.
- **Metadata stored with the data:** each record in `logs.dat` includes its agent ID and
  sequence number, so the batch and its de-duplication state are written in one
  operation and can never disagree. The log is the source of truth; in-memory state is
  rebuilt from it.
- **Record progress only after it happens:** the server updates its sequence map only
  after `force()`, and the agent updates its checkpoint only after the ACK. Doing either
  earlier turns a crash into data loss.
- **The log file is the buffer:** while the server is unreachable, the tailer simply
  pauses. New lines wait safely in `app.log` until delivery resumes.

## Known limitations

- **Log rotation:** the agent's checkpoint counts lines. If `app.log` is replaced with a
  new, shorter file, the count no longer matches. Filebeat solves this by tracking the
  file's identity and a byte offset.
- **Narrow resend window:** a crash after receiving an ACK but before saving the
  checkpoint resends one batch. It is usually de-duplicated, but if its batch boundaries
  come out differently, some lines could be duplicated or skipped.
- **Checkpoint save failures** stall the agent (it keeps resending) rather than losing data.
- **Server startup** reads the whole of `logs.dat` to rebuild state, which gets slower
  as the file grows. Periodic snapshots or segment files would fix this.
- **One batch in flight:** stop-and-wait keeps things simple and ordered, but limits
  throughput. Pipelining several unacknowledged batches is a future improvement.