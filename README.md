# Sluice

A crash-safe log pipeline in Java: **agent → ingestion server → indexed storage**.

Sluice is a learning project that rebuilds the core ideas behind log shippers like
Filebeat, Fluent Bit and the Datadog Agent: tailing log files, batching lines, and
delivering them reliably to a central server, with no lost or duplicated lines even
when either side crashes.

> 🚧 **Status:** work in progress. Reliable delivery is complete end to end and
> proven by an automated chaos test that randomly kills the server and agent while
> logs are flowing (10/10 runs: 0 lines lost, 0 duplicated).
> Next: backpressure and search.

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
| Server crashes or restarts | Agent keeps the unacknowledged batch, reconnects with exponential backoff and resends it. Server rebuilds its de-duplication state from `logs.dat` on startup | No loss, no duplicates |
| ACK lost on the network | Agent times out and resends; server sees the lines are already stored, skips them, and ACKs again | No duplicates |
| Agent restarts | Agent loads its checkpoint, skips lines already shipped and continues | No loss, no duplicates |
| Agent killed after an ACK but before saving its checkpoint | Agent resends lines the server already has, possibly batched differently; server stores only the lines it doesn't have yet | No loss, no duplicates |
| Crash while saving the checkpoint | Checkpoint is written to a temp file and atomically renamed, so the old one stays intact | No corruption |
| Server down at agent startup | Agent keeps retrying the first batch until the server appears | No loss |

In short: **at-least-once delivery with de-duplication by line position**, so every
line is stored exactly once.

## Crash test

The `crashtest` module checks these guarantees automatically:

1. Starts the server and agent as separate processes in a clean `crash-test-run/` folder.
2. Writes 5,000 numbered lines (`line 000001` … `line 005000`) to `app.log`.
3. **Chaos:** while lines are being written, every 2–6 seconds it hard-kills
   (`destroyForcibly`) either the server or the agent at random, and restarts it a
   second later.
4. Stops the chaos, waits for the agent's checkpoint to reach 5,000, then kills both.
5. Reads every record in `logs.dat` and counts each line number: reports
   **Missing**, **Duplicates**, **Corrupt** records and **PASS/FAIL** (exit code 0/1).

```
Kill #1: server
Kill #2: agent
...
Chaos stopped after 12 kills
Delivered (per checkpoint): 5000 / 5000
Written: 5000  Stored: 5000  Missing: 0  Duplicates: 0  Corrupt: 0  Bad lines: 0
PASS
```

### A bug it found

An early run failed with exactly one line missing (`3969`), even though the agent's
checkpoint said all 5,000 had been shipped. The evidence in `logs.dat` and `server.out`:

- Batch `#827` had been stored with **4** lines (3965–3968), because the time-based flush fired early.
- The agent was then killed **after** the ACK but **before** saving its checkpoint.
- After restart it rebuilt batch `#827`, but this time with **5** lines (3965–3969).
- The server, which de-duplicated by sequence number, saw `#827` again and dropped
  the whole batch, so line 3969 was never stored.

The root cause was the assumption that *the same sequence number always means the
same lines*. That holds for retries within one agent run, but not after a restart.

**Fix:** each batch now carries `firstLine`, its line number in `app.log`. The server
tracks the last line it has stored per agent and keeps only the lines past it. A batch
can be fully new, fully duplicate, or partly overlapping, and each case is handled.
This is the same idea as Kafka's idempotent producer. After the fix, 10 runs in a row passed.

## How it works

### Agent
- **`LogTailer`** follows a log file like `tail -f` and emits only **complete lines**.
  If a writer has only written half a line, it waits for the newline instead of
  splitting one event into two. On startup it skips the lines already shipped.
- **`LineBatcher`** flushes a batch when it reaches **N lines** (throughput) **or**
  after **T ms** (latency), whichever comes first. Thread-safe: the tailer thread and
  the timer thread share one lock.
- **`BatchSender`** gives each batch a sequence number and its `firstLine`, sends it as
  one JSON line, and **waits for `ACK <seq>`** (5 s timeout). On any failure it closes
  the connection, waits (1 s, 2 s, 4 s… up to 20 s) and resends the same batch. It never
  gives up on a batch.
- **`CheckpointStore`** saves `{linesShipped, lastSeq}` after every ACK, using a temp
  file plus an atomic rename, so a restart resumes close to where it left off.

### Server
- Accepts many agents at once, with **one virtual thread per connection**.
- **`LogStore`** appends each batch to `logs.dat` as one JSON record, then calls
  `force()` (fsync) so it is on disk, not just in the operating system's cache.
- Replies `ACK <seq>` **only after** the batch is safely on disk. An ACK is a
  durability guarantee, not just a receipt.
- **De-duplication by line position:** remembers the last line stored per agent.
  - all lines already stored → store nothing, still ACK
  - partly overlapping → store only the new lines
  - a gap (lines missing before this batch) → log a loud warning and store
- On startup, **rebuilds** that per-agent state by reading `logs.dat`, before accepting
  any connections. A half-written last record from a crash is skipped safely.
- Errors are handled at the right level: a malformed message is skipped, a broken
  connection ends only that client, a failed disk write means no ACK, and a failed
  `accept()` doesn't stop the server.

## Protocol

**Agent → server:** one batch per line of JSON ([JSON Lines](https://jsonlines.org/)):

```json
{"agentId":"agent-1","seq":3,"firstLine":11,"lines":["2026-09-28 10:00:21 INFO  [cache] cache miss key=user:4821", "..."]}
```

- `seq` identifies the batch, so an ACK can be matched to it.
- `firstLine` is the line number (1-based) of the first line in `app.log`; it is what
  de-duplication uses.

**Server → agent:** one acknowledgement per line, sent after the batch is on disk:

```
ACK 3
```

The same JSON records are what the server stores in `logs.dat`. If only part of a
batch was new, the stored record holds just those lines with an adjusted `firstLine`,
so every record says exactly which lines it contains.

## Build and run

Requires **Java 21+** and **Maven**. All programs are built as self-contained jars.

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

### Run the crash test

```bash
mvn package
java -jar crashtest/target/crashtest-0.1.0-SNAPSHOT.jar
```

Run it from the project root. Output and logs from each run (`app.log`, `logs.dat`,
`server.out`, `agent.out`, the checkpoint) are kept in `crash-test-run/` until the
next run.

### Testing the server by hand (Windows PowerShell)

```powershell
$client = New-Object System.Net.Sockets.TcpClient("localhost", 9000)
$stream = $client.GetStream()
$writer = New-Object System.IO.StreamWriter($stream); $writer.AutoFlush = $true
$reader = New-Object System.IO.StreamReader($stream)

$writer.WriteLine('{"agentId":"test","seq":1,"firstLine":1,"lines":["hello"]}'); $reader.ReadLine()   # ACK 1
$writer.WriteLine('{"agentId":"test","seq":1,"firstLine":1,"lines":["hello"]}'); $reader.ReadLine()   # ACK 1 (duplicate, not stored again)
$client.Close()
```

## Project layout

```
sluice/
├── common/      LogBatch: the message shared by agent and server
├── agent/       LogTailer, LineBatcher, BatchSender, Checkpoint, CheckpointStore, Agent (entry point)
├── server/      Server (entry point), LogStore (durable storage + de-duplication)
└── crashtest/   CrashTest: chaos test that kills server/agent and verifies every line
```

## Roadmap

- [x] Tail a log file, emitting only complete lines
- [x] Batch lines by size and time
- [x] Multi-client TCP server (virtual thread per connection)
- [x] Send batches to the server as JSON lines
- [x] Durable storage: write + fsync before acknowledging
- [x] Server acknowledgements and de-duplication
- [x] Agent waits for ACKs, reconnects with backoff and resends
- [x] De-duplication state rebuilt from disk after a server restart
- [x] Agent checkpoint: resume after restart without loss or duplicates
- [x] Automated crash test (random server and agent kills under load, verify every line exactly once)
- [x] De-duplication by line position (fixes a data-loss bug found by the crash test)
- [ ] Unit tests for `LogStore`
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
  document per line gives each message a clear boundary.
- **Virtual threads:** each connection spends most of its time waiting for data, so
  one virtual thread per connection is cheap enough for thousands of agents.
- **Durable before ACK:** `write()` only hands data to the operating system. The server
  calls `force()` before acknowledging, so an ACK always means the batch survives a crash.
- **Identical retries:** a resent batch keeps its sequence number and `firstLine`, so the
  server can recognise it.
- **De-duplicate by position, not by batch:** batch boundaries depend on timing and can
  change after a restart, but a line's position in the file doesn't. The server decides
  what to store by line number, so correctness doesn't depend on the agent rebuilding
  batches exactly the same way. The agent's checkpoint is now only an efficiency hint:
  if it falls behind, the server simply discards lines it already has.
- **Fresh connection on every retry:** a dead connection can't be revived, and reusing it
  after a timeout could pair a late ACK with the wrong batch.
- **Exponential backoff:** retrying 1 s, 2 s, 4 s… avoids hammering a server that is
  restarting.
- **Metadata stored with the data:** each record in `logs.dat` includes its agent ID,
  sequence number and `firstLine`, so the batch and its de-duplication state are written
  in one operation and can never disagree. The log is the source of truth; in-memory
  state is rebuilt from it.
- **Record progress only after it happens:** the server updates its last-stored line only
  after `force()`, and the agent updates its checkpoint only after the ACK. Doing either
  earlier turns a crash into data loss.
- **The log file is the buffer:** while the server is unreachable, the tailer simply
  pauses. New lines wait safely in `app.log` until delivery resumes.
- **Test the failure, not just the happy path:** the crash test kills processes at random
  moments rather than chosen ones, which is how it found a bug that manual testing missed.

## Known limitations

- **Log rotation:** the agent counts lines. If `app.log` is replaced with a new, shorter
  file, line numbers no longer match. Filebeat solves this by tracking the file's
  identity and a byte offset.
- **Gaps are reported, not repaired:** if the server ever sees a batch that starts past
  the next expected line, it logs a warning but cannot recover the missing lines. The
  agent has no way to rewind yet.
- **Checkpoint save failures** make the agent resend batches; the server discards the
  duplicates, but a persistent failure means repeated resends.
- **Server startup** reads the whole of `logs.dat` to rebuild state, which gets slower
  as the file grows. Periodic snapshots or segment files would fix this.
- **One batch in flight:** stop-and-wait keeps things simple and ordered, but limits
  throughput. Pipelining several unacknowledged batches is a future improvement.