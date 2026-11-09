# Sluice

A crash-safe log pipeline in Java: **agent → ingestion server → durable storage**.

Sluice is a learning project that rebuilds the core ideas behind log shippers like
Filebeat, Fluent Bit and the Datadog Agent: tailing log files, batching lines, and
delivering them reliably to a central server, with no lost or duplicated lines even
when either side crashes, and without falling over when many agents send at once.

> **Status:** level 1 complete. Reliable delivery is proven by an automated chaos
> test (random server and agent kills: 0 lines lost, 0 duplicated), and the server
> uses group commit and backpressure, measured with a custom load generator
> (**4.1× throughput** at 50 concurrent agents).
> Next level: binary WAL, segment storage with compaction, and search.

## Architecture

```
            AGENT                                                  SERVER
┌──────────────────────────────────────────────┐   ┌──────────────────────────────────────────────┐
│ app.log ─► LogTailer ─► LineBatcher ─► BatchSender ─TCP─► virtual thread per client            │
│            (lines)      (size/time)    (JSON lines) ◄ACK/BUSY─   │                              │
│                                            │       │   │        ▼  bounded queue (1000)        │
│                              agent-checkpoint.json │   │   ┌─────────────────────────┐          │
└──────────────────────────────────────────────┘   │   │ ONE writer thread:      │          │
                                                    │   │ take + drain up to 100  │          │
                                                    │   │ write all → ONE fsync   │──► logs.dat
                                                    │   │ → wake waiting clients  │          │
                                                    │   └─────────────────────────┘          │
                                                    └──────────────────────────────────────────────┘
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
| Server overloaded (write queue full) | Server replies `BUSY <seq>` immediately; agent backs off and resends the same batch | No loss, no timeouts |
| Disk write or fsync fails | No ACK is sent; the store refuses further writes until restart, then rebuilds from what is really on disk | No silent loss |

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

## Performance

### Group commit

Originally every batch was written and fsynced on its own, inside one lock: 50 waiting
batches meant 50 fsyncs, one after another. The load test showed the result: going from
10 to 50 agents added only 17% throughput but made every agent wait 4× longer.

Now client threads don't write at all. Each one puts its batch and a
`CompletableFuture` into a bounded queue and waits. A **single writer thread** takes
everything that is waiting (up to 100 batches), writes them all, calls `force()`
**once**, and only then completes the futures, which lets each client send its ACK.

- **Light load:** a group is 1 batch, so latency stays low.
- **Heavy load:** many batches share one fsync, so throughput scales.
- **Durability is unchanged:** no ACK is sent before the group's `force()` returns.

### Backpressure

The write queue is **bounded** (1,000 batches). When it is full, the server doesn't
make clients wait silently (they would hit their 5 s timeout, reconnect and resend,
creating a retry storm). Instead it replies **`BUSY <seq>`** immediately. The agent keeps
the connection, backs off and resends the same batch.

### Benchmarks

Measured with the `loadgen` module: N simulated agents, each sending batches of 100
lines as fast as the server ACKs them, for 20 seconds. Latency is the time from first
sending a batch to receiving its ACK, including any retries.

| Agents | Before (one fsync per batch) | After (group commit) | p50 before → after | p99 before → after |
|---|---|---|---|---|
| 1 | 28,879 lines/s | 38,525 lines/s | 2.9 → 1.9 ms | 7.5 → 10.9 ms |
| 10 | 82,836 lines/s | **299,535 lines/s (3.6×)** | 11.1 → **2.7 ms** | 31.4 → **8.8 ms** |
| 50 | 97,299 lines/s | **398,474 lines/s (4.1×)** | 46.5 → **10.9 ms** | 137.7 → **52.1 ms** |

0 timeouts in every run. Single runs on a Windows laptop, so numbers vary between
runs (a repeat of the 50-agent run reached 539,440 lines/s).

**Overload test** (queue capacity temporarily set to 5, 50 agents): 9,951 `BUSY`
replies and **0 timeouts**. p50 stayed at 2.9 ms; p99 rose to 265 ms because of the
back-off pauses. Overloaded agents are told immediately and slow down, instead of
timing out and retrying.

## How it works

### Agent
- **`LogTailer`** follows a log file like `tail -f` and emits only **complete lines**.
  If a writer has only written half a line, it waits for the newline instead of
  splitting one event into two. On startup it skips the lines already shipped.
- **`LineBatcher`** flushes a batch when it reaches **N lines** (throughput) **or**
  after **T ms** (latency), whichever comes first. Thread-safe: the tailer thread and
  the timer thread share one lock.
- **`BatchSender`** gives each batch a sequence number and its `firstLine`, sends it as
  one JSON line, and **waits for `ACK <seq>`** (5 s timeout). On a failure it closes the
  connection, waits (1 s, 2 s, 4 s… up to 20 s) and resends the same batch. On `BUSY`
  it keeps the connection and backs off the same way. It never gives up on a batch.
- **`CheckpointStore`** saves `{linesShipped, lastSeq}` after every ACK, using a temp
  file plus an atomic rename, so a restart resumes close to where it left off.

### Server
- Accepts many agents at once, with **one virtual thread per connection**.
- **`LogStore`** stores each batch in `logs.dat` as one JSON record, using **group
  commit**: one writer thread, one `force()` (fsync) per group of batches.
- Replies `ACK <seq>` **only after** the batch is safely on disk. An ACK is a
  durability guarantee, not just a receipt.
- **De-duplication by line position:** remembers the last line stored per agent.
  - all lines already stored → store nothing, still ACK
  - partly overlapping → store only the new lines
  - a gap (lines missing before this batch) → log a loud warning and store
- Replies `BUSY <seq>` when the write queue is full.
- On startup, **rebuilds** its per-agent state by reading `logs.dat`, before accepting
  any connections. A half-written last record from a crash is skipped safely.
- Errors are handled at the right level: a malformed message is skipped, a broken
  connection ends only that client, a failed disk write means no ACK, and a failed
  `accept()` doesn't stop the server.

## Protocol

**Agent → server:** one batch per line of JSON ([JSON Lines](https://jsonlines.org/)):

```json
{"agentId":"agent-1","seq":3,"firstLine":11,"lines":["2026-09-28 10:00:21 INFO  [cache] cache miss key=user:4821", "..."]}
```

- `seq` identifies the batch, so a reply can be matched to it.
- `firstLine` is the line number (1-based) of the first line in `app.log`; it is what
  de-duplication uses.

**Server → agent:** one reply per batch:

```
ACK 3      the batch is on disk (or was already stored)
BUSY 3     the server is overloaded: nothing was stored, back off and resend
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

### Run the tests

```bash
mvn test
```

Unit tests for `LogStore` cover a new batch, an exact duplicate, a duplicate after a
restart, a partial overlap and a gap.

### Run the crash test

```bash
mvn package
java -jar crashtest/target/crashtest-0.1.0-SNAPSHOT.jar
```

Run it from the project root, with no other Sluice server running on port 9000.
Logs from each run (`app.log`, `logs.dat`, `server.out`, `agent.out`, the checkpoint)
are kept in `crash-test-run/` until the next run.

### Run the load generator

Start the server in an empty folder, then:

```bash
java -jar loadgen/target/loadgen-0.1.0-SNAPSHOT.jar [agents] [durationSec] [batchSize]
java -jar loadgen/target/loadgen-0.1.0-SNAPSHOT.jar 50 20 100
```

```
Agents: 50  Duration: 20s  Batch: 100
Lines ACKed: 7,976,300  Throughput: 398,474 lines/s
Batches: 79763  Timeouts: 0  BUSY: 0
Latency  p50: 10.9 ms  p99: 52.1 ms  max: 297.8 ms
```

Delete `logs.dat` and restart the server between runs so results are comparable.

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
├── server/      Server (entry point), LogStore (group commit, de-duplication), StoreBusyException
├── crashtest/   CrashTest: chaos test that kills server/agent and verifies every line
└── loadgen/     LoadGen: simulates many agents, reports throughput and latency
```

## Roadmap

**Level 1 (done)**
- [x] Tail a log file, emitting only complete lines
- [x] Batch lines by size and time
- [x] Multi-client TCP server (virtual thread per connection)
- [x] Durable storage: write + fsync before acknowledging
- [x] ACKs, retries with backoff, and de-duplication
- [x] De-duplication state rebuilt from disk after a server restart
- [x] Agent checkpoint: resume after restart without loss or duplicates
- [x] Automated crash test (random server and agent kills under load)
- [x] De-duplication by line position (fixes a data-loss bug found by the crash test)
- [x] Unit tests for `LogStore`
- [x] Load generator and benchmarks
- [x] Group commit: one fsync per group of batches
- [x] Backpressure: bounded queue + `BUSY` replies

**Next level**
- [ ] Binary write-ahead log with length prefix and CRC32 checksums
- [ ] Segment files with compression, background compaction and retention
- [ ] Inverted index and search (`level=ERROR AND "timeout"`), HTTP query API
- [ ] Per-agent rate limiting (token bucket)
- [ ] Metrics (Prometheus + Grafana) and Docker Compose
- [ ] gRPC streaming instead of JSON over raw TCP

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
- **Group commit:** fsync is the expensive part (about 1 ms here), and it costs roughly
  the same for one batch or a hundred. Sharing one fsync across every batch that is
  waiting turns that fixed cost into throughput. The writer uses `take()` (wait for at
  least one) plus `drainTo()` (grab the rest without waiting), so groups grow and
  shrink with the load on their own.
- **Progress only after it's durable:** inside a group, de-duplication progress goes
  into a temporary `pending` map, copied to the real state only after `force()`
  succeeds. If the fsync fails, nothing is marked as stored, so resends are written
  again instead of being wrongly treated as duplicates.
- **Fail hard on fsync errors:** after a failed fsync, you can't know what reached the
  disk. Like PostgreSQL, the store stops accepting writes instead of retrying; a restart
  rebuilds state from what is actually in the file.
- **Tell overloaded clients early:** `offer()` instead of `put()` means a full queue
  answers `BUSY` immediately, rather than letting clients wait until they time out and
  retry. Explicit backpressure prevents retry storms.
- **Identical retries:** a resent batch keeps its sequence number and `firstLine`, so the
  server can recognise it.
- **De-duplicate by position, not by batch:** batch boundaries depend on timing and can
  change after a restart, but a line's position in the file doesn't. The agent's
  checkpoint is only an efficiency hint: if it falls behind, the server simply discards
  lines it already has.
- **Fresh connection after a failure:** a dead connection can't be revived, and reusing it
  after a timeout could pair a late ACK with the wrong batch.
- **Exponential backoff with jitter:** retrying 1 s, 2 s, 4 s… avoids hammering a server
  that is restarting; random pauses stop clients that were rejected together from all
  coming back together.
- **Metadata stored with the data:** each record in `logs.dat` includes its agent ID,
  sequence number and `firstLine`, so the batch and its de-duplication state are written
  in one operation and can never disagree. The log is the source of truth; in-memory
  state is rebuilt from it.
- **The log file is the buffer:** while the server is unreachable, the tailer simply
  pauses. New lines wait safely in `app.log` until delivery resumes.
- **Measure, don't guess:** the load generator showed that one fsync per batch, one at a
  time, was the bottleneck before any optimisation was written, and confirmed the gain
  afterwards. The crash test kills processes at random moments rather than chosen ones,
  which is how it found a bug that manual testing missed.

## Known limitations

- **Log rotation:** the agent counts lines. If `app.log` is replaced with a new, shorter
  file, line numbers no longer match. Filebeat solves this by tracking the file's
  identity and a byte offset.
- **Gaps are reported, not repaired:** if the server sees a batch that starts past the
  next expected line, it logs a warning but cannot recover the missing lines.
- **Storage format:** records are JSON lines in a single file. Server startup reads the
  whole file to rebuild state, which gets slower as it grows. Segment files and a binary
  format are planned for the next level.
- **One batch in flight per agent:** stop-and-wait keeps things simple and ordered, but
  limits a single agent's throughput.
- **After a failed fsync** the store refuses all writes until the server is restarted.
- **Thousands of simultaneous connections** can overflow the operating system's accept
  backlog; agents retry with backoff.
- **No per-agent rate limiting yet:** the bounded queue protects the server, but one very
  busy agent can still take a larger share than others.