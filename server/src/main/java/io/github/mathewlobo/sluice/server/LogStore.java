package io.github.mathewlobo.sluice.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.mathewlobo.sluice.common.LogBatch;

/**
 * Durable, de-duplicating storage for log batches, with GROUP COMMIT.
 *
 * Many client threads call append() at the same time. Each one puts its batch
 * in a bounded queue and waits. ONE writer thread takes everything waiting,
 * writes it all, calls force() (fsync) ONCE for the whole group, and only then
 * tells each client its batch is durable, so it can send the ACK.
 *
 * Before: 50 waiting batches = 50 fsyncs, one after another.
 * Now:    50 waiting batches = 1 fsync.
 */
public class LogStore implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int QUEUE_CAPACITY = 1000;   // bounded queue: at most this many batches waiting
    private static final int MAX_GROUP = 100;         // at most this many batches per fsync

    /** One request from a client thread: the batch, plus the "buzzer" the writer completes. */
    private record PendingWrite(LogBatch batch, CompletableFuture<Boolean> result) { }

    private final Path filePath;
    private final FileChannel fileChannel;

    // Last line stored ON DISK per agent. Only the writer thread touches it after startup.
    private final Map<String, Long> lastStoredLine = new HashMap<>();

    private final BlockingQueue<PendingWrite> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final Thread writerThread;

    // Set after a failed write/force: from then on every write is refused (no ACK).
    // volatile = a change made by one thread is seen immediately by others.
    private volatile boolean failed = false;

    public LogStore(Path filePath) throws IOException {
        this.filePath = Objects.requireNonNull(filePath, "File path cannot be null");

        // 1. Rebuild per-agent progress from the file (the file is the source of truth)
        if (Files.exists(this.filePath)) {
            try (BufferedReader bf = Files.newBufferedReader(this.filePath, StandardCharsets.UTF_8)) {
                String line;
                while ((line = bf.readLine()) != null) {
                    if (line.isBlank()) continue;
                    try {
                        LogBatch record = MAPPER.readValue(line, LogBatch.class);
                        long end = record.firstLine() + record.lines().size() - 1;   // last line this record holds
                        lastStoredLine.merge(record.agentId(), end, Math::max);
                    } catch (JsonProcessingException e) {
                        System.err.println("Skipping unreadable record: " + e.getOriginalMessage());
                    }
                }
            }
        }
        System.out.println("Recovered state for " + lastStoredLine.size() + " agent(s): " + lastStoredLine);

        // 2. Open the file for appending
        fileChannel = FileChannel.open(this.filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.WRITE);

        // 3. Start the writer LAST, once everything it uses is ready
        this.writerThread = new Thread(this::writerLoop, "group-commit");
        this.writerThread.start();
    }

    /**
     * Called by many client threads at once (NOT synchronized).
     * Hands the batch to the writer thread and blocks until it is durable.
     * Returns true if new lines were stored, false if it was a full duplicate.
     * Throws IOException if the write failed, so Server does not send an ACK.
     */
    public boolean append(LogBatch batch) throws IOException {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        try {
            queue.put(new PendingWrite(batch, future));   // waits if the queue is full
            return future.get();                          // waits until the writer completes it
        } catch (ExecutionException e) {
            // The writer failed this batch (e.g. force() threw) -> no ACK
            throw new IOException("Write failed for batch " + batch.seq(), e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();           // restore the interrupt flag
            throw new IOException("Interrupted while waiting for batch " + batch.seq(), e);
        }
    }

    /**
     * Runs on the ONE writer thread until close() interrupts it.
     * Each round: collect a group, write all, ONE force(), then record progress
     * and complete the futures (which lets the client threads send their ACKs).
     */
    private void writerLoop() {
        List<PendingWrite> group = new ArrayList<>();
        try {
            while (true) {
                // a. Wait for at least one batch, then grab whatever else is ALREADY waiting.
                //    Light load -> group of 1 (low latency). Heavy load -> up to MAX_GROUP (one fsync).
                group.add(queue.take());
                queue.drainTo(group, MAX_GROUP - 1);

                // After a fatal write error, refuse everything (but never leave a client waiting)
                if (failed) {
                    for (PendingWrite pw : group) {
                        pw.result().completeExceptionally(new IOException("store failed earlier, refusing writes"));
                    }
                    group.clear();
                    continue;
                }

                try {
                    // b. Write every batch (no force yet). Progress goes into 'pending',
                    //    so a later batch in the SAME group dedups against an earlier one.
                    Map<String, Long> pending = new HashMap<>();
                    List<Boolean> results = new ArrayList<>(group.size());
                    for (PendingWrite pw : group) {
                        results.add(writeBatch(pw.batch(), pending));
                    }

                    // c. ONE fsync for the whole group
                    fileChannel.force(false);

                    // d. Durable now: record progress, THEN let each client send its ACK.
                    //    This order is the durability guarantee: no ACK before force().
                    lastStoredLine.putAll(pending);
                    for (int i = 0; i < group.size(); i++) {
                        group.get(i).result().complete(results.get(i));
                    }

                } catch (IOException e) {
                    // We can't know what reached the disk. Fail this group and refuse
                    // all future writes; a server restart rebuilds state from the file.
                    System.err.println("FATAL: write/force failed, store is now read-only: " + e);
                    failed = true;
                    for (PendingWrite pw : group) {
                        pw.result().completeExceptionally(e);
                    }
                }
                group.clear();
            }
        } catch (InterruptedException e) {
            // close() asked us to stop. Fail anything still waiting so no client hangs.
            for (PendingWrite pw : group) {
                pw.result().completeExceptionally(new IOException("store closed"));
            }
            PendingWrite leftover;
            while ((leftover = queue.poll()) != null) {
                leftover.result().completeExceptionally(new IOException("store closed"));
            }
        }
    }

    /**
     * Runs ONLY on the writer thread.
     * Dedups against 'pending' (earlier batches in this group) and lastStoredLine (on disk),
     * writes the new lines WITHOUT force(), and records progress in 'pending'.
     * Returns true if anything was written, false if every line was already stored.
     */
    private boolean writeBatch(LogBatch batch, Map<String, Long> pending) throws IOException {
        String agentId     = batch.agentId();
        List<String> lines = batch.lines();
        long last          = pending.getOrDefault(agentId, lastStoredLine.getOrDefault(agentId, 0L));
        long first         = batch.firstLine();
        long batchLast     = first + lines.size() - 1;

        // 1. Every line already stored (on disk or earlier in this group) -> nothing to write
        if (batchLast <= last) {
            return false;
        }

        // 2. Gap: lines between last and first never arrived. Can't recover them here -> warn loudly, keep going
        if (first > last + 1) {
            System.err.println("[" + agentId + "] GAP before batch #" + batch.seq()
                    + ": expected line " + (last + 1) + ", got " + first
                    + " (lines " + (last + 1) + "-" + (first - 1) + " missing)");
        }

        // 3. Drop the lines we already have, keep only the new ones
        int skip = (int) Math.max(0, last - first + 1);
        List<String> newLines = lines.subList(skip, lines.size());
        LogBatch toStore = new LogBatch(agentId, batch.seq(), first + skip, newLines);

        // 4. Write (NO force here: the writer does one force() for the whole group)
        String s = MAPPER.writeValueAsString(toStore) + "\n";
        ByteBuffer buffer = ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }

        // 5. Record progress in 'pending' only; copied to lastStoredLine after the group's force()
        pending.put(agentId, batchLast);

        if (skip > 0) {
            System.out.println("[" + agentId + "] batch #" + batch.seq() + ": skipped " + skip
                    + " already-stored line(s), stored " + newLines.size());
        }
        return true;
    }

    /** Stops the writer thread first, then closes the file. */
    @Override
    public void close() throws IOException {
        writerThread.interrupt();          // wakes take() with InterruptedException -> loop exits
        try {
            writerThread.join();           // wait until it has really finished
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        fileChannel.close();
    }
}