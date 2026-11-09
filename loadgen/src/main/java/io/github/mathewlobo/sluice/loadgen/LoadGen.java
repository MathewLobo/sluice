package io.github.mathewlobo.sluice.loadgen;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.mathewlobo.sluice.common.LogBatch;

/**
 * Load generator for the Sluice server.
 *
 * Simulates many agents at once, each sending LogBatch JSON over TCP as fast as
 * the server allows (stop-and-wait: send one batch, wait for its ACK, repeat).
 * It does NOT tail a file like the real agent, because that would be too slow to
 * stress the server.
 *
 * At the end it prints:
 *   - throughput: lines ACKed per second, across all agents
 *   - latency: time from FIRST sending a batch to receiving its ACK, including
 *     any retries after BUSY or a timeout (p50, p99, max)
 *   - timeouts: how often a batch got no reply within ACK_TIMEOUT_MS
 *   - BUSY: how often the server said its queue was full
 *
 * Usage: java -jar loadgen.jar [agents] [durationSec] [batchSize]
 *   e.g. java -jar loadgen.jar 50 30 100   -> 50 agents, 30 seconds, 100 lines per batch
 */
public class LoadGen {

    // ---- Settings ----------------------------------------------------------

    private static final String HOST = "localhost";
    private static final int PORT = 9000;

    // Same as the real agent: if no reply arrives in this time, give up on the
    // connection, reconnect and resend the same batch.
    private static final int ACK_TIMEOUT_MS = 5000;

    // After a BUSY reply, wait a RANDOM time in this range before resending.
    // Random ("jitter") so that agents told BUSY at the same moment don't all
    // come back at the same moment and overload the server again.
    private static final long BUSY_PAUSE_MIN_MS = 20;
    private static final long BUSY_PAUSE_MAX_MS = 100;

    // ObjectMapper is thread-safe once configured, so all agent threads can share one.
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * What one simulated agent reports back when it finishes.
     *
     * @param latenciesNanos one entry per ACKed batch: first send -> ACK time in nanoseconds
     * @param linesAcked     total lines the server confirmed for this agent
     * @param timeouts       how many times this agent waited ACK_TIMEOUT_MS with no reply
     * @param busyReplies    how many times the server answered BUSY (queue full)
     */
    record AgentResult(List<Long> latenciesNanos, long linesAcked, int timeouts, int busyReplies) { }

    // ---- Entry point -------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // Read settings from the command line, falling back to defaults.
        // This lets us compare runs (1 vs 10 vs 50 agents) without recompiling.
        int agents      = args.length > 0 ? Integer.parseInt(args[0]) : 10;
        int durationSec = args.length > 1 ? Integer.parseInt(args[1]) : 30;
        int batchSize   = args.length > 2 ? Integer.parseInt(args[2]) : 100;

        System.out.println("Agents: " + agents + "  Duration: " + durationSec + "s  Batch: " + batchSize);

        // One shared end time for every agent. Each agent checks it in its loop,
        // so no "stop" flag is needed. It never changes, so sharing it is thread-safe.
        long startNanos = System.nanoTime();
        long deadline = startNanos + TimeUnit.SECONDS.toNanos(durationSec);

        // A Future is a "receipt" for a result that isn't ready yet.
        // We keep one per agent and redeem them all at the end.
        List<Future<AgentResult>> futures = new ArrayList<>();

        // Executor that gives every task its own virtual thread (cheap, like the server).
        // ExecutorService is AutoCloseable since Java 21: try-with-resources shuts it down.
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {

            // Start all agents.
            for (int i = 0; i < agents; i++) {
                // A lambda can only use variables that never change. 'i' changes
                // every iteration, so copy it into a final variable first.
                final int id = i;

                // submit() starts the task right away and returns a Future.
                // The lambda returns a value and may throw checked exceptions, so Java
                // treats it as a Callable (a Runnable can do neither).
                futures.add(pool.submit(() -> runAgent(id, deadline, batchSize)));
            }

            // Collect and merge the results.
            List<Long> allLatencies = new ArrayList<>();
            long totalLines = 0;
            int totalTimeouts = 0;
            int totalBusy = 0;

            for (Future<AgentResult> f : futures) {
                // get() WAITS until this agent has finished, then returns its result.
                // Calling it on every Future = "wait for all agents".
                // If the agent threw an exception, get() rethrows it here
                // (wrapped in ExecutionException), so a broken run fails loudly.
                AgentResult r = f.get();
                allLatencies.addAll(r.latenciesNanos());
                totalLines += r.linesAcked();
                totalTimeouts += r.timeouts();
                totalBusy += r.busyReplies();
            }

            // Use the REAL elapsed time, not durationSec: agents finish the batch
            // they were sending after the deadline passes.
            double elapsedSec = (System.nanoTime() - startNanos) / 1_000_000_000.0;

            printReport(allLatencies, totalLines, totalTimeouts, elapsedSec, totalBusy);
        }
    }

    // ---- One simulated agent -----------------------------------------------

    /**
     * Runs one fake agent until the deadline.
     *
     * Everything here is a LOCAL variable, so each thread has its own copy:
     * no shared state, no locks, no atomics needed.
     */
    private static AgentResult runAgent(int id, long deadline, int batchSize)
            throws IOException, InterruptedException {

        // "load-" prefix makes these easy to tell apart from real agents in logs.dat.
        String agentId = "load-" + id;

        List<Long> latencies = new ArrayList<>();
        long seq = 0;          // batch number, used to match ACKs
        long firstLine = 1;    // line number of the first line in the next batch (for dedup)
        long linesAcked = 0;
        int timeouts = 0;
        int busy = 0;

        // The connection. null means "not connected", so the next attempt reconnects.
        Socket socket = null;
        BufferedWriter writer = null;
        BufferedReader reader = null;

        try {
            // OUTER LOOP: one iteration per NEW batch, until time is up.
            while (System.nanoTime() < deadline) {

                // Build the next batch ONCE. If it has to be retried, we resend
                // this exact JSON (same seq, same firstLine, same lines), just
                // like the real agent does.
                seq++;
                List<String> lines = new ArrayList<>(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    lines.add(agentId + " line " + (firstLine + i));   // e.g. "load-3 line 101"
                }
                String json = MAPPER.writeValueAsString(new LogBatch(agentId, seq, firstLine, lines));

                // Start the clock ONCE per batch, before the first attempt.
                // Latency = first send -> final ACK, INCLUDING any BUSY/timeout retries.
                // That's the delay the agent really experiences.
                long batchStart = System.nanoTime();

                // INNER LOOP: keep trying THIS batch until it is ACKed (or time is up).
                boolean acked = false;
                while (!acked && System.nanoTime() < deadline) {
                    try {
                        // Connect only if we don't have a connection yet
                        // (first time, or after a timeout threw the old one away).
                        if (socket == null) {
                            socket = new Socket(HOST, PORT);
                            socket.setSoTimeout(ACK_TIMEOUT_MS);   // readLine() gives up after this long
                            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
                            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                        }

                        writer.write(json);
                        writer.newLine();      // JSON Lines framing: one message per line
                        writer.flush();        // actually push the bytes onto the network
                        String reply = reader.readLine();   // waits for "ACK <seq>" or "BUSY <seq>"

                        // null = the server closed the connection before replying.
                        if (reply == null) {
                            throw new IOException(agentId + ": server closed the connection");
                        }

                        // Server overloaded (queue full): nothing was stored.
                        // Keep the connection, pause a random short time, resend the same batch.
                        if (reply.equals("BUSY " + seq)) {
                            busy++;
                            Thread.sleep(ThreadLocalRandom.current().nextLong(BUSY_PAUSE_MIN_MS, BUSY_PAUSE_MAX_MS + 1));
                            continue;
                        }

                        // Any reply other than our ACK means something is badly wrong.
                        if (!reply.equals("ACK " + seq)) {
                            throw new IllegalStateException(agentId + ": expected ACK " + seq + ", got " + reply);
                        }

                        // Success: record it and move on to the next batch.
                        latencies.add(System.nanoTime() - batchStart);
                        linesAcked += batchSize;
                        firstLine += batchSize;   // next batch starts right after this one
                        acked = true;             // ends the inner loop

                    } catch (SocketTimeoutException e) {
                        // No reply within ACK_TIMEOUT_MS: the server is overloaded.
                        // Count it, throw the connection away (a late ACK arriving on
                        // it could be mistaken for the next batch's ACK), and loop
                        // again to reconnect and resend the SAME batch.
                        // seq does not change, because seq++ only runs in the outer loop.
                        timeouts++;
                        closeQuietly(socket);
                        socket = null;
                    }
                    // Any OTHER IOException (e.g. "connection refused" because the
                    // server isn't running) is NOT caught: it ends this agent and
                    // main() reports it. For a benchmark, a broken setup should fail
                    // loudly instead of quietly producing wrong numbers.
                }
            }
        } finally {
            // Always close the connection, even if an exception is thrown.
            // Closing the socket also closes its reader and writer.
            closeQuietly(socket);
        }

        return new AgentResult(latencies, linesAcked, timeouts, busy);
    }

    // ---- Reporting ---------------------------------------------------------

    /** Prints throughput, batch/timeout/BUSY counts and latency percentiles. */
    private static void printReport(List<Long> latencies, long totalLines, int timeouts,
                                    double elapsedSec, int busy) {

        // %,d / %,.0f = numbers with thousands separators (e.g. 1,234,000).
        System.out.printf("Lines ACKed: %,d  Throughput: %,.0f lines/s%n", totalLines, totalLines / elapsedSec);
        System.out.println("Batches: " + latencies.size() + "  Timeouts: " + timeouts + "  BUSY: " + busy);

        if (latencies.isEmpty()) {
            System.out.println("No batches ACKed");
            return;
        }

        // Percentiles need the values in order.
        Collections.sort(latencies);

        // p50 = typical batch, p99 = the slowest 1% (the ones that hit timeouts first),
        // max = the single worst batch. Latencies are stored in nanoseconds and
        // converted to milliseconds only for printing, so no precision is lost.
        System.out.printf("Latency  p50: %.1f ms  p99: %.1f ms  max: %.1f ms%n",
                percentileMs(latencies, 50),
                percentileMs(latencies, 99),
                latencies.get(latencies.size() - 1) / 1_000_000.0);
    }

    /**
     * Returns the p-th percentile of a SORTED list, in milliseconds.
     *
     * Uses the "nearest rank" method: the value at position ceil(p% of size).
     * Example: 1,000 values, p99 -> rank 990 -> index 989.
     * "99% of batches were at least this fast."
     */
    private static double percentileMs(List<Long> sorted, double p) {
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        // Keep the index inside the list (matters for very small lists).
        index = Math.max(0, Math.min(index, sorted.size() - 1));
        return sorted.get(index) / 1_000_000.0;
    }

    // ---- Helpers -----------------------------------------------------------

    /** Closes a socket, ignoring errors: it's already broken or no longer needed. */
    private static void closeQuietly(Socket s) {
        if (s == null) return;
        try {
            s.close();
        } catch (IOException ignored) {
            // nothing useful to do
        }
    }
}