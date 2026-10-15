package io.github.mathewlobo.sluice.agent;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import io.github.mathewlobo.sluice.common.LogBatch;

public class BatchSender implements Consumer<List<String>>{

    private final String host;
    private final int port;
    private final String agentId;
    private final ObjectMapper converter = new ObjectMapper();
    private final AtomicLong sequence;   
    private Socket socket;
    private BufferedWriter writer;
    private BufferedReader reader;
    private final CheckpointStore checkpointStore;
    private long linesShipped;

    private static final int ACK_TIMEOUT_MS = 5000;
    private static final long INITIAL_BACKOFF_MS = 1_000;    // 1 second
    private static final long MAX_BACKOFF_MS = 20_000;

    public BatchSender(String host, int port, String agentId, CheckpointStore checkpointStore, Checkpoint start){

        this.host = Objects.requireNonNull(host, "host cannot be null");
        this.agentId = Objects.requireNonNull(agentId, "agentId cannot be null");

        this.port = port;
        if (port < 1 || port > 65535){
            throw new IllegalArgumentException("port must be between 1 and 65535, got " + port);
        }
        this.checkpointStore = Objects.requireNonNull(checkpointStore, "checkpointStore cannot be null");
        Objects.requireNonNull(start, "start checkpoint cannot be null");

        this.sequence = new AtomicLong(start.lastSeq());
        this.linesShipped = start.linesShipped();
    }

    @Override
    public synchronized void accept(List<String> batch) {
        LogBatch lb = new LogBatch(agentId, sequence.incrementAndGet(), batch);  // seq taken once
        long backoffMs = INITIAL_BACKOFF_MS;

        String json;
        try {
            json = converter.writeValueAsString(lb);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException("Couldn't convert batch " + lb.seq() + " to JSON", e);
        }


        while (true){
            try {
  

                if (socket == null || socket.isClosed()) {
                    connect();
                }
                // send
                writer.write(json);
                writer.newLine();
                writer.flush();

                // wait for the ACK (up to ACK_TIMEOUT_MS, set in connect())
                String reply = reader.readLine();
                if (reply == null) {
                    throw new IOException("server closed connection before ACK for batch " + lb.seq());
                }
                if (!reply.equals("ACK " + lb.seq())) {
                    throw new IOException("unexpected reply for batch " + lb.seq() + ": " + reply);
                }

                System.out.println("ACKed batch " + lb.seq());
                long newTotal = linesShipped + batch.size();
                checkpointStore.save(new Checkpoint(newTotal, lb.seq()));
                linesShipped = newTotal;
                
                return;

            } catch (IOException e) {
                System.err.println("Failed to send batch " + lb.seq() + ": " + e.getMessage()+ " (retrying in " + backoffMs + " ms)");                
                closeQuietly();
                
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while retrying batch " + lb.seq(), ie);
                }

                backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
            }
        }
    }

    public void connect() throws IOException {
        socket = new Socket(host, port);
        socket.setSoTimeout(ACK_TIMEOUT_MS);
        writer = new BufferedWriter(
                new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        reader = new BufferedReader(
            new InputStreamReader(socket.getInputStream(),StandardCharsets.UTF_8));
    }

    public void close() throws IOException {
        if (writer != null) {
            writer.close();    // flushes anything still buffered, then closes
        }
        if (reader != null){
            reader.close();
        }
        if (socket != null) {
            socket.close();
        }
    }

    private void closeQuietly() {
        try {
            if (socket != null) {
                socket.close();     // also closes its reader and writer
            }
        } catch (IOException ignored) {
            // already broken, nothing more to do
        }
        socket = null;
        reader = null;
        writer = null;
    }
        
}
