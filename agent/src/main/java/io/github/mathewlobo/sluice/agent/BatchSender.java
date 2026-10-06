package io.github.mathewlobo.sluice.agent;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.mathewlobo.sluice.common.LogBatch;

public class BatchSender implements Consumer<List<String>>{

    private final String host;
    private final int port;
    private final String agentId;
    private final ObjectMapper converter = new ObjectMapper();
    private final AtomicLong sequence = new AtomicLong();   // starts at 0
    private Socket socket;
    private BufferedWriter writer;

    public BatchSender(String host, int port, String agentId){

        this.host = Objects.requireNonNull(host, "host cannot be null");
        this.agentId = Objects.requireNonNull(agentId, "agentId cannot be null");

        this.port = port;
        if (port < 1 || port > 65535){
            throw new IllegalArgumentException("port must be between 1 and 65535, got " + port);
        }
    }

    @Override 
    public void accept(List<String> batch){
        LogBatch lb = new LogBatch(agentId, sequence.incrementAndGet(), batch);
        try{
            String json = converter.writeValueAsString(lb);
            writer.write(json);
            writer.newLine();
            writer.flush();
        }catch(IOException e){
            throw new UncheckedIOException("Failed to send Batch" + lb.seq(), e);
        }
    }

    public void connect() throws IOException {
        socket = new Socket(host, port);
        writer = new BufferedWriter(
                new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
    }

    public void close() throws IOException {
        if (writer != null) {
            writer.close();    // flushes anything still buffered, then closes
        }
        if (socket != null) {
            socket.close();
        }
    }
        
}
