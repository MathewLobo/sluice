package io.github.mathewlobo.sluice.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.github.mathewlobo.sluice.common.LogBatch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;

public class LogStore implements AutoCloseable{
    
    private static final Path filePath = Path.of("logs.dat");
    private final FileChannel fileChannel;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Map<String, Long> lastStoredLine = new HashMap<>();

    public LogStore() throws IOException {

        if (Files.exists(filePath)) {
            try (BufferedReader bf = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {

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

        fileChannel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.WRITE);
    }

    public synchronized boolean append(LogBatch batch) throws IOException {

        String agentId  = batch.agentId();
        List<String> lines = batch.lines();
        long last       = lastStoredLine.getOrDefault(agentId, 0L);
        long first      = batch.firstLine();
        long batchLast  = first + lines.size() - 1;

        // 1. Every line already stored -> nothing to write (caller still ACKs)
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

        String s = MAPPER.writeValueAsString(toStore) + "\n";
        ByteBuffer buffer = ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }
        fileChannel.force(false);

        lastStoredLine.put(agentId, batchLast);   // only after it's durable

        if (skip > 0) {
            System.out.println("[" + agentId + "] batch #" + batch.seq() + ": skipped " + skip
                    + " already-stored line(s), stored " + newLines.size());
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
 
}


                         


