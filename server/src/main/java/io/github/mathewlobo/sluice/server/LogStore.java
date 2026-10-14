package io.github.mathewlobo.sluice.server;

import java.nio.file.Path;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

import io.github.mathewlobo.sluice.common.LogBatch;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;

public class LogStore implements AutoCloseable{
    
    private static final Path filePath = Path.of("logs.dat");
    private final FileChannel fileChannel;
    private final Map<String, Long> lastStoredSeq = new HashMap<>();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public LogStore() throws IOException{

            fileChannel = FileChannel.open( filePath,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND,
            StandardOpenOption.WRITE);
            
    }

    public synchronized boolean append(LogBatch batch) throws IOException{

        long last = lastStoredSeq.getOrDefault(batch.agentId(), 0L);
        if (batch.seq() <= last) {
            return false;               // already stored, so skip writing
        }

        String s = MAPPER.writeValueAsString(batch) + "\n";
        ByteBuffer buffer = ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));  
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }
        fileChannel.force(false);
        
        lastStoredSeq.put(batch.agentId(), batch.seq());

        return true; 
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
 
}


                         


