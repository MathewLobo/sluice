package io.github.mathewlobo.sluice.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

import io.github.mathewlobo.sluice.common.LogBatch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;

public class LogStore implements AutoCloseable{
    
    private static final Path filePath = Path.of("logs.dat");
    private final FileChannel fileChannel;
    private final Map<String, Long> lastStoredSeq = new HashMap<>();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public LogStore() throws IOException{
    
        if (Files.exists(filePath)){

            try (BufferedReader bf = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)){
                
                String line;
                while ((line = bf.readLine()) != null){

                    try{
                        LogBatch checklog = MAPPER.readValue(line, LogBatch.class);
                        long current = lastStoredSeq.getOrDefault(checklog.agentId(), 0L);
                        lastStoredSeq.put(checklog.agentId(), Math.max(current, checklog.seq()));

                    }catch(JsonProcessingException e){
                        System.err.println("Skipping unreadbale record " + e);
                    }
                }
            }
        }
        System.out.println("Recovered State for " + lastStoredSeq.size() + " agent(s) " + lastStoredSeq);
    
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


                         


