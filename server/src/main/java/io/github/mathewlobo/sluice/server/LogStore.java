package io.github.mathewlobo.sluice.server;

import java.nio.file.Path;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;

import io.github.mathewlobo.sluice.common.LogBatch;

import java.nio.ByteBuffer;

public class LogStore implements AutoCloseable{
    
    private static final Path filePath = Path.of("logs.dat");
    private final FileChannel fileChannel;

    public LogStore() throws IOException{

            fileChannel = FileChannel.open( filePath,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND,
            StandardOpenOption.WRITE);
            
    }

    public synchronized void append(LogBatch batch) throws IOException{

        StringBuilder sb = new StringBuilder();
        for ( String line: batch.lines()){
            sb.append(line).append("\n");
        }
        ByteBuffer buffer = ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.UTF_8));  
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }
        fileChannel.force(false);
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
 
    }


                         


