package io.github.mathewlobo.sluice.agent;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class LineBatcher implements Consumer<String>{
    
    private final int batchSize;
    private List<String> elementArr = new ArrayList<>();

    private final Consumer<List<String>> batchConsumer;
    private final long flushIntervalMs;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public LineBatcher(int batchSize, long flushIntervalMs, Consumer<List<String>> batchConsumer) {

        if (batchSize < 1) {
            throw new IllegalArgumentException("Batch size must be greater than 0. Provided: " + batchSize);
        }

        if (flushIntervalMs < 1){
            throw new IllegalArgumentException("flushIntervalMs must be atleast 1, got " + flushIntervalMs);
        }
        
        this.batchSize = batchSize; 
        this.batchConsumer = Objects.requireNonNull(batchConsumer, "batchConsumer cannot be null");
        this.flushIntervalMs = flushIntervalMs;
    }

    @Override 
    public synchronized void accept(String line){

        elementArr.add(line);

        if (elementArr.size()>= batchSize){
            processBatch();
        }
    }

    private synchronized void processBatch() {
        if (elementArr.isEmpty()) {
            return;                          
        }

        List<String> batch = elementArr;     
        elementArr = new ArrayList<>();      
        batchConsumer.accept(batch);         
    }

    public void start(){
        scheduler.scheduleAtFixedRate(this::processBatch, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    public void close(){
        processBatch();
        scheduler.shutdown();
    }




    

}
