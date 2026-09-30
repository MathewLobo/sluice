package io.github.mathewlobo.sluice.agent;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class LineBatcher implements Consumer<String>{
    
    private final int batchSize;
    private final LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();

    private final Consumer<List<String>> batchConsumer;

    public LineBatcher(int batchSize, Consumer<List<String>> batchConsumer) {

        if (batchSize < 1) {
            throw new IllegalArgumentException("Batch size must be greater than 0. Provided: " + batchSize);
        }
        
        // If we get here, we know batchSize is valid (> 0)
        this.batchSize = batchSize; 
        this.batchConsumer = Objects.requireNonNull(batchConsumer, "batchConsumer cannot be null");
    }

    @Override 
    public void accept(String line){

        queue.offer(line);

        if (queue.size()>= batchSize){
            processBatch();
        }
    }

    public synchronized void processBatch(){

        List<String> batch = new ArrayList<>(this.batchSize);

        queue.drainTo(batch,this.batchSize);

        if (!batch.isEmpty()) {
            batchConsumer.accept(batch);
        }
    }




    

}
