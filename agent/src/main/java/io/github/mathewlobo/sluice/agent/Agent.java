package io.github.mathewlobo.sluice.agent;

import java.io.IOException;
import java.nio.file.Path;

public class Agent {
    public static void main(String[] args) {
        System.out.println("agent started");

        // default to app.log in the working directory so it runs anywhere
        String filePathString = (args.length > 0) ? args[0] : "app.log";
        Path filePath = Path.of(filePathString);


        try {
            LineBatcher batcher = new LineBatcher(5, 500, batch -> {
                System.out.println("---- Batch of " + batch.size() + " lines ----");
                for (String line : batch) {
                    System.out.println(line);
                }
            }); 
            LogTailer tailer = new LogTailer(filePath, batcher);
            batcher.start();
            tailer.run();
            batcher.close();

        } catch (IllegalArgumentException e){
            System.err.println("Illegal Argument " + e.getMessage());
            System.exit(1);
        }catch(IOException e){
            System.err.println("Error reading File: " + e.getMessage());
            System.exit(1);
        }
       
    }
}