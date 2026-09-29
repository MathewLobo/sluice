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
            LogTailer tailer = new LogTailer(filePath);
            tailer.run();

        } catch (IllegalArgumentException e){
            System.err.println("Illegal Argument " + e.getMessage());
            System.exit(1);
        }catch(IOException e){
            System.err.println("Error reading File: " + e.getMessage());
            System.exit(1);
        }
       
    }
}