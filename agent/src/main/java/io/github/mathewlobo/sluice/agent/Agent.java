package io.github.mathewlobo.sluice.agent;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;

public class Agent {
    public static void main(String[] args) {
        System.out.println("agent started");

        // default to app.log in the working directory so it runs anywhere
        String filePathString = (args.length > 0) ? args[0] : "app.log";
        Path filePath = Path.of(filePathString);


        try {
            BatchSender sender = new BatchSender("localhost", 9000, "agent-1");
            sender.connect();
            LineBatcher batcher = new LineBatcher(5, 500, sender);
            LogTailer tailer = new LogTailer(filePath, batcher);
            batcher.start();
            tailer.run();
            batcher.close();
            sender.close();

        } catch (IllegalArgumentException e){
            System.err.println("Illegal Argument " + e.getMessage());
            System.exit(1);
        }catch(IOException e){
            System.err.println("Error reading File: " + e.getMessage());
            System.exit(1);
        }
       
    }
}