package io.github.mathewlobo.sluice.agent;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Agent {
    public static void main(String[] args) {
        System.out.println("agent started");

        // default to app.log in the working directory so it runs anywhere
        String filePathString = (args.length > 0) ? args[0] : "app.log";

        // 2. Modern File API: Convert string string into a java.nio Path
        Path filePath = Path.of(filePathString);

        if (!Files.exists(filePath)) {
            System.err.println("Error: Target log file does not exist: " + filePath.toAbsolutePath());
            System.exit(1);
        }

        try (BufferedReader br = Files.newBufferedReader(filePath)) {

            String line;
            int count = 1;
            while (true) {

                line = br.readLine();
                if (line != null) {
                    System.out.println("Line " +count+ ": " + line);
                    count += 1;
                } else {
                    Thread.sleep(500);
                }
            }

        } catch (IOException e) {
            System.err.println("Error Reading the File: " + e.getMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            System.err.println("Tailer interupted:" + e.getMessage());
            Thread.currentThread().interrupt();
            System.exit(1);
        }
    }
}