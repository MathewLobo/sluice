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

            StringBuilder lineBuilder = new StringBuilder();
            int charCode;
            int count = 1;

            while (true) {

                charCode = br.read();
                if (charCode != -1){
                    char c = (char) charCode;

                    if (c == '\n') {
                        // Since we reached a new line we print the line
                        System.out.println("Line " + count + ": " + lineBuilder.toString());
                        lineBuilder.setLength(0);
                        count +=1;
                    }
                    else if(c != '\r'){
                        lineBuilder.append(c);
                    }

                } else {
                    Thread.sleep(500);
                }
            }

        } catch (IOException e) {
            System.err.println("Error Reading the File: " + e.getMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            System.err.println("Tailer interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
            System.exit(1);
        }
    }
}