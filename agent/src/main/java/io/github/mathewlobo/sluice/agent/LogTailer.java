package io.github.mathewlobo.sluice.agent;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class LogTailer {

    private static final int POLL_INTERVAL_MS = 500;

    private final Path filePath;

    public LogTailer(Path filePath) {  // checks if file exists plus if its valid
        
        this.filePath = Objects.requireNonNull(filePath, "File path cannot be Null");

        if (!Files.exists(this.filePath)) {
            throw new IllegalArgumentException(": Target log file does not exist: " + this.filePath.toAbsolutePath());
        }

        if (Files.isDirectory(this.filePath)) {
            throw new IllegalArgumentException(": Path points to a directory, not a file: " + this.filePath.toAbsolutePath());
        }
    }

    public void run() throws IOException {

        try (BufferedReader br = Files.newBufferedReader(this.filePath)) {

            StringBuilder lineBuilder = new StringBuilder();
            int charCode;
            int count = 1;

            while (true) {

                charCode = br.read();
                if (charCode != -1) {
                    char c = (char) charCode;

                    if (c == '\n') {
                        // Since we reached a new line we print the line
                        System.out.println("Line " + count + ": " + lineBuilder.toString());
                        lineBuilder.setLength(0);
                        count += 1;
                    } else if (c != '\r') {
                        lineBuilder.append(c);
                    }

                } else {
                    Thread.sleep(POLL_INTERVAL_MS);
                }
            }
        } catch (InterruptedException e) {
            System.err.println("Tailer interrupted: stopping");
            Thread.currentThread().interrupt();
        }

    }

}
