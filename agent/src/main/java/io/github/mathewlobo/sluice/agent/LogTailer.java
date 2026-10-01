package io.github.mathewlobo.sluice.agent;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Consumer;

public class LogTailer {

    private static final int POLL_INTERVAL_MS = 500;

    private final Path filePath;
    private final Consumer<String> lineConsumer;

    public LogTailer(Path filePath, Consumer<String> lineConsumer) {  // checks if file exists plus if its valid
        
        this.filePath = Objects.requireNonNull(filePath, "filePath cannot be null");
        this.lineConsumer = Objects.requireNonNull(lineConsumer, "lineConsumer cannot be null");

        if (!Files.exists(this.filePath)) {
            throw new IllegalArgumentException("Target log file does not exist: " + this.filePath.toAbsolutePath());
        }

        if (Files.isDirectory(this.filePath)) {
            throw new IllegalArgumentException("Path points to a directory, not a file: " + this.filePath.toAbsolutePath());
        }
    }

    public void run() throws IOException {

        try (BufferedReader br = Files.newBufferedReader(this.filePath)) {

            StringBuilder lineBuilder = new StringBuilder();
            int charCode;

            while (true) {

                charCode = br.read();
                if (charCode != -1) {
                    char c = (char) charCode;

                    if (c == '\n') {
                        // Complete line found -> hand it to consumer
                        this.lineConsumer.accept(lineBuilder.toString());
                        lineBuilder.setLength(0);
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
