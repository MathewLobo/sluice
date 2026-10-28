package io.github.mathewlobo.sluice.crashtest;
import io.github.mathewlobo.sluice.common.LogBatch;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.stream.Stream;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


public class CrashTest {

    private static final Path RUN_DIR = Path.of("crash-test-run");
    private static final Path SERVER_JAR = Path.of("server", "target", "server-0.1.0-SNAPSHOT.jar").toAbsolutePath();
    private static final Path AGENT_JAR = Path.of("agent", "target", "agent-0.1.0-SNAPSHOT.jar").toAbsolutePath();

    private static final int TOTAL_LINES = 5000;
    private static final long WRITE_DELAY_MS = 10;
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);
    private static final long DELIVERY_TIMEOUT_MS = 120_000;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    private static final AtomicReference<Process> serverRef = new AtomicReference<>();
    private static final AtomicReference<Process> agentRef  = new AtomicReference<>();
    private static final AtomicBoolean chaosOn = new AtomicBoolean(true);
    private static final AtomicBoolean chaosOk = new AtomicBoolean(true);
    private static final AtomicInteger kills = new AtomicInteger();
    private static final long MIN_GAP_MS = 2000;
    private static final long MAX_GAP_MS = 6000;
    private static final long RESTART_DELAY_MS = 1000;


    private static void deleteDirectoryContent(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;                          // nothing to clean
        }

        try (Stream<Path> paths = Files.walk(dir)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                if (!path.equals(dir)) {     // keep the folder itself, delete what's inside
                    Files.delete(path);
                }
            }
        }
        System.out.println("Cleaned " + dir);
    }

    private static Process startProcess(Path jar, String outputFileName) throws IOException{
        
        ProcessBuilder pb = new ProcessBuilder("java", "-jar", jar.toString());
        pb.directory(RUN_DIR.toFile());
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(RUN_DIR.resolve(outputFileName).toFile()));

        return pb.start();

    }

    private static Thread startWriter(){

        Thread writerThread = new Thread(() -> {
            Path logFile = RUN_DIR.resolve("app.log");

        
            try (BufferedWriter writer = Files.newBufferedWriter(
                    logFile, 
                    StandardOpenOption.CREATE, 
                    StandardOpenOption.APPEND)) {

              
                for (int i = 1; i <= TOTAL_LINES; i++) {
                    
                    String line = String.format("line %06d%n", i);
                    
                    writer.write(line);
                    writer.flush(); 

                    
                    Thread.sleep(WRITE_DELAY_MS);
                }

            } catch (IOException e) {
                System.err.println("File write error: " + e.getMessage());
                isRunning.set(false);
            } catch (InterruptedException e) {
                System.out.println("Writer thread was interrupted.");
                // Good practice: restore the interrupt flag
                Thread.currentThread().interrupt();
                isRunning.set(false);
            }
        });

        writerThread.start();
        return writerThread;

    }

    private static long waitForDelivery() throws InterruptedException{

        Path checkpointFile = RUN_DIR.resolve("agent-checkpoint.json");
        long deadline = System.currentTimeMillis() + DELIVERY_TIMEOUT_MS;

        while (true){
            long shipped = readShipped(checkpointFile);
            if (shipped >= TOTAL_LINES){
                return shipped;
            }
            if (System.currentTimeMillis() > deadline){
                return shipped;
            }
            Thread.sleep(1000);
        }
    }

    private static long readShipped(Path checkpointFile){

         if (!Files.exists(checkpointFile)){
            return 0;
        }

        try{
            JsonNode node = MAPPER.readTree(checkpointFile.toFile());
            if (node.get("linesShipped") == null){
                return -1;
            }
            return node.get("linesShipped").asLong();
        }catch(IOException e){
            return -1;
        }

    }

    private static boolean verify() throws IOException {
        Path logsFile = RUN_DIR.resolve("logs.dat");
        if (!Files.exists(logsFile)) {
            System.out.println("FAIL: logs.dat not found");
            return false;
        }

        int[] counts = new int[TOTAL_LINES + 1];   // index = line number, 0 unused
        long stored = 0;
        int corrupt = 0;      // records that aren't valid JSON
        int badLines = 0;     // valid JSON, but the text isn't "line NNNNNN" in range

        for (String record : Files.readAllLines(logsFile)) {
            if (record.isBlank()) continue;
            LogBatch batch;
            try {
                batch = MAPPER.readValue(record, LogBatch.class);
            } catch (JsonProcessingException e) {
                corrupt++;
                continue;
            }
            for (String logLine : batch.lines()) {
                stored++;
                try {
                    int n = Integer.parseInt(logLine.trim().substring(5)); // "line 000123" -> 123
                    if (n < 1 || n > TOTAL_LINES) { badLines++; continue; }
                    counts[n]++;
                } catch (NumberFormatException | IndexOutOfBoundsException e) {
                    badLines++;
                }
            }
        }

        List<Integer> missing = new ArrayList<>();
        List<Integer> duplicates = new ArrayList<>();
        for (int n = 1; n <= TOTAL_LINES; n++) {
            if (counts[n] == 0) missing.add(n);
            else if (counts[n] > 1) duplicates.add(n);
        }

        boolean pass = missing.isEmpty() && duplicates.isEmpty() && corrupt == 0 && badLines == 0;

        System.out.println("Written: " + TOTAL_LINES
                + "  Stored: " + stored
                + "  Missing: " + missing.size()
                + "  Duplicates: " + duplicates.size()
                + "  Corrupt: " + corrupt
                + "  Bad lines: " + badLines);
        if (!missing.isEmpty())
            System.out.println("  First missing: " + missing.subList(0, Math.min(10, missing.size())));
        if (!duplicates.isEmpty())
            System.out.println("  First duplicates: " + duplicates.subList(0, Math.min(10, duplicates.size())));
        System.out.println(pass ? "PASS" : "FAIL");

        return pass;
    }

    private static Thread startChaos() {
        Thread t = new Thread(() -> {
            try {
                while (chaosOn.get()) {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(MIN_GAP_MS, MAX_GAP_MS + 1));
                    if (!chaosOn.get()) break;   // main said stop while we slept

                    boolean hitServer = ThreadLocalRandom.current().nextBoolean();
                    AtomicReference<Process> ref = hitServer ? serverRef : agentRef;
                    Path jar = hitServer ? SERVER_JAR : AGENT_JAR;
                    String outFile = hitServer ? "server.out" : "agent.out";
                    String name = hitServer ? "server" : "agent";

                    kill(ref.get());
                    int n = kills.incrementAndGet();
                    System.out.println("Kill #" + n + ": " + name);

                    Thread.sleep(RESTART_DELAY_MS);
                    ref.set(startProcess(jar, outFile));
                }
            } catch (IOException e) {
                System.out.println("CHAOS FAILED: could not restart process: " + e);
                chaosOk.set(false);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "chaos");
        t.start();
        return t;
    }

    private static void kill(Process p) throws InterruptedException {
        if (p != null) {
            p.destroyForcibly();
            p.waitFor();
        }
    }


    public static void main (String args[]) throws Exception{

        System.out.println("Test started");
        if (!Files.exists(SERVER_JAR)){
            throw new IllegalStateException("Build first: " + SERVER_JAR);
        }
        if (!Files.exists(AGENT_JAR)){
            throw new IllegalStateException("Build first: " + AGENT_JAR);
        }

        deleteDirectoryContent(RUN_DIR);
        Files.createDirectories(RUN_DIR);   

        Files.writeString(RUN_DIR.resolve("app.log"), "");


        Thread chaos = null;
        try {
            serverRef.set(startProcess(SERVER_JAR, "server.out"));
            Thread.sleep(1000);
            agentRef.set(startProcess(AGENT_JAR, "agent.out"));

            Thread writer = startWriter();
            chaos = startChaos();

            writer.join();
            if (!isRunning.get()) throw new IllegalStateException("Writer failed");
            System.out.println("Writer done: " + TOTAL_LINES + " lines");

            chaosOn.set(false);
            chaos.join();                 // wait for any in-progress restart to finish
            if (!chaosOk.get()) throw new IllegalStateException("Chaos thread failed");
            System.out.println("Chaos stopped after " + kills.get() + " kills");

            long shipped = waitForDelivery();
            System.out.println("Delivered (per checkpoint): " + shipped + " / " + TOTAL_LINES);
        } finally {
            chaosOn.set(false);
            if (chaos != null) chaos.join();   // no restarts can happen after this
            kill(agentRef.get());
            kill(serverRef.get());
        }

        boolean pass = verify();
        System.out.println("Kills: " + kills.get());
        System.exit(pass ? 0 : 1);        
    }

}
