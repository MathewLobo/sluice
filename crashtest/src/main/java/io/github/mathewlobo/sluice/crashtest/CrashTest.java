package io.github.mathewlobo.sluice.crashtest;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.concurrent.atomic.AtomicBoolean;

public class CrashTest {

    private static final Path RUN_DIR = Path.of("crash-test-run");
    private static final Path SERVER_JAR = Path.of("server", "target", "server-0.1.0-SNAPSHOT.jar").toAbsolutePath();
    private static final Path AGENT_JAR = Path.of("agent", "target", "agent-0.1.0-SNAPSHOT.jar").toAbsolutePath();
    private static final int TOTAL_LINES = 5000;
    private static final long WRITE_DELAY_MS = 1;
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);
    private static final long DELIVERY_TIMEOUT_MS = 60_000;
    private static final ObjectMapper MAPPER = new ObjectMapper();
  
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


        Process server = null;
        Process agent = null;
        try{
            server = startProcess(SERVER_JAR, "server.out");
            Thread.sleep(1000);
            agent = startProcess(AGENT_JAR, "agent.out");

            Thread writer = startWriter();

            writer.join();          // wait until all lines are written
            if (isRunning.get()){
                System.out.println("Writer done: " + TOTAL_LINES + " lines");
            }
            else{
                throw new IllegalStateException("Writer failed");
            }
            long shipped = waitForDelivery();
            System.out.println("Delivered (per checkpoint): " + shipped + " / " + TOTAL_LINES);
        }finally{
            
            if (agent != null){
                agent.destroyForcibly();  
                agent.waitFor();
            }
            if (server != null){
                server.destroyForcibly(); 
                server.waitFor();
            }

        }
       
        
    }

}
