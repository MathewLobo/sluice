package io.github.mathewlobo.sluice.crashtest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

public class CrashTest {

    private static final Path RUN_DIR = Path.of("crash-test-run");
    private static final Path SERVER_JAR = Path.of("server", "target", "server-0.1.0-SNAPSHOT.jar").toAbsolutePath();
    private static final Path AGENT_JAR = Path.of("agent", "target", "agent-0.1.0-SNAPSHOT.jar").toAbsolutePath();

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

        Process server = startProcess(SERVER_JAR, "server.out");
        Thread.sleep(1000);
        Process agent = startProcess(AGENT_JAR, "agent.out");
        Thread.sleep(5000);
    
        agent.destroyForcibly();  
        agent.waitFor();
        server.destroyForcibly(); 
        server.waitFor();
        System.out.println("CT1 done");
    
    }

}
