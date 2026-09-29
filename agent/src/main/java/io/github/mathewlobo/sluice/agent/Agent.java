package io.github.mathewlobo.sluice.agent;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Agent {
    public static void main(String[] args) {
        System.out.println("agent started");
        
        // default to app.log in the working directory so it runs anywhere
        String filePathString = (args.length> 0) ? args[0] : "app.log";

        // 2. Modern File API: Convert string string into a java.nio Path
        Path filepath = Path.of(filePathString);


        try(BufferedReader br = Files.newBufferedReader(filepath)){
            
            String firstLine = br.readLine();
            
            if (firstLine != null){
                System.out.println("First Line: " + firstLine );
            }
            else{
                System.out.println("The file is empty");
            }
        }catch (IOException e){
            System.err.println("Error Reading the File: " + e.getMessage());
            System.exit(1);
        }
    }
}