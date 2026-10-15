package io.github.mathewlobo.sluice.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.mathewlobo.sluice.common.LogBatch;

public class Server{
    private static final int PORT = 9000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static void handleClient(Socket clientSocket, LogStore store){

            String client = clientSocket.getRemoteSocketAddress().toString();

            try(
                clientSocket;
                BufferedReader br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8));
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8))
                ){

                System.out.println(client + ": Client Connected");
            
                String message = br.readLine();

                while (message != null){
                    
                    try{
                        LogBatch batch = MAPPER.readValue(message, LogBatch.class);
                        

                        boolean stored = store.append(batch);
                        if (stored)
                            System.out.println(client + ": [" + batch.agentId() + "] batch #" + batch.seq() + ": " + batch.lines().size() + " lines stored");
                        else
                            System.out.println(client + ": [" + batch.agentId() + "] batch #" + batch.seq() + ": " + batch.lines().size() + " lines already present");
                        writer.write("ACK "+batch.seq());
                        writer.newLine();
                        writer.flush();
                    }catch(JsonProcessingException e){
                        System.err.println("Invalid JSon format " +e.getMessage());
                    }
                    message = br.readLine();
                }
                System.out.println(client + ": Client Disconnected");
            }catch(IOException e){
                System.err.println(client + ": Client Error: " + e.getMessage());
            }
                
    }
  
    public static void main(String args[]){

        System.out.println("Server starting... waiting for client connection on port " + PORT);

        try (ServerSocket serverSocket = new ServerSocket(PORT);
             LogStore store = new LogStore()){

            while (true){
                try{
                    Socket clientSocket = serverSocket.accept();
                    Thread.ofVirtual().start(() -> handleClient(clientSocket, store));
                }catch(IOException e){
                    if (serverSocket.isClosed()) {
                        break;      // server socket is gone, so stop accepting
                    }

                    System.err.println("Connection Error: " + e.getMessage());
                }
            }

        }catch(IOException e){
            System.err.println("Server Error: " + e.getMessage());
        }
        System.out.println("Server Shutdown");        
    }

}