package io.github.mathewlobo.sluice.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Server{
    private static final int PORT = 9000;

    private static void handleClient(Socket clientSocket){

            String client = clientSocket.getRemoteSocketAddress().toString();

            try(
                clientSocket;
                BufferedReader br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8))
                ){
                System.out.println(client + ": Client Connected");
            
                String message = br.readLine();
                while (message != null){
                    System.out.println(client + ": Message from Client: " + message);
                    message = br.readLine();
                }
                System.out.println(client + ": Client Disconnected");
            }catch(IOException e){
                System.err.println(client + ": Client Error: " + e.getMessage());
            }
                
    }
  
    public static void main(String args[]){

        System.out.println("Server starting... waiting for client connection on port " + PORT);

        try (ServerSocket serverSocket = new ServerSocket(PORT)){

            while (true){
                try{
                    Socket clientSocket = serverSocket.accept();
                    Thread.ofVirtual().start(() -> handleClient(clientSocket));
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