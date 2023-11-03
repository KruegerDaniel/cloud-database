package de.tum.i13;

import de.tum.i13.client.KVClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;


public class ClientTest {
    public static Integer portKV = 5150;
    public static Integer portECS = 5451;

    @Test
    public void testClientPut() throws IOException {
        AsyncTester serverThread = new AsyncTester(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(portKV);
                Socket client = serverSocket.accept();
                System.out.println("Connection accepted");
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter printWriter = new PrintWriter(client.getOutputStream());
                printWriter.println("connection accepted");
                printWriter.flush();
                String request = bufferedReader.readLine();
                printWriter.println("put_success key");
                printWriter.flush();
                if (!request.equals("put key hello")) {
                    throw new AssertionError("Server did not receive put key hello");
                }
                serverSocket.close();
                client.close();
            } catch (IOException e) {
            }
        });
        serverThread.start();

        String input = "connect 127.0.0.1 " + portKV + "\n" +
                "put key hello\nquit\n";
        InputStream in = new ByteArrayInputStream(input.getBytes());
        System.setIn(in);
        KVClient.main(null);
    }

    @Test
    public void testClientGet() throws IOException {
        AsyncTester serverThread = new AsyncTester(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(portKV);
                Socket client = serverSocket.accept();
                System.out.println("Connection accepted");
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter printWriter = new PrintWriter(client.getOutputStream());
                printWriter.println("connection accepted");
                printWriter.flush();
                String request = bufferedReader.readLine();
                printWriter.println("get_success key value");
                printWriter.flush();
                if (!request.equals("get key")) {
                    throw new AssertionError("Server did not receive get key");
                }
                serverSocket.close();
                client.close();
            } catch (IOException e) {
            }
        });
        serverThread.start();


        String input = "connect 127.0.0.1 " + portKV + "\n" +
                "get key\nquit\n";
        InputStream in = new ByteArrayInputStream(input.getBytes());
        System.setIn(in);
        KVClient.main(null);
        try {
            serverThread.test();
        } catch (InterruptedException e) {

        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void testClientDelete() throws IOException {
        AsyncTester serverThread = new AsyncTester(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(portKV);
                Socket client = serverSocket.accept();
                System.out.println("Connection accepted");
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter printWriter = new PrintWriter(client.getOutputStream());
                printWriter.println("connection accepted");
                printWriter.flush();
                String request = bufferedReader.readLine();
                printWriter.println("delete_success key");
                printWriter.flush();
                if (!request.equals("delete key")) {
                    throw new AssertionError("Server did not receive delete key");
                }
                serverSocket.close();
                client.close();
            } catch (IOException e) {
            }
        });
        serverThread.start();

        String input = "connect 127.0.0.1 " + portKV + "\n" +
                "delete key\nquit\n";
        InputStream in = new ByteArrayInputStream(input.getBytes());
        System.setIn(in);
        KVClient.main(null);
        try {
            serverThread.test();
        } catch (InterruptedException e) {

        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void testClientConnect() throws IOException {
        AsyncTester serverThread = new AsyncTester(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(portKV);
                Socket client = serverSocket.accept();
                System.out.println("Connection accepted: " + client.getRemoteSocketAddress());
                serverSocket.close();
                client.close();
            } catch (IOException e) {
                throw new AssertionError();
            }
        });

        serverThread.start();

        String input = "connect 127.0.0.1 " + portKV + "\nquit\n";
        InputStream in = new ByteArrayInputStream(input.getBytes());
        System.setIn(in);
        KVClient.main(null);
        try {
            serverThread.test();
        } catch (InterruptedException e) {

        }
    }


}


