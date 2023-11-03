package de.tum.i13.KVStore;

import de.tum.i13.shared.MessagingProtocol;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

import static de.tum.i13.client.KVClient.LOGGER;

/**
 * ListenerThread is a Thread that infinitely listens for a notification connection.
 * When a notification arrives, this thread will simply print it out and reply with a confirmation.
 */
public class ListenerThread extends Thread {
    private int port;
    private Socket notificationSocket;
    private ServerSocket listenSocket;
    private BufferedReader reader;
    private BufferedWriter writer;


    /**
     * Random available port will be selected
     */
    public ListenerThread() {
        this.port = 0;
    }

    public ListenerThread(int port) {
        this.port = port;
    }

    /**
     * Checks the availability of the port passed
     *
     * @param port to be checked whether it can be user
     * @return true (available) | false (in use)
     */
    public static boolean portAvailable(int port) {
        try {
            ServerSocket socket = new ServerSocket(port);
            socket.close();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    public void run() {
        try {
            listenSocket = new ServerSocket(port); //this selects a random free port
            LOGGER.info("ListenerThread started at port " + port);
            this.port = listenSocket.getLocalPort();
            notificationSocket = listenSocket.accept(); //bsp 10200 (for all topics)
            LOGGER.info("Received connection from: " + notificationSocket.getRemoteSocketAddress());
            reader = new BufferedReader(new InputStreamReader(notificationSocket.getInputStream()));

            //endlessly listen for notifications from the connection with the PubSubServer
            while (true) {
                String notification;
                notification = reader.readLine();
                LOGGER.info("Received message: " + notification);
                if (notification.startsWith("notify ")) {
                    System.out.println(notification);
                    System.out.print("T15Client> ");
                }
                writer = new BufferedWriter(new OutputStreamWriter(notificationSocket.getOutputStream()));
                writer.write(MessagingProtocol.StatusType.ACK.toString());
                writer.flush();
                writer.close();
                //accept new connection after closing
                endThread();
                listenSocket = new ServerSocket(this.port);
                notificationSocket = listenSocket.accept();
                LOGGER.info("Received connection from: " + notificationSocket.getRemoteSocketAddress());
                reader = new BufferedReader(new InputStreamReader(notificationSocket.getInputStream()));
            }
        } catch (IOException e) {
            LOGGER.info("Thread closed");
            //e.printStackTrace();
        }
    }

    public int getPort() {
        return port;
    }

    /**
     * This method will end the thread by closing all IO-blocking attributes
     *
     * @throws IOException
     */
    public void endThread() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (writer != null) {
            writer.close();
        }
        if (notificationSocket != null) {
            notificationSocket.close();
        }
        if (listenSocket != null) {
            listenSocket.close();
        }
    }
}
