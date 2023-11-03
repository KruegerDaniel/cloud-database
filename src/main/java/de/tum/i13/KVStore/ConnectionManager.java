package de.tum.i13.KVStore;

import de.tum.i13.client.KVClient;
import de.tum.i13.shared.BackOff;
import de.tum.i13.shared.MessagingProtocol;
import de.tum.i13.shared.Pair;
import de.tum.i13.shared.Range;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Abstract class defining a disting connection to a socket, the
 */
public abstract class ConnectionManager {
    protected final BackOff backOff = new BackOff(100, 1024);
    protected ActiveConnection clientSocket;
    protected List<Range> writeRanges;
    protected List<Range> readRanges;
    protected ListenerThread listenerThread;
    protected int listenerPort;
    ConnectionManager(ActiveConnection clientSocket) {
        this.clientSocket = clientSocket;
        writeRanges = new ArrayList<>();
        readRanges = new ArrayList<>();
    }

    public ListenerThread getListenerThread() {
        return listenerThread;
    }

    /**
     * The method tries to build a connection with the KVServer. If the passed parameters are correct, the client is
     * connected to the server, and we can start the interaction. If the passed parameters are wrong, there is no
     * connection established.
     */
    public String buildConnection(Pair<String, Integer> addressPair) {
        try {
            ConnectionBuilder kvcb = new ConnectionBuilder(addressPair.getLeft(), addressPair.getRight());
            clientSocket = kvcb.connect();
            startListening();
            return clientSocket.readline();
        } catch (IOException e) {
            return "Could not connect to server";
        }
    }

    /**
     * The method tries to disconnect from the KVServer.
     */
    public void closeCurrentConnection() {
        try {
            clientSocket.close();
            clientSocket = null;
            stopListening();
        } catch (Exception e) {
            clientSocket = null;
        }
    }

    /**
     * The method gives further to the server the command given by the client. In case there is no connection established,
     * we let the client know. If the message from the client is empty, we also let him know, and we do not pass this
     * message to the server. Finally, if the message is complete, we pass it to the server. We then try to read and
     * print the answer of the server.
     *
     * @param command tells the server what it is supposed to do
     * @param data    the data the server must handle in relation to the passed command.
     */
    protected String sendMessage(MessagingProtocol.StatusType command, String[] data) throws IOException {
        if (clientSocket == null) {
            throw new IOException();
        }

        String line = command.toString().toLowerCase() + " " + KVClient.convertStrArrayToStr(data, " ");

        clientSocket.write(line);

        return clientSocket.readline();
    }

    protected String sendMessage(MessagingProtocol.StatusType command) throws IOException {
        return sendMessage(command, new String[0]);
    }

    protected boolean stillReachable() {
        try {
            sendMessage(MessagingProtocol.StatusType.PING);
        } catch (IOException notReachable) {
            return false;
        }
        return true;
    }

    /**
     * In case of server shutdown, reconnect to a random server in the metadata.
     * This method will not connect to the current connection, since we assume it is shutdown
     */
    protected void randomConnect() {
        //in case there is only one KVServer running
        if (writeRanges.size() == 1) {
            return;
        }

        //remove the current connection from temporary metadata copy
        List<Range> tempList = new ArrayList<>(writeRanges);
        Pair<String, Integer> remove = clientSocket.getAddressInfoPair();
        for (Range r : writeRanges) {
            if (r.getAddress().equals(remove.getLeft()) && r.getPort() == remove.getRight()) {
                tempList.remove(r);
                break;
            }
        }

        //connect to random server
        Range toConnect = tempList.get(new Random().nextInt(tempList.size()));
        buildConnection(new Pair<>(toConnect.getAddress(), toConnect.getPort()));
    }

    private void startListening() {
        //Disconnect/error has not occured. No need to start a new thread
        if (listenerThread == null || listenerThread.isAlive()) {
            return;
        }

        //Thread is dead. Must be restarted
        listenerThread = new ListenerThread(listenerPort);
        listenerThread.start();
    }

    private void stopListening() {
        if (listenerThread == null) {
            return;
        }

        try {
            listenerThread.endThread();
        } catch (IOException e) {
            System.err.println("Thread could not be shut down");
        }
    }
}

