package de.tum.i13.KVStore;

import de.tum.i13.KVserver.nio.StartKVServer;
import de.tum.i13.ecs.StartECS;
import de.tum.i13.shared.MessagingProtocol;

import java.io.IOException;

public class ServerToServerConnection extends ConnectionManager {
    public ServerToServerConnection(ActiveConnection clientSocket) {
        super(clientSocket);
    }

    public ServerToServerConnection() {
        super(null);
    }

    public void putHashedKey(String key, String value) {
        try {
            String[] response = sendMessage(MessagingProtocol.StatusType.PUT_HASH, new String[]{key, value}).split(" ");

            if (!response[0].equals("put_success") && !response[0].equals("put_update")) {
                StartKVServer.logger.warning("Put hashed key returned error. Retrying...");
                backOff.backOffWithJitter();
                putHashedKey(key, value);
            }
        } catch (IOException e) {
            StartKVServer.logger.severe("Putting hasedKey to " + clientSocket.getRemoteInfo() + " failed");
        }
    }

    public void deleteHashedKey(String key) {
        try {
            String[] response = sendMessage(MessagingProtocol.StatusType.DELETE_HASH, new String[]{key}).split(" ");

            if (!response[0].equals("delete_success")) {
                StartKVServer.logger.warning("Delete hashed key returned error. Retrying...");
                backOff.backOffWithJitter();
                deleteHashedKey(key);
            }
        } catch (IOException e) {
            StartKVServer.logger.severe("Deleting hasedKey to " + clientSocket.getRemoteInfo() + " failed");
        }
    }

    public String messageECS(MessagingProtocol.StatusType command, String message) throws IOException {
        return sendMessage(command, new String[]{message});
    }

    public String register(String ip, int port) throws IOException {
        return sendMessage(MessagingProtocol.StatusType.REGISTER, new String[]{ip, "" + port});
    }

    public String deregister(String ip, int port) throws IOException {
        return sendMessage(MessagingProtocol.StatusType.DEREGISTER, new String[]{ip, "" + port});
    }

    public void sendMetadata(String metadataString) {
        try {
            sendMessage(MessagingProtocol.StatusType.METADATA, new String[]{metadataString});
        } catch (IOException e) {
            StartECS.logger.severe("Sending metadata to " + clientSocket.getRemoteInfo() + " failed");
        }
    }

    public void setWriteLock(String predecessorIp, Integer predecessorPort) {
        try {
            sendMessage(MessagingProtocol.StatusType.SET_WRITE_LOCK, new String[]{predecessorIp,
                    String.valueOf(predecessorPort)});
        } catch (IOException e) {
            StartECS.logger.severe("Setting " + clientSocket.getRemoteInfo() + " to write_lock failed");
        }
    }

    public void ping() throws IOException {
        sendMessage(MessagingProtocol.StatusType.PING, new String[0]);
    }

    public void startServer() {
        try {
            sendMessage(MessagingProtocol.StatusType.START_SERVER);
        } catch (IOException e) {
            StartKVServer.logger.severe("Starting server to " + clientSocket.getRemoteInfo() + " failed");
        }
    }
}
