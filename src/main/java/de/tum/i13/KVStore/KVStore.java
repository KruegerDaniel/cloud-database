package de.tum.i13.KVStore;

import de.tum.i13.shared.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static de.tum.i13.client.KVClient.LOGGER;

/**
 * Library that implements the Messaging protocol from client to KVServer and responses
 */
public class KVStore extends ConnectionManager {

    public KVStore() {
        super(null);
    }

    public KVMessage put(String key, String value) {
        if (clientSocket == null) {
            LOGGER.info("No connection");
            return new KVMessage(MessagingProtocol.StatusType.ERROR, new String[]{"Not", "Connected"});
        }

        if (value.length() > 120_000 || key.length() > 20) {
            LOGGER.info("Key/Value length too long. Key: " + key.length() + ". Value: " + value.length());
            return new KVMessage(MessagingProtocol.StatusType.PUT_ERROR, new String[]{"Arg", "length", "exceeded"});
        }

        //send put message and get response
        LOGGER.info("Sending PUT request for key: " + key);
        String[] response = sendWrite(key, value, MessagingProtocol.StatusType.PUT, false);
        LOGGER.info("Answer to put " + key + ": " + response[0]);
        MessagingProtocol.StatusType status = MessagingProtocol.StatusType.ERROR;
        switch (response[0]) {
            case "put_success" -> status = KVMessage.StatusType.PUT_SUCCESS;
            case "put_error" -> status = KVMessage.StatusType.PUT_ERROR;
            case "put_update" -> status = KVMessage.StatusType.PUT_UPDATE;
            case "server_stopped" -> { // add timeout, if too many attempts fail
                backOff.backOffWithJitter();
                return put(key, value);
            }
            case "server_not_responsible" -> {
                keyrange(false);
                return put(key, value);
            }
            case "server_write_lock" -> {
                keyrange(false);
                backOff.backOffWithJitter();
                //check if the server is shutdown. If yes, then connect randomly
                if (!stillReachable()) {
                    randomConnect();
                }
                return put(key, value);
                //status = MessagingProtocol.StatusType.SERVER_WRITE_LOCK;
            }
        }
        backOff.resetBackOff();
        return new KVMessage(status, new String[]{key});
    }

    public KVMessage get(String key) {
        if (clientSocket == null) {
            LOGGER.info("No connection");
            return new KVMessage(MessagingProtocol.StatusType.ERROR, new String[]{"Not", "Connected"});
        }

        if (key.length() > 20) {
            LOGGER.info("Key length too long. Key: " + key.length());

            return new KVMessage(MessagingProtocol.StatusType.GET_ERROR, new String[]{"Arg", "length", "exceeded"});
        }

        LOGGER.info("Sending GET request for key: " + key);
        String[] response = sendWrite(key, "", MessagingProtocol.StatusType.GET, true);
        LOGGER.info("Answer to get " + key + ": " + response[0]);

        switch (response[0]) {
            case "get_success" -> {
                // get_success 13123123 science man

                String[] value = new String[response.length - 1];
                System.arraycopy(response, 2, value, 1, value.length - 1);
                value[0] = key;

                for (int i = 1; i < value.length; i++) {
                    value[i] = value[i].replace("\\n", "\n");
                }

                backOff.resetBackOff();
                return new KVMessage(MessagingProtocol.StatusType.GET_SUCCESS, value);
            }
            case "get_error" -> {
                backOff.resetBackOff();
                return new KVMessage(MessagingProtocol.StatusType.GET_ERROR, new String[]{key});
            }
            case "server_stopped" -> { // add timeout, if too many attempts fail
                backOff.backOffWithJitter();
                return get(key);
            }
            case "server_not_responsible" -> {
                keyrange(true);
                return get(key);
            }
            default -> {
                return null;
            }
        }
    }

    public KVMessage delete(String key) {
        if (clientSocket == null) {
            LOGGER.info("No connection");
            return new KVMessage(MessagingProtocol.StatusType.ERROR, new String[]{"Not", "Connected"});
        }

        if (key.length() > 20) {
            LOGGER.info("Key length too long. Key: " + key.length());
            return new KVMessage(MessagingProtocol.StatusType.DELETE_ERROR, new String[]{"Arg", "length", "exceeded"});
        }

        //send delete message and get a response
        LOGGER.info("Sending DELETE request for key: " + key);
        String[] response = sendWrite(key, "", MessagingProtocol.StatusType.DELETE, false);
        LOGGER.info("Answer to delete " + key + ": " + response[0]);

        KVMessage.StatusType status = MessagingProtocol.StatusType.ERROR;
        switch (response[0]) {
            case "delete_success" -> status = KVMessage.StatusType.DELETE_SUCCESS;
            case "delete_error" -> status = KVMessage.StatusType.DELETE_ERROR;
            case "server_stopped" -> {
                backOff.backOffWithJitter();
                return delete(key);
            }
            case "server_not_responsible" -> {
                keyrange(false);
                return delete(key);
            }
            case "server_write_lock" -> {
                keyrange(false);
                backOff.backOffWithJitter();
                //check if server is shutdown. If yes, then connect to random server
                if (!stillReachable()) {
                    randomConnect();
                }
                return delete(key);
                //status = MessagingProtocol.StatusType.SERVER_WRITE_LOCK;
            }
        }
        backOff.resetBackOff();
        return new KVMessage(status, new String[]{key});
    }

    /**
     * Sends a publish request to a KVServer. The KVstore will automatically attempt to connect to the correct KVserver
     *
     * @param key
     * @param value
     * @return PUBLICATION_ERROR | PUBLICATION_SUCCESS
     */
    public KVMessage publish(String key, String value) {
        if (clientSocket == null) {
            LOGGER.info("No connection");
            return new KVMessage(MessagingProtocol.StatusType.ERROR, new String[]{"Not", "Connected"});
        }
        if (key.length() > 20 || value.length() > 120_000) {
            LOGGER.info("Key/Value length too long. Key: " + key.length() + ". Value: " + value.length());
            return new KVMessage(MessagingProtocol.StatusType.PUBLICATION_ERROR, new String[]{"Arg", "length", "exceeded"});
        }
        //send publish message and receive response
        LOGGER.info("Sending PUBLISH request for key: " + key);
        String[] response = sendWrite(key, value, MessagingProtocol.StatusType.PUBLISH, false);
        LOGGER.info("Answer to PUBLISH " + key + ": " + response[0]);

        MessagingProtocol.StatusType status = MessagingProtocol.StatusType.ERROR;
        switch (response[0]) {
            case "publication_success" -> status = KVMessage.StatusType.PUBLICATION_SUCCESS;
            case "publication_error" -> status = KVMessage.StatusType.PUBLICATION_ERROR;
            case "server_stopped" -> { // add timeout, if too many attempts fail
                backOff.backOffWithJitter();
                return publish(key, value);
            }
            case "server_not_responsible" -> {
                keyrange(false);
                return publish(key, value);
            }
            case "server_write_lock" -> {
                keyrange(false);
                backOff.backOffWithJitter();
                //check if the server is shutdown. If yes, then connect randomly
                if (!stillReachable()) {
                    randomConnect();
                }
                return publish(key, value);
                //status = MessagingProtocol.StatusType.SERVER_WRITE_LOCK;
            }
        }
        return new KVMessage(status, new String[]{key, value});
    }

    /**
     * Will send a subscribe request to a KVServer.
     * Will include a SID and the port of the listenerThread in the request
     *
     * @param key  subscription topic
     * @param sid  String identifier
     * @param port Is parsed to int
     * @return SUBSCRIBE_SUCCESS (errors are handled)
     */
    public KVMessage subscribe(String sid, String key, String port) {
        if (clientSocket == null) {
            LOGGER.info("No connection");
            return new KVMessage(MessagingProtocol.StatusType.ERROR, new String[]{"Not", "Connected"});
        }
        if (key.length() > 20) {
            LOGGER.info("Key length too long. Key: " + key.length());
            return new KVMessage(MessagingProtocol.StatusType.ERROR, new String[]{"Arg", "length", "exceeded"});
        }
        //initialize the thread, if it is null
        if (listenerThread == null) {
            try {
                int portInt = Integer.parseInt(port);
                if (!ListenerThread.portAvailable(portInt)) {
                    return new KVMessage(MessagingProtocol.StatusType.ERROR, new String[]{"-", "Port", "not", "available"});
                }

                listenerThread = new ListenerThread(portInt);
                listenerThread.start();
                Thread.sleep(100);
                listenerPort = listenerThread.getPort();
            } catch (InterruptedException ie) {
                System.err.println("listenerThread sleep was interrupted");
            } catch (NumberFormatException nfe) {
                return new KVMessage(MessagingProtocol.StatusType.ERROR, new String[]{"-", "Port", "not", "a", "number"});
            }
        }

        LOGGER.info("Sending SUBSCRIBE request for key: " + key);
        String message = key + " " + listenerThread.getPort();
        String[] response = sendWrite(sid, message, MessagingProtocol.StatusType.SUBSCRIBE, true);
        LOGGER.info("Answer to SUBSCRIBE " + key + ": " + response[0]);

        MessagingProtocol.StatusType status = MessagingProtocol.StatusType.ERROR;
        switch (response[0]) {
            case "subscribe_success" -> status = KVMessage.StatusType.SUBSCRIBE_SUCCESS;
            case "server_stopped" -> { // add timeout, if too many attempts fail
                backOff.backOffWithJitter();
                return subscribe(sid, key, port);
            }
            case "server_not_responsible" -> {
                keyrange(true);
                return subscribe(sid, key, port);
            }
            case "server_write_lock" -> {
                keyrange(true);
                backOff.backOffWithJitter();
                //check if the server is shutdown. If yes, then connect randomly
                if (!stillReachable()) {
                    randomConnect();
                }
                return subscribe(sid, key, port);
                //status = MessagingProtocol.StatusType.SERVER_WRITE_LOCK;
            }
        }

        return new KVMessage(status, new String[]{sid, key});
    }

    /**
     * Will send an unsubscribe request to a KVServer.
     * Will include the SID in the request
     *
     * @param key subscription topic
     * @return
     */
    public KVMessage unsubscribe(String sid, String key) {
        if (clientSocket == null) {
            LOGGER.info("No connection");
            return new KVMessage(MessagingProtocol.StatusType.ERROR, new String[]{"Not", "Connected"});
        }
        if (key.length() > 20) {
            LOGGER.info("Key length too long. Key: " + key.length());
            return new KVMessage(MessagingProtocol.StatusType.ERROR, new String[]{"Arg", "length", "exceeded"});
        }
        //if listenerThread is null or terminated, it is due to subscribe not being initialized
        if (listenerThread == null || !listenerThread.isAlive()) {
            LOGGER.info("Unsubscribe unsuccessful: ListenerThread is not running");
            return new KVMessage(MessagingProtocol.StatusType.ERROR, new String[]{"No", "ongoing", "subscriptions"});
        }

        LOGGER.info("Sending UNSUBSCRIBE request for key: " + key);
        String[] response = sendWrite(sid, key, MessagingProtocol.StatusType.UNSUBSCRIBE, true);
        LOGGER.info("Answer to UNSUBSCRIBE " + key + ": " + response[0]);

        MessagingProtocol.StatusType status = MessagingProtocol.StatusType.ERROR;
        switch (response[0]) {
            case "unsubscribe_success" -> status = KVMessage.StatusType.UNSUBSCRIBE_SUCCESS;
            case "server_stopped" -> { // add timeout, if too many attempts fail
                backOff.backOffWithJitter();
                return unsubscribe(sid, key);
            }
            case "server_not_responsible" -> {
                keyrange(true);
                return unsubscribe(sid, key);
            }
            case "server_write_lock" -> {
                keyrange(true);
                backOff.backOffWithJitter();
                //check if the server is shutdown. If yes, then connect randomly
                if (!stillReachable()) {
                    randomConnect();
                }
                return unsubscribe(sid, key);
                //status = MessagingProtocol.StatusType.SERVER_WRITE_LOCK;
            }
        }

        return new KVMessage(status, new String[]{key});
    }

    /**
     * Sends a KVMessage of type keyrange to the server
     * The server responds with server_stopped or keyrange_success and the metadata
     */
    private void keyrange(boolean isRead) {
        // response is either keyrange_success with the metadata
        // or server_stopped without any further data
        String[] response = new String[0]; // if keyrange_success then split into response message and payload
        try {
            if (isRead) {
                response = sendMessage(MessagingProtocol.StatusType.KEYRANGE_READ).split(" ");
            } else {
                response = sendMessage(MessagingProtocol.StatusType.KEYRANGE).split(" ");
            }
        } catch (IOException e) {
            System.err.println("Connection error getting keyrange");
        }

        LOGGER.info("Keyrange answer received: " + response[0] + " " + response[1]);
        switch (response[0]) { // response message
            case "keyrange_success" -> {
                String[] ranges = response[1].split(";"); // Ranges are seperated by semicolons
                writeRanges = new ArrayList<>();
                for (String range : ranges) {
                    writeRanges.add(Range.parseRange(range)); // Parse every range and add it to the metadata
                }
            }
            case "keyrange_read_success" -> {
                String[] ranges = response[1].split(";"); // Ranges are seperated by semicolons
                readRanges = new ArrayList<>();
                for (String range : ranges) {
                    readRanges.add(Range.parseRange(range)); // Parse every range and add it to the metadata
                }
            }
            case "server_stopped" -> {
                backOff.backOffWithJitter();
                keyrange(isRead);
            }
        }
        backOff.resetBackOff();
    }

    private void updateServerConnection(String hashedKey, boolean isRead) {
        Pair<String, Integer> correctServer = findServerForHash(hashedKey, isRead);
        if (correctServer != null && !this.clientSocket.getAddressInfoPair().equals(correctServer)) {
            LOGGER.info("Update connection to Server: " + correctServer.getLeft() + ":" + correctServer.getRight());
            closeCurrentConnection();
            buildConnection(correctServer);
        }
    }

    /**
     * Finds the server in the metadata that is responsible for storing this particular key
     *
     * @param hashKey the hexadecimal String value of the hashed key
     * @return A pair of String and Integer representing the correct servers address and port
     */
    private Pair<String, Integer> findServerForHash(String hashKey, boolean isRead) {
        if (isRead && readRanges.size() != 0) {
            List<Pair<String, Integer>> possibleReadServers = new ArrayList<>();
            for (Range r : readRanges) {
                if (KVHash.inRange(hashKey, r.getLowerBound(), r.getUpperBound())) {
                    possibleReadServers.add(new Pair<>(r.getAddress(), r.getPort()));
                }
            }
            return possibleReadServers.get(new Random().nextInt(possibleReadServers.size()));
        } else {
            if (writeRanges.size() != 0) {
                for (Range r : writeRanges) {
                    if (KVHash.inRange(hashKey, r.getLowerBound(), r.getUpperBound())) {
                        return new Pair<>(r.getAddress(), r.getPort());
                    }
                }
            }
        }
        return null; // metadata is empty
    }

    /**
     * Used by commands that will write/publish KVs. ATM: put, delete, publish
     *
     * @param key
     * @param value
     * @param command
     * @return
     */
    private String[] sendWrite(String key, String value, MessagingProtocol.StatusType command, boolean isRead) {
        String keyHash = KVHash.bytesToHex(KVHash.hashKey(key)); //hash the key
        updateServerConnection(keyHash, isRead); // Check in our Metadata if we are connected to the right server

        value = value.replace("\n", "\\n");
        String[] toSend = value.isBlank() ? new String[]{key} : new String[]{key, value};
        String[] response;

        try {
            response = sendMessage(command, toSend).split(" ");
        } catch (IOException e) {
            LOGGER.warning("Connection was lost during send");
            return new String[]{"Connection", "lost"};
        }
        return response;
    }


}
