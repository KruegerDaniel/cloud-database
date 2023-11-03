package de.tum.i13.KVserver.kv;

import de.tum.i13.KVserver.nio.StartKVServer;
import de.tum.i13.ecs.ECSMessage;
import de.tum.i13.shared.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static de.tum.i13.client.KVClient.convertStrArrayToStr;

/**
 * This class is responsible for handling client messages. Valid commands are:
 * put <key> <value>
 * get <key>
 * delete <key>
 * Failed operations return an ERROR message, else SUCCESS message is returned.
 * Other unknown commands return an Error message.
 */
public class KVCommandProcessor implements CommandProcessor {
    /**
     * The KVManager is responsible for handling cache and accessing the Database
     */
    private final KVManager kvManager;

    public KVCommandProcessor(KVManager kvManager) {
        this.kvManager = kvManager;
    }


    /**
     * This method will parse the client message for a put, get, delete message.
     * The proper storage handling is passed on to the KVManager.
     * Additionally, the method provides correct message handling for put_hash, keyrange, set_write_lock, metadata, ping.
     * The above mentioned messages are sent from either another KVServer, or the ECS.
     * Those new messages are also handled by the KVManager
     * If the command is unknown, an ERROR message is returned
     *
     * @param command String that contains the entire message from the client
     * @return storeAnswer is a KVmessage.toString(). Can be a SUCCESS or ERROR message
     */
    @Override
    public String process(String command) {
        command = command.replace("\r\n", "");
        String[] commandArgs = command.split(" ");
        MessagingProtocol storeAnswer;
        if (kvManager.server_stopped && (commandArgs[0].equals("keyrange") ||
                commandArgs[0].equals("put") || commandArgs[0].equals("get") || commandArgs[0].equals("delete"))) {
            return new KVMessage(MessagingProtocol.StatusType.SERVER_STOPPED).toString() + "\r\n";
        }
        switch (commandArgs[0]) {

            case "put" -> {
                String[] valueArr = new String[commandArgs.length - 2];
                System.arraycopy(commandArgs, 2, valueArr, 0, valueArr.length);

                storeAnswer = put(commandArgs[1], valueArr, false);
            }
            case "put_hash" -> {
                // Inserts an already hashed key and its value into the database
                String[] valueArr = new String[commandArgs.length - 2];
                System.arraycopy(commandArgs, 2, valueArr, 0, valueArr.length);

                storeAnswer = put(commandArgs[1], valueArr, true);
            }
            case "get" -> {
                storeAnswer = get(commandArgs[1]);
            }
            case "delete" -> {
                storeAnswer = delete(commandArgs[1], false);
            }
            case "publish" -> {
                String[] valueArr = new String[commandArgs.length - 2];
                System.arraycopy(commandArgs, 2, valueArr, 0, valueArr.length);

                storeAnswer = publish(commandArgs[1], valueArr);
            }
            case "subscribe" -> {
                storeAnswer = subscribe(commandArgs[1], commandArgs[2], commandArgs[3], commandArgs[4]);
            }
            case "unsubscribe" -> {
                storeAnswer = unsubscribe(commandArgs[1], commandArgs[2]);
            }
            case "delete_hash" -> {
                // Inserts an already hashed key and its value into the database
                storeAnswer = delete(commandArgs[1], true);
            }
            case "keyrange" -> {
                storeAnswer = keyrange(false);
            }
            case "set_write_lock" -> {
                StartKVServer.logger.info("Received Write_lock. Transferring keys...");
                kvManager.write_lock = true;
                // index 1 is predecessor ip
                // index 2 is predecessor port
                // index 3 is a flag that is set if this write_lock is invoked by a shutdown hook

                try {
                    // flushing cache to disk because of possible concurrent
                    // modification exception when iterating over cache
                    kvManager.flushCacheToDisk();
                } catch (IOException ignored) {
                }
                if (commandArgs.length == 3) { // If this is NOT a shutdown hook, start a new thread
                    new Thread(() -> {
                        // Shutdown hook Thread can not start new Threads apparently,
                        // took me a while to find that out
                        try {
                            kvManager.transferKeys(commandArgs[1], Integer.parseInt(commandArgs[2]), false);
                            kvManager.write_lock = false; // After transfer is done, writelock can be removed
                            StartKVServer.logger.info("Write_lock lifted");
                        } catch (IOException ignored) {
                        }
                    }).start();
                } else {
                    if (Integer.parseInt(commandArgs[2]) != -1) { // if the last server flag is set (-1) we don't send keys
                        try {
                            kvManager.transferKeys(commandArgs[1], Integer.parseInt(commandArgs[2]), false);
                            kvManager.write_lock = false; // After transfer is done, writelock can be removed
                            StartKVServer.logger.info("Write_lock lifted");
                        } catch (IOException ignored) {
                        }
                    }
                }
                storeAnswer = new ECSMessage(MessagingProtocol.StatusType.ACK);
            }
            case "metadata" -> {
                storeAnswer = metadata(commandArgs[1]);
            }
            case "ping" -> {
                StartKVServer.logger.finest("Received Ping from ECS");
                storeAnswer = new ECSMessage(MessagingProtocol.StatusType.PING);
            }
            case "start_server" -> {
                StartKVServer.logger.info("Server received go-ahead. Setting server_stopped to false");
                kvManager.server_stopped = false; // The server adjusts its own range
                storeAnswer = new ECSMessage(ECSMessage.StatusType.ACK);
            }
            case "keyrange_read" -> {
                storeAnswer = keyrange(true);
            }
            default -> storeAnswer = new KVMessage(MessagingProtocol.StatusType.ERROR, new String[]{"Unknown Command"});
        }
        return storeAnswer.toString() + "\r\n";
    }

    /**
     * Notification for an accepted connection
     *
     * @param address       address of the KVserver
     * @param remoteAddress the address of the client
     * @return notification message
     */
    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        StartKVServer.logger.info("New connection: " + remoteAddress.toString());
        return "Connection to T15 Storage KVserver established: " + address.toString() + "\r\n";
    }

    /**
     * Logging for a closed connection
     *
     * @param address address of the KVserver
     */
    @Override
    public void connectionClosed(InetAddress address) {
        StartKVServer.logger.info("Connection closed: " + address.toString());
    }

    /**
     * @param key
     * @param values
     * @param isHashed if the key is already hashed, due to KV coming from another server, then true
     * @return WRITE_LOCK | SUCCESS | PUT_ERROR
     */
    private KVMessage put(String key, String[] values, boolean isHashed) {
        if (kvManager.write_lock) {
            return new KVMessage(MessagingProtocol.StatusType.SERVER_WRITE_LOCK);
        }

        String value = convertStrArrayToStr(values, " ");

        try {
            StartKVServer.logger.info("The client tries to insert the pair " + key + ": " + value + " in the database.");
            return kvManager.put(key, value, isHashed);
        } catch (Exception e) {
            StartKVServer.logger.severe("There has been an error while trying to insert the pair " + key + ": " + value + " in the database.");
            return new KVMessage(MessagingProtocol.StatusType.PUT_ERROR, new String[]{key, value});
        }
    }

    /**
     * @param key
     * @return GET_SUCCESS {key} {value} | GET_ERROR
     */
    private KVMessage get(String key) {
        try {
            StartKVServer.logger.info("The client tries to read the value of the key: " + key + ".");
            return kvManager.get(key);
        } catch (Exception e) {
            StartKVServer.logger.severe("There has been an error while trying to read the value of the key: " + key + ".");
            return new KVMessage(MessagingProtocol.StatusType.GET_ERROR, new String[]{key});
        }
    }

    /**
     * @param key
     * @param isHashed
     * @return
     */
    private KVMessage delete(String key, boolean isHashed) {
        if (kvManager.write_lock) {
            return new KVMessage(MessagingProtocol.StatusType.SERVER_WRITE_LOCK);
        }

        try {
            StartKVServer.logger.info("The client tries to delete the key: " + key + " and its associated value from the database.");
            return kvManager.delete(key, isHashed);
        } catch (Exception e) {
            StartKVServer.logger.severe("There has been an error while trying to delete the key: " + key + " and its associated value.");
            return new KVMessage(MessagingProtocol.StatusType.DELETE_ERROR, new String[]{key});
        }
    }

    /**
     * Returns the kvmanagers metadata. Depending on parameter, will return readranges or writeranges
     *
     * @param isReadRange true, when readranges are requested
     * @return KEYRANGE_READ_SUCCESS | KEYRANGE_SUCCESS
     */
    private KVMessage keyrange(boolean isReadRange) {
        StringBuilder response = new StringBuilder();

        if (isReadRange) {
            StartKVServer.logger.info("Client requested read-keyrange");
            for (Range r : kvManager.getReplicationData()) {
                response.append(r);
            }
            return new KVMessage(KVMessage.StatusType.KEYRANGE_READ_SUCCESS, new String[]{response.toString()});
        }

        StartKVServer.logger.info("Client requested keyrange");

        for (Range r : kvManager.getWriteRanges()) {
            response.append(r);
        }
        return new KVMessage(KVMessage.StatusType.KEYRANGE_SUCCESS, new String[]{response.toString()});
    }

    /**
     * @param metadata
     * @return
     */
    private ECSMessage metadata(String metadata) {
        StartKVServer.logger.info("Received metadata update from ECS : " + metadata);
        List<Range> oldWriteranges = new ArrayList<>(kvManager.getWriteRanges());
        kvManager.setWriteRanges(new ArrayList<>());
        String[] ranges = metadata.split(";"); // Ranges are seperated by semicolons
        for (String range : ranges) {
            kvManager.writeRanges.add(Range.parseRange(range)); // Parse every range and add it to the metadata
        }
        // Calculate getRanges from metadata
        kvManager.updateLocalRanges(); // The server adjusts its own range

        try { //replication is handled inside try-block
            // when updated metadata received, handle replication
            if (kvManager.getWriteRanges().size() >= 3 && oldWriteranges.size() != 0) {

                if (kvManager.getWriteRanges().size() == 3) {
                    kvManager.initiateReplication();
                } else if (oldWriteranges.size() < kvManager.getWriteRanges().size()) { //new server added
                    kvManager.newServerReplication(oldWriteranges);
                } else { //server was removed
                    kvManager.removeServerReplication(oldWriteranges);
                }
            }

            //update read ranges
            kvManager.setReplicationData(KVHash.calculateReplicatorRange(kvManager.writeRanges));
            kvManager.updateLocalRanges();

        } catch (IOException e) {
            StartKVServer.logger.severe("Replication failed");
        }
        return new ECSMessage(ECSMessage.StatusType.ACK);
    }

    /**
     * Put the KV in Server. If successful, forward message to ECS pubsub service.
     *
     * @param key    key
     * @param values array of values
     * @return PUBLICATION_SUCCESS | PUBLICATION_ERROR
     */
    public KVMessage publish(String key, String[] values) {
        if (kvManager.write_lock) {
            return new KVMessage(MessagingProtocol.StatusType.PUBLICATION_ERROR, new String[]{"Server", "is", "currently", "writelocked"});
        }

        //first attempt to push
        String value = convertStrArrayToStr(values, " ");
        KVMessage putResponse = new KVMessage(MessagingProtocol.StatusType.ERROR);
        try {
            StartKVServer.logger.info("The client tries to publish. Temporarily storing " + key + " " + value);
            putResponse = kvManager.put(key, value, false);
        } catch (Exception e) {
            StartKVServer.logger.info("Could not store " + key + " " + value + ". Return publication_error");
            return new KVMessage(MessagingProtocol.StatusType.PUBLICATION_ERROR, new String[]{"KV", "could", "not", "be", "stored"});
        }

        //catch the push failure messages and return the message. Only pass on success or update
        if (putResponse.getStatusType() != MessagingProtocol.StatusType.PUT_SUCCESS &&
                putResponse.getStatusType() != MessagingProtocol.StatusType.PUT_UPDATE) {
            StartKVServer.logger.info("Could not store " + key + " " + value + ".");
            return new KVMessage(putResponse.getStatusType(), new String[]{"KV", "could", "not", "be", "stored"});
        }

        //send publish to ECS and return success or error
        try {
            String[] response = kvManager.forwardToECS(MessagingProtocol.StatusType.PUBLISH, key + " " + value
                    + " Retention: " + kvManager.getRetentionTime()).split(" ");
            String[] temp = (key + " " + value).split(" ");
            return response[0].equals("publication_success") ?
                    new KVMessage(MessagingProtocol.StatusType.PUBLICATION_SUCCESS, temp) :
                    new KVMessage(MessagingProtocol.StatusType.PUBLICATION_ERROR, new String[]{"publish", "failed"});
        } catch (IOException e) {
            return new KVMessage(MessagingProtocol.StatusType.PUBLICATION_ERROR, new String[]{"PubSub", "connection", "failed"});
        }
    }

    /**
     * Forward the subscribe message to ECS pubsub service.
     *
     * @param SID, key, localaddress, port
     * @return SUBSCRIBE_SUCCESS | ERROR
     */
    public KVMessage subscribe(String SID, String key, String port, String IP) {
        //
        StartKVServer.logger.info("Client attempts to subscribe with " + SID + " " + key + " " + IP + ":" + port);
        String hashKey = KVHash.bytesToHex(KVHash.hashKey(key));
        if (!kvManager.inReadRange(hashKey)) {
            StartKVServer.logger.info("Provided key out of range. Server not responsible");
            return new KVMessage(MessagingProtocol.StatusType.SERVER_NOT_RESPONSIBLE);
        }

        try {
            String message = SID + " " + key + " " + port + " " + IP;
            String[] response = kvManager.forwardToECS(MessagingProtocol.StatusType.SUBSCRIBE, message).split(" ");
            return response[0].equals("subscribe_success") ?
                    new KVMessage(MessagingProtocol.StatusType.SUBSCRIBE_SUCCESS, new String[]{SID, key}) :
                    new KVMessage(MessagingProtocol.StatusType.ERROR, new String[]{"Subscribe", "failed"});

        } catch (IOException e) {
            return new KVMessage(MessagingProtocol.StatusType.ERROR);
        }
    }

    /**
     * Forward the unsubscribe message to ECS pubsub service.
     *
     * @param SID, key
     * @return UNSUBSCRIBE_SUCCESS | ERROR
     */
    public KVMessage unsubscribe(String SID, String key) {
        StartKVServer.logger.info("Client attempts to unsubscribe with " + SID + " " + key);
        String hashKey = KVHash.bytesToHex(KVHash.hashKey(key));
        if (!kvManager.inReadRange(hashKey)) {
            StartKVServer.logger.info("Provided key out of range. Server not responsible");
            return new KVMessage(MessagingProtocol.StatusType.SERVER_NOT_RESPONSIBLE);
        }
        try {
            String message = SID + " " + key;
            String[] response = kvManager.forwardToECS(MessagingProtocol.StatusType.UNSUBSCRIBE, message).split(" ");
            return response[0].equals("unsubscribe_success") ?
                    new KVMessage(MessagingProtocol.StatusType.UNSUBSCRIBE_SUCCESS, new String[]{key}) :
                    new KVMessage(MessagingProtocol.StatusType.ERROR, new String[]{"Subscribe", "failed"});

        } catch (IOException e) {
            return new KVMessage(MessagingProtocol.StatusType.ERROR);
        }
    }
}
