package de.tum.i13.KVserver.kv;

import de.tum.i13.KVStore.ServerToServerConnection;
import de.tum.i13.KVserver.kv.persistence.PersistenceHandler;
import de.tum.i13.KVserver.nio.StartKVServer;
import de.tum.i13.shared.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * KVManager is responsible for handling the cache and accessing the database if a cache miss occurs.
 */
public abstract class KVManager {
    public Map<String, String> cache;
    public boolean write_lock;
    public boolean server_stopped;
    /**
     * maxSize is the size of the cache
     */
    protected int maxSize;
    /**
     * Direct access to the database is delegated to the persistenceHandler
     */
    protected PersistenceHandler persistenceHandler;
    List<Range> writeRanges;
    List<Range> replicationData; // Range this server can serve read-requests to (includes replications)
    /**
     * KVManager has the address of the ecs, since he needs to contact the pubsub service
     */
    private InetSocketAddress ecs;
    private Range localWriteRange;
    private Range localReadRange;

    /**
     * The attribute saves the value of the retention time for the KVServer
     */
    private int retentionTime;

    public KVManager(int maxSize) {
        this.cache = new HashMap<>();
        this.maxSize = maxSize;
        writeRanges = new ArrayList<>();
        replicationData = new ArrayList<>();
        server_stopped = true;
    }

    static Pair<Range, Range> getSuccessors(List<Range> metadata, Range curr) {
        if (metadata.size() < 3) {
            return null;
        }

        for (int i = 0; i < metadata.size(); i++) {
            if (metadata.get(i).equals(curr)) {
                Range rep1 = metadata.get((i + 1) % metadata.size());
                Range rep2 = metadata.get((i + 2) % metadata.size());
                return new Pair<>(rep1, rep2);
            }
        }
        return null;
    }

    static Pair<Range, Range> getPredeccessors(List<Range> metadata, Range curr) {
        if (metadata.size() < 3) {
            return null;
        }

        for (int i = 0; i < metadata.size(); i++) {
            if (metadata.get(i).equals(curr)) {
                Range rep1 = metadata.get(Math.floorMod((i - 1), metadata.size()));
                Range rep2 = metadata.get(Math.floorMod((i - 2), metadata.size()));
                return new Pair<>(rep1, rep2);
            }
        }
        return null;
    }

    public InetSocketAddress getEcs() {
        return ecs;
    }

    public void setEcs(InetSocketAddress ecs) {
        this.ecs = ecs;
    }

    public List<Range> getReplicationData() {
        return replicationData;
    }

    public void setReplicationData(List<Range> replicationData) {
        this.replicationData = replicationData;
    }

    public List<Range> getWriteRanges() {
        return writeRanges;
    }

    public void setWriteRanges(List<Range> writeRanges) {
        this.writeRanges = writeRanges;
    }

    public void setPersistenceHandler(PersistenceHandler persistenceHandler) {
        this.persistenceHandler = persistenceHandler;
    }

    public int getRetentionTime() {
        return retentionTime;
    }

    public void setRetentionTime(int retentionTime) {
        this.retentionTime = retentionTime;
    }

    /**
     * Inserts a key-value pair into the KVServer.
     *
     * @param key   the key that identifies the given value.
     * @param value the value that is indexed by the given key.
     * @return a message that confirms the insertion of the tuple or an error.
     * @throws Exception if put command cannot be executed (e.g. not connected to any
     *                   KV KVserver).
     */
    public KVMessage put(String key, String value, boolean isHashed) throws Exception {
        String hashedKey = key;
        if (!isHashed) {
            hashedKey = KVHash.bytesToHex(KVHash.hashKey(key));
        }
        if (!inWriteRange(hashedKey) && !isHashed) {
            StartKVServer.logger.info("Provided key out of range. Server not responsible");
            return new KVMessage(MessagingProtocol.StatusType.SERVER_NOT_RESPONSIBLE);
        }
        MessagingProtocol.StatusType responseStatus = MessagingProtocol.StatusType.PUT_SUCCESS;
        String cacheValue = cache.get(hashedKey);
        updatePut(hashedKey);
        cache.put(hashedKey, value);
        if (cacheValue != null) { // cache hit
            responseStatus = KVMessage.StatusType.PUT_UPDATE;
            StartKVServer.logger.info("The system updated the existing key: " + hashedKey + " with the new value: " + value + " in the cache.");
        }

        Pair<Pair<String, Integer>, Pair<String, Integer>> replicators = getSuccessors();
        if (replicators != null && !isHashed) {
            forwardToReplicators(true, hashedKey, value, replicators);
        }

        return new KVMessage(responseStatus, new String[]{key});
    }

    /**
     * Checks if this server is responsible for the key specifiedby comparing it to the local range
     */
    protected boolean inWriteRange(String hashedKey) {
        return KVHash.inRange(hashedKey, localWriteRange.getLowerBound(), localWriteRange.getUpperBound());
    }

    protected boolean inReadRange(String hashedKey) {
        return KVHash.inRange(hashedKey, localReadRange.getLowerBound(), localReadRange.getUpperBound());
    }

    /**
     * when triggered (on metadata receival), goes through metadata and adjusts
     * localRange to the range specified in the metadata
     */
    public void updateLocalRanges() {
        for (Range range : writeRanges) {
            if (localWriteRange.getAddress().equals(range.getAddress()) && localWriteRange.getPort() == range.getPort()) {
                this.localWriteRange = range;
            }
        }
        for (Range range : replicationData) {
            if (localWriteRange.getAddress().equals(range.getAddress()) && localWriteRange.getPort() == range.getPort()) {
                this.localReadRange = range;
            }
        }
    }

    /**
     * Retrieves the value for a given key from the KVServer.
     *
     * @param key the key that identifies the value.
     * @return the value, which is indexed by the given key.
     * @throws Exception if getFromDB command cannot be executed (e.g. not connected to any
     *                   KV KVserver).
     */
    public KVMessage get(String key) throws Exception {
        String hashKey = KVHash.bytesToHex(KVHash.hashKey(key));
        if (!inReadRange(hashKey)) {
            StartKVServer.logger.info("Provided key out of range. Server not responsible");
            return new KVMessage(MessagingProtocol.StatusType.SERVER_NOT_RESPONSIBLE);
        }
        KVMessage.StatusType responseStatus = KVMessage.StatusType.GET_SUCCESS;
        String cacheValue = cache.get(hashKey);
        if (cacheValue == null) { // cache miss
            String dbValue = persistenceHandler.getFromDB(hashKey);
            if (dbValue == null) { // db miss, key not present anywhere
                StartKVServer.logger.warning("The key: " + hashKey + " requested by the client has no associated value in the database or cache.");
                responseStatus = KVMessage.StatusType.GET_ERROR;
                return new KVMessage(responseStatus, new String[]{key});
            } else { // key only in db => load key into cache and return
                cache.put(hashKey, dbValue);
                updatePut(hashKey);
                StartKVServer.logger.info("The system found the KVPair: " + hashKey + ": " + dbValue + " on disk.");
                return new KVMessage(responseStatus, new String[]{key, dbValue});
            }
        } else { // key in cache, return value
            updateGet(hashKey);
            StartKVServer.logger.info("The system found the KVPair: " + hashKey + ": " + cacheValue + " in the cache.");
            return new KVMessage(responseStatus, new String[]{key, cacheValue});
        }
    }

    /**
     * Deletes the value and key for a given key from the KVServer.
     *
     * @param key the key that identifies the value.
     * @return whether the operation was successful or not
     * @throws Exception if delete command cannot be executed (e.g. not connected to any
     *                   KV KVserver).
     */
    public KVMessage delete(String key, boolean isHashed) throws Exception { // if a key is to be deleted, simply remove it from cache and db
        String hashedKey = key;
        if (!isHashed) {
            hashedKey = KVHash.bytesToHex(KVHash.hashKey(key));
        }
        if (!inWriteRange(hashedKey) && !isHashed) {
            StartKVServer.logger.info("Provided key out of range. Server not responsible");
            return new KVMessage(MessagingProtocol.StatusType.SERVER_NOT_RESPONSIBLE);
        }
        KVMessage.StatusType responseStatus = KVMessage.StatusType.DELETE_SUCCESS;
        String cacheValue = cache.remove(hashedKey);
        updateDelete(hashedKey);
        String dbKey = persistenceHandler.deleteFromDB(hashedKey);
        if (cacheValue == null) {
            if (dbKey == null) {
                StartKVServer.logger.info("The key: " + hashedKey + " passed by the client doesn't exist in the database or cache.");
                responseStatus = KVMessage.StatusType.DELETE_ERROR;
                return new KVMessage(responseStatus, new String[]{key});
            }
        }
        StartKVServer.logger.info("The system deleted the key: " + hashedKey + " and its paired value from the database");
        Pair<Pair<String, Integer>, Pair<String, Integer>> replicators = getSuccessors();
        if (replicators != null && !isHashed) {
            forwardToReplicators(false, hashedKey, null, replicators);
        }
        return new KVMessage(responseStatus, new String[]{key});
    }

    /**
     * Removes all keys and values in this cache and writes them to the persistence file
     * Afterwards, it initates a new cache
     */
    public void flushCacheToDisk() throws IOException {
        for (Map.Entry<String, String> entry : cache.entrySet()) {
            if (persistenceHandler.getFromDB(entry.getKey()) == null) {
                persistenceHandler.insertInDB(entry.getKey(), entry.getValue());
            } else {
                persistenceHandler.updateInDB(entry.getKey(), entry.getValue());
            }
        }
        cache = new HashMap<>();
    }

    public void setLocalWriteRange(Range localWriteRange) {
        this.localWriteRange = localWriteRange;
    }

    abstract void updatePut(String key) throws IOException;

    abstract void updateGet(String key);

    abstract void updateDelete(String key);

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    /**
     * Depending on writeTransfer, this method either forwards all its keys or only within its write range.
     * The new KVServer will only receive the hashed keys and can't see the plain text keys
     *
     * @param ip             ip of the server to tranfer to
     * @param port           port of the server that will receive the keys
     * @param onlyWriteRange flag that determines if coordination keys (i.e. in write range) should be send to a replicator
     */
    public void transferKeys(String ip, int port, boolean onlyWriteRange) throws IOException {
        Map<String, String> transferMap = new HashMap<>();
        // add all elements from persistent database
        if (onlyWriteRange) {
            StartKVServer.logger.info("Transfering keys in write range to " + ip + ":" + port);
            persistenceHandler.getRangeData(this.localWriteRange.getLowerBound(), this.localWriteRange.getUpperBound(), true)
                    .forEach(s -> transferMap.put(s.getLeft(), s.getRight()));
        } else {
            StartKVServer.logger.info("Transfering keys out of write range to " + ip + ":" + port);
            persistenceHandler.getRangeData(this.localWriteRange.getLowerBound(), this.localWriteRange.getUpperBound(), false)
                    .forEach(s -> transferMap.put(s.getLeft(), s.getRight()));
        }
        ServerToServerConnection serverConnection = new ServerToServerConnection(null);
        serverConnection.buildConnection(new Pair<>(ip, port)); // connect to predecessor node and put all keys subsequently
        for (Map.Entry<String, String> stringStringEntry : transferMap.entrySet()) {
            serverConnection.putHashedKey(stringStringEntry.getKey(), stringStringEntry.getValue());

            //delete the key after securing it in successor
            try {
                if (!inReadRange(stringStringEntry.getKey())) {
                    // This happens in case of a graceful shutdown, so delete all keys now
                    persistenceHandler.deleteFromDB(stringStringEntry.getKey());
                }
            } catch (Exception ignored) {
            }
        }

        serverConnection.startServer(); // After the new server received all the keys, we can start it
        serverConnection.closeCurrentConnection();
    }

    Pair<Pair<String, Integer>, Pair<String, Integer>> getSuccessors() {
        if (writeRanges.size() < 3) {
            return null;
        }

        for (int i = 0; i < writeRanges.size(); i++) {
            if (writeRanges.get(i).equals(localWriteRange)) {
                Range rep1 = writeRanges.get((i + 1) % writeRanges.size());
                Range rep2 = writeRanges.get((i + 2) % writeRanges.size());
                return new Pair<>(new Pair<>(rep1.getAddress(), rep1.getPort()), new Pair<>(rep2.getAddress(), rep2.getPort()));
            }
        }
        return null;
    }

    Pair<Pair<String, Integer>, Pair<String, Integer>> getPredeccessors() {
        if (writeRanges.size() < 3) {
            return null;
        }

        for (int i = 0; i < writeRanges.size(); i++) {
            if (writeRanges.get(i).equals(localWriteRange)) {
                Range rep1 = writeRanges.get((i - 1) % writeRanges.size());
                Range rep2 = writeRanges.get((i - 2) % writeRanges.size());
                return new Pair<>(new Pair<>(rep1.getAddress(), rep1.getPort()), new Pair<>(rep2.getAddress(), rep2.getPort()));
            }
        }
        return null;
    }

    public void deleteAllOutOfReadRangeData() throws IOException {
        Map<String, String> transferMap = new HashMap<>();
        persistenceHandler.getRangeData(this.localWriteRange.getLowerBound(), this.localWriteRange.getUpperBound(), false)
                .forEach(s -> transferMap.put(s.getLeft(), s.getRight()));
        for (Map.Entry<String, String> stringStringEntry : transferMap.entrySet()) {
            //delete the key after securing it in successor
            try {
                if (!inReadRange(stringStringEntry.getKey())) {
                    // This happens in case of a graceful shutdown, so delete all keys now
                    persistenceHandler.deleteFromDB(stringStringEntry.getKey());
                }
            } catch (Exception ignored) {
            }

        }
    }

    /**
     * Handles the replication in case a new server is registered
     *
     * @param oldMetadata A copy of metadata before the new server was registered
     * @throws IOException transferkeys could throw an IOException
     */
    void newServerReplication(List<Range> oldMetadata) throws IOException {
        if (oldMetadata == null) {
            return;
        }

        //find the new server
        Range newServer = null;
        for (Range r : writeRanges) {
            if (!oldMetadata.contains(r)) {
                newServer = r;
                break;
            }
        }

        //get the responsible successors. The 1st successor transfers to the new server.
        Pair<Range, Range> successors = getSuccessors(writeRanges, newServer);
        Pair<Range, Range> temp = getSuccessors(writeRanges, successors.getRight());
        if (localWriteRange.equals(successors.getLeft())) {
            transferKeys(newServer.getAddress(), newServer.getPort(), false);
        }
        setReplicationData(KVHash.calculateReplicatorRange(writeRanges));
        updateLocalRanges();

        //The 1st, 2nd, and 3rd successor of the new Server are the only nodes that needs to delete outOfReadRange data
        if (localWriteRange.equals(successors.getLeft()) ||
                localWriteRange.equals(successors.getRight()) ||
                localWriteRange.equals(temp.getLeft())) {
            deleteAllOutOfReadRangeData();
        }
    }

    /**
     * Handles the replication in case a server is removed
     *
     * @param oldMetadata a copy of the metadata before a server was removed
     * @throws IOException transferkeys may throw an IOException
     */
    void removeServerReplication(List<Range> oldMetadata) throws IOException {
        if (oldMetadata == null) {
            return;
        }

        //find the deleted server
        Range deletedServer = null;
        for (Range r : oldMetadata) {
            if (!writeRanges.contains(r)) {
                deletedServer = r;
                break;
            }
        }
        setReplicationData(KVHash.calculateReplicatorRange(writeRanges));
        updateLocalRanges();
        Range replicateTo = getSuccessors(oldMetadata, deletedServer).getRight();
        if (localWriteRange.equals(getPredeccessors(oldMetadata, deletedServer).getLeft())) {
            transferKeys(replicateTo.getAddress(), replicateTo.getPort(), true);
        } else if (localWriteRange.equals(getPredeccessors(oldMetadata, deletedServer).getRight())) {
            replicateTo = getSuccessors(oldMetadata, deletedServer).getLeft();
            transferKeys(replicateTo.getAddress(), replicateTo.getPort(), true);
        } else if (localWriteRange.getUpperBound().equals(getSuccessors(oldMetadata, deletedServer).getLeft().getUpperBound())) {
            replicateTo = getSuccessors(writeRanges, localWriteRange).getRight();
            transferKeys(replicateTo.getAddress(), replicateTo.getPort(), true);
        }

    }

    /**
     *
     */
    void initiateReplication() throws IOException {
        if (writeRanges.size() != 3) {
            return;
        }
        setReplicationData(KVHash.calculateReplicatorRange(writeRanges));
        updateLocalRanges();
        // we have to transfer outOfWriteRange as well, since, the writeranges were minimized before
        Pair<Pair<String, Integer>, Pair<String, Integer>> replicators = getSuccessors();
        transferKeys(replicators.getRight().getLeft(), replicators.getRight().getRight(), true);
        transferKeys(replicators.getRight().getLeft(), replicators.getRight().getRight(), false);

        transferKeys(replicators.getLeft().getLeft(), replicators.getLeft().getRight(), true);
        transferKeys(replicators.getLeft().getLeft(), replicators.getLeft().getRight(), false);

    }

    /**
     * When given a single put or delete request, it is forwarded to the replicators.
     *
     * @param isPut       if true, then put. If false, then delete
     * @param hashedKey
     * @param value
     * @param replicators servers to forward the replicator KVs to
     */
    private void forwardToReplicators(boolean isPut, String hashedKey, String value, Pair<Pair<String, Integer>, Pair<String, Integer>> replicators) {
        ServerToServerConnection connect1 = new ServerToServerConnection();
        ServerToServerConnection connect2 = new ServerToServerConnection();

        connect1.buildConnection(replicators.getLeft()); //connect to replicator 1
        connect2.buildConnection(replicators.getRight()); //connect to replicator 2

        if (isPut) {
            connect1.putHashedKey(hashedKey, value);
            connect2.putHashedKey(hashedKey, value);
        } else {
            connect1.deleteHashedKey(hashedKey);
            connect2.deleteHashedKey(hashedKey);
        }

        connect1.closeCurrentConnection();
        connect2.closeCurrentConnection();
    }

    /**
     * @param command
     * @param message
     */
    String forwardToECS(MessagingProtocol.StatusType command, String message) throws IOException {
        ServerToServerConnection connection = new ServerToServerConnection();
        connection.buildConnection(new Pair<>(ecs.getHostName(), ecs.getPort()));
        String response = connection.messageECS(command, message);
        connection.closeCurrentConnection();
        return response;
    }
}
