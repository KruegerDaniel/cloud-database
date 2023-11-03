package de.tum.i13.KVserver.kv;

import de.tum.i13.KVserver.nio.StartKVServer;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * KVManagerFIFO implements the FIFO strategy for the cache
 */
public class KVManagerFIFO extends KVManager {
    public Queue<String> queue;

    public KVManagerFIFO(int maxSize) {
        super(maxSize);
        queue = new LinkedBlockingQueue<>();
    }


    void updatePut(String key) throws IOException {
        if (queue.contains(key)) {
            queue.remove(key);
            queue.add(key);
            return; //The queue is not affected by updates
        }
        if (queue.size() == maxSize) {
            StartKVServer.logger.info("Wrote " + key + " in cache");
            String removedKey = queue.remove();
            String removedValue = cache.remove(removedKey);
            if (persistenceHandler.getFromDB(removedKey) == null) {
                persistenceHandler.insertInDB(removedKey, removedValue);
                StartKVServer.logger.info("Wrote " + removedKey + ": " + removedValue + " into database");
            } else {
                persistenceHandler.updateInDB(removedKey, removedValue);
                StartKVServer.logger.info("Updated " + removedKey + " in database");
            }
        }
        queue.add(key);
    }


    @Override
    public void flushCacheToDisk() throws IOException {
        super.flushCacheToDisk();
        queue = new LinkedBlockingQueue<>();
    }

    @Override
    void updateGet(String key) {
    }

    @Override
    void updateDelete(String key) {
        queue.remove(key);
    }

    @Override
    public String toString() {
        return "FIFO";
    }
}
