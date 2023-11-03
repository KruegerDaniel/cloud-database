package de.tum.i13.KVserver.kv;

import de.tum.i13.KVserver.nio.StartKVServer;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * KVManagerLRU implements the LRU strategy for the cache
 */
public class KVManagerLRU extends KVManager {
    public Queue<String> queue;

    public KVManagerLRU(int maxSize) {
        super(maxSize);
        queue = new LinkedBlockingQueue<>();
    }

    void updatePut(String key) throws IOException {
        if (queue.contains(key)) {
            queue.remove(key);
        } else if (queue.size() == maxSize) {
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
        if (queue.contains(key)) {
            queue.remove(key);
            queue.add(key);
        }
    }

    @Override
    void updateDelete(String key) {
        queue.remove(key);
    }

    @Override
    public String toString() {
        return "LRU";
    }
}
