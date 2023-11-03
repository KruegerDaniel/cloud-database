package de.tum.i13.KVserver.kv;

import de.tum.i13.KVserver.nio.StartKVServer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * KVManagerLFU implements the LFU strategy for the cache
 */
public class KVManagerLFU extends KVManager {
    public Map<String, Integer> frequecyMap;

    public KVManagerLFU(int maxSize) {
        super(maxSize);
        frequecyMap = new HashMap<>();
    }

    void updatePut(String key) throws IOException {
        if (cache.containsKey(key)) {
            int frequency = frequecyMap.get(key);
            frequecyMap.replace(key, frequency + 1);
            return;
        }
        if (cache.size() == maxSize) {
            Map.Entry<String, Integer> minFreq = null;
            for (Map.Entry<String, Integer> i : frequecyMap.entrySet()) {
                if (minFreq == null || i.getValue() < minFreq.getValue()) {
                    minFreq = i;
                }
            }
            String removedKey = minFreq.getKey();
            String removedValue = cache.remove(removedKey);
            frequecyMap.remove(removedKey);
            if (persistenceHandler.getFromDB(removedKey) == null) {
                persistenceHandler.insertInDB(removedKey, removedValue);
                StartKVServer.logger.info("Wrote " + removedKey + ": " + removedValue + " into database");
            } else {
                persistenceHandler.updateInDB(removedKey, removedValue);
                StartKVServer.logger.info("Updated " + removedKey + " in database");
            }
        }
        frequecyMap.put(key, 1);
    }

    @Override
    public void flushCacheToDisk() throws IOException {
        super.flushCacheToDisk();
        frequecyMap = new HashMap<>();
    }

    @Override
    void updateGet(String key) {
        if (cache.containsKey(key)) {
            frequecyMap.replace(key, frequecyMap.get(key) + 1);
        }
    }

    @Override
    void updateDelete(String key) {
        frequecyMap.remove(key);
    }

    @Override
    public String toString() {
        return "LFU";
    }
}
