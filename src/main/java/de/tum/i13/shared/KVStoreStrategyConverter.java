package de.tum.i13.shared;

import de.tum.i13.KVserver.kv.KVManager;
import de.tum.i13.KVserver.kv.KVManagerFIFO;
import de.tum.i13.KVserver.kv.KVManagerLFU;
import de.tum.i13.KVserver.kv.KVManagerLRU;
import picocli.CommandLine;

public class KVStoreStrategyConverter implements CommandLine.ITypeConverter<KVManager> {
    @Override
    public KVManager convert(String s) throws Exception {
        switch (s.toUpperCase().trim()) {
            case "LRU":
                return new KVManagerLRU(0);
            case "LFU":
                return new KVManagerLFU(0);
            default:
                return new KVManagerFIFO(0);
        }
    }
}
