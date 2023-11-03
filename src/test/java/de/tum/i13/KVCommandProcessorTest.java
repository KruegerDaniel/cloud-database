package de.tum.i13;

import de.tum.i13.KVserver.kv.KVCommandProcessor;
import de.tum.i13.KVserver.kv.KVManager;
import de.tum.i13.shared.KVMessage;
import de.tum.i13.shared.MessagingProtocol;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.mockito.Mockito.*;

public class KVCommandProcessorTest {

    @Test
    public void correctParsingOfPut() throws Exception {
        KVManager kv = mock(KVManager.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv);
        KVMessage response = new KVMessage(MessagingProtocol.StatusType.PUT_SUCCESS, new String[]{"key", "hello"});
        when(kv.put("key", "hello", false)).thenReturn(response);
        kvcp.process("put key hello");
        verify(kv).put("key", "hello", false);
    }

    @Test
    public void correctParsingOfGet() throws Exception {
        KVManager kv = mock(KVManager.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv);
        KVMessage response = new KVMessage(MessagingProtocol.StatusType.GET_SUCCESS, new String[]{"key", "hello"});
        when(kv.get("key")).thenReturn(response);
        kvcp.process("get key");
        verify(kv).get("key");
    }

    @Test
    public void correctParsingOfDelete() throws Exception {
        KVManager kv = mock(KVManager.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv);
        KVMessage response = new KVMessage(MessagingProtocol.StatusType.DELETE_SUCCESS, new String[]{"key"});
        when(kv.delete("key", false)).thenReturn(response);
        kvcp.process("delete key");
        verify(kv).delete("key", false);
    }

    @Test
    public void correctParsingOfKeyRange() {
        KVManager kv = mock(KVManager.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv);
        when(kv.getWriteRanges()).thenReturn(new ArrayList<>());
        kvcp.process("keyrange");
        verify(kv).getWriteRanges();
    }

    @Test
    public void correctParsingOfPutHash() throws Exception {
        KVManager kv = mock(KVManager.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv);
        String hashedKey = "12345678901234567890123456789012";
        KVMessage response = new KVMessage(MessagingProtocol.StatusType.PUT_SUCCESS, new String[]{hashedKey});
        when(kv.put(hashedKey, "value", true)).thenReturn(response);
        kvcp.process("put_hash " + hashedKey + " value");
        verify(kv).put(hashedKey, "value", true);
    }
}
