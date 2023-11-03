package de.tum.i13;

import de.tum.i13.KVStore.ServerToServerConnection;
import de.tum.i13.KVserver.nio.SimpleNioServer;
import de.tum.i13.KVserver.nio.StartKVServer;
import de.tum.i13.ecs.ECSCommandProcessor;
import de.tum.i13.ecs.ECSManager;
import de.tum.i13.shared.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.reflection.FieldSetter;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ECSTest {

    public static Map<Pair<String, Integer>, ServerToServerConnection> connectionMap;
    public static Integer portKV = 5160;
    public static Integer portECS = 5462;
    public static ECSManager ecsManager = new ECSManager("data");

    @BeforeAll
    public static void serverSetup() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        Thread thECS = new Thread(() -> {
            try {
                ECSCommandProcessor ecsCommandProcessor = new ECSCommandProcessor("data");
                try {
                    FieldSetter.setField(ecsCommandProcessor, ecsCommandProcessor.getClass().getDeclaredField("ecsManager"), ecsManager);
                } catch (NoSuchFieldException e) {
                    e.printStackTrace();
                }
                SimpleNioServer sn = new SimpleNioServer(ecsCommandProcessor);
                sn.bindSockets("127.0.0.1", portECS);
                sn.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        thECS.start(); // started the ECS
        Field privateMap = ECSManager.class.getDeclaredField("connectionMap");
        privateMap.setAccessible(true);
        connectionMap = (Map<Pair<String, Integer>, ServerToServerConnection>) privateMap.get(ecsManager);

        Thread.sleep(2000);
    }

    @Test
    public void registerConcurrently() throws InterruptedException {
        AsyncTester thKV = new AsyncTester(() -> {
            try {
                StartKVServer.main(new String[]{"-p" + portKV.toString(), "-b 127.0.0.1:" + portECS});
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        AsyncTester thKV1 = new AsyncTester(() -> {
            try {
                StartKVServer.main(new String[]{"-p" + (portKV + 1), "-b 127.0.0.1:" + portECS});
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        thKV.start();
        thKV1.start();
        Thread.sleep(1000);

        //check if both servers are now registered in ECSManager
        assertEquals(2, connectionMap.size());
    }

    @Test
    public void testDisconnect() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        //check if both servers are now registered in ECSManager
        Method privateMethod = StartKVServer.class.getDeclaredMethod("deregisterFromECS", InetSocketAddress.class, String.class, int.class);
        privateMethod.setAccessible(true);
        privateMethod.invoke(null, new InetSocketAddress("127.0.0.1", portECS), "127.0.0.1", portKV);
        privateMethod.invoke(null, new InetSocketAddress("127.0.0.1", portECS), "127.0.0.1", portKV + 1);
        assertEquals(0, connectionMap.size(), "The deregister did not work");
    }


}
