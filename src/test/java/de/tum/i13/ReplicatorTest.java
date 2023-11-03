package de.tum.i13;

import de.tum.i13.KVStore.KVStore;
import de.tum.i13.KVserver.nio.StartKVServer;
import de.tum.i13.ecs.StartECS;
import de.tum.i13.shared.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReplicatorTest {
    public static Integer portECS = 5650;
    public static Integer[] ports = {5651, 5652, 5653};
    public static Socket[] sockets;

    @BeforeAll
    public static void serverSetup() throws InterruptedException {
        Thread thECS = new Thread(() -> {
            try {
                StartECS.main(new String[]{"-p", portECS.toString()});
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        thECS.start(); // started the ECS
        Thread.sleep(2000);

        for (Integer port : ports) {
            new Thread(() -> {
                try {
                    StartKVServer.main(new String[]{"-p" + port.toString(), "-b 127.0.0.1:" + portECS});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
            Thread.sleep(2000);
        }

        sockets = new Socket[ports.length];
        for (int i = 0; i < ports.length; i++) {
            sockets[i] = new Socket();
            try {
                sockets[i].connect(new InetSocketAddress("localhost", ports[i]));
                doRequest(sockets[i], ""); //read the connection established string
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static String doRequest(Socket s, String req) throws IOException {
        PrintWriter output = new PrintWriter(s.getOutputStream());
        BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));

        output.write(req + "\r\n");
        output.flush();

        return input.readLine();
    }

    @Test
    public void putAndGetFromReplicator() throws IOException {
        KVStore client = new KVStore();
        client.buildConnection(new Pair<>("localhost", ports[0]));
        client.put("hello", "world");

        for (Socket s : sockets) {
            assertEquals("get_success hello world", doRequest(s, "get hello"));
        }
    }

    @Test
    public void updateAndGetFromReplicator() throws IOException {
        KVStore client = new KVStore();
        client.buildConnection(new Pair<>("localhost", ports[0]));
        client.put("hello", "world");
        client.put("hello", "jupiter");

        for (Socket s : sockets) {
            assertEquals("get_success hello jupiter", doRequest(s, "get hello"));
        }
    }

    @Test
    public void deleteAndGetFromReplicator() throws IOException {
        KVStore client = new KVStore();
        client.buildConnection(new Pair<>("localhost", ports[0]));
        client.put("key", "value");
        client.delete("key");

        for (Socket s : sockets) {
            assertEquals("get_error key", doRequest(s, "get key"));
        }
    }

    // @Test
    // public void checkReplicationOnServerStart() {
    //
    // }
//
    // @Test
    // public void checkReplicationOfUngracefulShutdown() throws InterruptedException, IOException {
    //     //start temporary server with port 5154
    //     Thread tempServer = new Thread(() -> {
    //         try {
    //             StartKVServer.main(new String[]{"-p" + "5154", "-b 127.0.0.1:" + portECS});
    //         } catch (IOException e) {
    //             e.printStackTrace();
    //         }
    //     });
    //     tempServer.start();
    //     Thread.sleep(1000);
//
    //     //insert a-z as keys into KVservers
    //     KVStore input = new KVStore();
    //     input.buildConnection(new Pair<>("localhost", ports[0]));
    //     int start = 'a';
    //     for (int i = 0; i < 26; i++) {
    //         input.put("" + (char) (start + i), "" + i);
    //     }
//
    //     //shutdown temp server
    //     Socket shutdown = new Socket();
    //     shutdown.connect(new InetSocketAddress("localhost", 5154));
    //     doRequest(shutdown, "shutdown");
//
    //     //check that the 3 servers have all the values
    //     for (Socket s : sockets) {
    //         for (int i = 0; i < 26; i++) {
    //             assertEquals("get_success " + (char) (start + i) + " " + i, doRequest(s, "get " + (char) (start + i)));
    //         }
    //     }
    // }
//
}
