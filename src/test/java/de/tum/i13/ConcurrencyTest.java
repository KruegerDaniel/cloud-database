package de.tum.i13;

import de.tum.i13.KVStore.KVStore;
import de.tum.i13.KVserver.nio.StartKVServer;
import de.tum.i13.ecs.StartECS;
import de.tum.i13.shared.KVHash;
import de.tum.i13.shared.KVMessage;
import de.tum.i13.shared.MessagingProtocol;
import de.tum.i13.shared.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The focus of this test class is to test the effects of concurrent commands from 2 clients on the database.
 */
public class ConcurrencyTest {

    public static Integer portKV = 5151;
    public static Integer portKV1 = 5152;
    public static Integer portECS = 5452;
    private static int offset = 0; //ignore this

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

        Thread thKV = new Thread(() -> {
            try {
                StartKVServer.main(new String[]{"-p" + portKV.toString(), "-b 127.0.0.1:" + portECS});
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        thKV.start(); // started the KVserver

        Thread thKV1 = new Thread(() -> {
            try {
                StartKVServer.main(new String[]{"-p" + portKV1.toString(), "-b 127.0.0.1:" + portECS});
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        thKV1.start();
        Thread.sleep(2000);
    }

    public String doRequest(Socket s, String req) throws IOException {
        PrintWriter output = new PrintWriter(s.getOutputStream());
        BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));

        output.write(req + "\r\n");
        output.flush();

        return input.readLine();
    }

    /**
     * The test connects 2 clients to our server. The first client inserts the key-value pair. The second client updates
     * the key inserted by the first client with the value eulav. The first client gets his original key and he must
     * receive the new value inserted by the second client, which he does.
     *
     * @throws IOException
     */
    @Test
    public void concurrentInsertionTest1() throws IOException {
        Socket client1 = new Socket();
        client1.connect(new InetSocketAddress("127.0.0.1", portKV));
        doRequest(client1, "");

        Socket client2 = new Socket();
        client2.connect(new InetSocketAddress("127.0.0.1", portKV));
        doRequest(client2, "");

        String keyHash = KVHash.bytesToHex(KVHash.hashKey("key"));
        assertThat(doRequest(client1, "put " + keyHash + " value"), is(equalTo("put_success " + keyHash)));
        assertThat(doRequest(client2, "put " + keyHash + " eulav"), is(equalTo("put_update " + keyHash)));
        assertThat(doRequest(client1, "get " + keyHash), is(equalTo("get_success " + keyHash + " eulav")));
        assertThat(doRequest(client1, "delete " + keyHash), is(equalTo("delete_success " + keyHash)));
    }


    /**
     * The test connects 2 clients to our server. The first client tries to get a nonexistent key from the database, and
     * he receives an error as the key is not in the database yet. The second client inserts the key-value pair in the
     * database. The first client tries again to get his original key. This time he receives a success, as the second
     * client inserted the key at a prior time.
     *
     * @throws IOException
     */
    @Test
    public void concurrentInsertionTest2() throws IOException {
        Socket client1 = new Socket();
        client1.connect(new InetSocketAddress("127.0.0.1", portKV));
        doRequest(client1, "");

        Socket client2 = new Socket();
        client2.connect(new InetSocketAddress("127.0.0.1", portKV));
        doRequest(client2, "");

        String keyHash = KVHash.bytesToHex(KVHash.hashKey("key"));
        assertThat(doRequest(client1, "get " + keyHash), is(equalTo("get_error " + keyHash)));
        assertThat(doRequest(client2, "put " + keyHash + " value"), is(equalTo("put_success " + keyHash)));
        assertThat(doRequest(client1, "get " + keyHash), is(equalTo("get_success " + keyHash + " value")));
        assertThat(doRequest(client1, "delete " + keyHash), is(equalTo("delete_success " + keyHash)));
    }

    @Test
    public void concurrentInsertionTest3() {

        KVStore client1 = new KVStore();
        client1.buildConnection(new Pair<>("127.0.0.1", portKV));

        KVStore client2 = new KVStore();
        client2.buildConnection(new Pair<>("127.0.0.1", portKV1));

        assertEquals(new KVMessage(MessagingProtocol.StatusType.GET_ERROR, new String[]{"key"}).toString(), client1.get("key").toString());
        assertEquals(new KVMessage(MessagingProtocol.StatusType.PUT_SUCCESS, new String[]{"key"}).toString(), client1.put("key", "value").toString());
        assertEquals(new KVMessage(MessagingProtocol.StatusType.PUT_UPDATE, new String[]{"key"}).toString(), client2.put("key", "value").toString());
    }

    /**
     * The test connects 2 clients to our server. The first client inserts the key-value pair. The second client deletes
     * the key and value inserted by the first client. The first client tries to get his inserted pair, but he receives
     * an error, as the second client deleted his entry.
     *
     * @throws IOException
     */
    @Test
    public void concurrentDeletionTest1() throws IOException {
        Socket client1 = new Socket();
        client1.connect(new InetSocketAddress("127.0.0.1", portKV));
        doRequest(client1, "");

        Socket client2 = new Socket();
        client2.connect(new InetSocketAddress("127.0.0.1", portKV));
        doRequest(client2, "");

        String keyHash = KVHash.bytesToHex(KVHash.hashKey("key"));

        assertThat(doRequest(client1, "put " + keyHash + " value"), is(equalTo("put_success " + keyHash)));
        assertThat(doRequest(client2, "delete " + keyHash), is(equalTo("delete_success " + keyHash)));
        assertThat(doRequest(client1, "get " + keyHash), is(equalTo("get_error " + keyHash)));
    }


    /**
     * The test connects 2 clients to our server. The first client inserts the key-value pair. The second client deletes
     * he key and value inserted by the first client. The first client tries to delete his inserted pair, but he receives
     * an error, as the second client already deleted his entry.
     *
     * @throws IOException
     */
    @Test
    public void concurrentDeletionTest2() throws IOException {
        Socket client1 = new Socket();
        client1.connect(new InetSocketAddress("127.0.0.1", portKV));
        doRequest(client1, "");

        Socket client2 = new Socket();
        client2.connect(new InetSocketAddress("127.0.0.1", portKV));
        doRequest(client2, "");

        String keyHash = KVHash.bytesToHex(KVHash.hashKey("key"));

        assertThat(doRequest(client1, "put " + keyHash + " value"), is(equalTo("put_success " + keyHash)));
        assertThat(doRequest(client2, "delete " + keyHash), is(equalTo("delete_success " + keyHash)));
        assertThat(doRequest(client1, "delete " + keyHash), is(equalTo("delete_error " + keyHash)));
    }

    @Test
    public void concurrentDeletionTest3() {

        KVStore client1 = new KVStore();
        client1.buildConnection(new Pair<>("127.0.0.1", portKV));

        KVStore client2 = new KVStore();
        client2.buildConnection(new Pair<>("127.0.0.1", portKV1));

        assertEquals(new KVMessage(MessagingProtocol.StatusType.DELETE_ERROR, new String[]{"deletion3"}).toString(), client1.delete("deletion3").toString());
        assertEquals(new KVMessage(MessagingProtocol.StatusType.PUT_SUCCESS, new String[]{"deletion3"}).toString(), client1.put("deletion3", "value").toString());
        assertEquals(new KVMessage(MessagingProtocol.StatusType.DELETE_SUCCESS, new String[]{"deletion3"}).toString(), client2.delete("deletion3").toString());
    }

    @Test
    public void test20Clients() throws IOException {

        AsyncTester[] clientThreads = new AsyncTester[20];

        for (int i = 0; i < clientThreads.length; i++) {
            clientThreads[i] = new AsyncTester(() -> {
                KVStore client = new KVStore();
                client.buildConnection(new Pair<>("127.0.0.1", portKV));
                offset++;
                String response = client.put("key" + offset, "value").toString().split(" ")[0];


                if (!response.equals("put_success")) {
                    throw new AssertionError("Put key" + offset + " was not successful: " + response);
                }

                client.closeCurrentConnection();
            });

            clientThreads[i].start();
        }


        for (AsyncTester c : clientThreads) {
            try {
                c.test();
            } catch (InterruptedException ignored) {

            }

        }

    }

}
