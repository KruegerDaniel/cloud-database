package de.tum.i13;

import de.tum.i13.KVStore.KVStore;
import de.tum.i13.KVserver.nio.StartKVServer;
import de.tum.i13.ecs.StartECS;
import de.tum.i13.shared.KVHash;
import de.tum.i13.shared.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KVIntegrationTest {

    private static final Random random = new Random();
    public static Integer portKV = 5154;
    public static Integer portECS = 5453;

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
        Thread.sleep(2000);
    }

    public static String doRequest(Socket s, String req) throws IOException {
        PrintWriter output = new PrintWriter(new OutputStreamWriter(
                s.getOutputStream(), StandardCharsets.UTF_8), true);
        BufferedReader input = new BufferedReader(new InputStreamReader(
                s.getInputStream(), StandardCharsets.UTF_8));

        output.write(req + "\r\n");
        output.flush();

        return input.readLine();
    }

    public static String doRequest(String req) throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", portKV));
        String res = doRequest(s, req);
        s.close();

        return res;
    }

    @Test
    public void testWelcomeMessage() throws IOException {
        assertThat(doRequest(""), is(equalTo("Connection to T15 Storage KVserver established: /127.0.0.1:" + portKV)));
    }

    @Test
    public void testPutArbitraryKey() throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", portKV));
        doRequest(s, "");
        String keyHash = KVHash.bytesToHex(KVHash.hashKey("£™¡®´∑ƒ©"));
        assertThat(doRequest(s, "put " + keyHash + " value"), is(equalTo("put_success " + keyHash)));
        assertThat(doRequest(s, "get " + keyHash), is(equalTo("get_success " + keyHash + " value")));
        assertThat(doRequest(s, "delete " + keyHash), is(equalTo("delete_success " + keyHash)));
        assertThat(doRequest(s, "get " + keyHash), is(equalTo("get_error " + keyHash)));
        s.close();
    }

    @Test
    public void testPutArbitraryValue() throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", portKV));
        doRequest(s, "");
        String keyHash = KVHash.bytesToHex(KVHash.hashKey("key"));
        assertThat(doRequest(s, "put " + keyHash + " £ ™¡® ´∑ƒ  ©"), is(equalTo("put_success " + keyHash)));
        assertThat(doRequest(s, "get " + keyHash), is(equalTo("get_success " + keyHash + " £ ™¡® ´∑ƒ  ©")));
        assertThat(doRequest(s, "delete " + keyHash), is(equalTo("delete_success " + keyHash)));
        assertThat(doRequest(s, "get " + keyHash), is(equalTo("get_error " + keyHash)));
        s.close();
    }

    @Test
    public void testInvalidCommand() throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", portKV));
        doRequest(s, "");
        assertThat(doRequest(s, "püt key £ ™¡® ´∑ƒ  ©"), is(equalTo("error Unknown Command")));
        assertThat(doRequest(s, "keyyyyyyyyyy geeeeeeet"), is(equalTo("error Unknown Command")));
        s.close();
    }

    @Test
    public void testDeleteNonExistent() throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", portKV));
        doRequest(s, "");
        String keyHash = KVHash.bytesToHex(KVHash.hashKey("ooga")); // Since we are pumping straight to KVCP,
        // we need to hash the values ourselves
        assertThat(doRequest(s, "put " + keyHash + " booga"), is(equalTo("put_success " + keyHash)));
        assertThat(doRequest(s, "delete " + keyHash), is(equalTo("delete_success " + keyHash)));
        assertThat(doRequest(s, "delete " + keyHash), is(equalTo("delete_error " + keyHash)));
    }

    @Test
    public void testGetNonExistent() throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", portKV));
        doRequest(s, "");
        String keyHash = KVHash.bytesToHex(KVHash.hashKey("ooga"));
        assertThat(doRequest(s, "get " + keyHash + " booga"), is(equalTo("get_error " + keyHash)));

    }

    @Test
    public void testUpdate() throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", portKV));
        doRequest(s, "");
        String keyHash = KVHash.bytesToHex(KVHash.hashKey("key"));
        assertThat(doRequest(s, "put " + keyHash + " booga"), is(equalTo("put_success " + keyHash)));
        assertThat(doRequest(s, "put " + keyHash + " looga"), is(equalTo("put_update " + keyHash)));
        assertThat(doRequest(s, "delete " + keyHash), is(equalTo("delete_success " + keyHash)));
    }

    @Test
    public void testMultilineValue() {
        Map<String, String> pairs = new HashMap<>();

        try {
            extractPairs(pairs);
        } catch (Exception e) {
            return;
        }

        KVStore kvStore = new KVStore();
        kvStore.buildConnection(new Pair<>("127.0.0.1", portKV));

        //Test for put
        for (Map.Entry<String, String> pair : pairs.entrySet()) {
            assertThat(kvStore.put(pair.getKey(), pair.getValue()).toString(), is(equalTo("put_success " + pair.getKey())));
        }
        //Test for get
        for (Map.Entry<String, String> pair : pairs.entrySet()) {
            assertThat(kvStore.get(pair.getKey()).toString(), is(equalTo("get_success " + pair.getKey() + " " + pair.getValue())));
        }
    }

    private void extractPairs(Map<String, String> pairs) throws IOException {
        File file = new File("maildir");
        if (!file.exists()) { // ex pipeline test
            throw new IOException();
        }
        File[] personFolders = file.listFiles(File::isDirectory);
        for (int i = 0; i < 10; i++) {
            //Get random person
            int personFolderIndex = random.nextInt(personFolders.length);
            File[] contentFolders = personFolders[personFolderIndex].listFiles(File::isDirectory);

            //Get random folder from the random person from above
            int contentFolderIndex = random.nextInt(contentFolders.length);
            File[] textFiles = contentFolders[contentFolderIndex].listFiles(File::isFile);

            //Get random file from the random folder from above
            int fileIndex = random.nextInt(textFiles.length);

            //Read the contents of the file and store them as a new value
            String path = textFiles[fileIndex].getPath();
            int pos = processString(path, 2);

            String key = path.substring(0, pos);
            String value = "";
            List<String> lines = Files.readAllLines(Path.of(textFiles[fileIndex].getPath()));
            for (String line : lines) {
                line += "\n";
                value += line;
            }
            //Check to see if the path from the key variable already exists in the map
            if (!pairs.containsKey(key)) {
                pairs.put(key, value);
            }
        }
    }

    public int processString(String path, int nthCharAppearance) {
        path = path.substring(path.indexOf("maildir") + "maildir".length());
        int pos = path.indexOf("/");
        while (--nthCharAppearance > 0 && pos != -1) {
            pos = path.indexOf("/", pos + 1);
        }
        return pos;
    }
}
