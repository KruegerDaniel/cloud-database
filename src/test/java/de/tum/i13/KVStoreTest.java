package de.tum.i13;


import de.tum.i13.KVStore.KVStore;
import de.tum.i13.KVStore.ListenerThread;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

public class KVStoreTest {
    private KVStore sut;

    /*
    @Test
    public void testSubscribe() {
        sut = new KVStore();

        new AsyncTester(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(6088);
                Socket socket = serverSocket.accept();
                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                if (!br.readLine().equals("subscribe "))
                    throw new AssertionError("Subscribe message was incorrect");
                br.close();
                socket.close();
                serverSocket.close();
            } catch (IOException e) {
            }
        }).start();
        sut.buildConnection(new Pair<>("localhost", 6088));
        sut.subscribe("poptart");
    }
*/

    @Test
    public void testNotifyListenerThread() throws IOException, InterruptedException {
        ListenerThread listener = new ListenerThread();
        listener.start();

        Thread.sleep(100);
        Socket notify = new Socket();
        notify.connect(new InetSocketAddress("localhost", listener.getPort()));
        PrintWriter output = new PrintWriter(notify.getOutputStream());
        output.println("notify poptarts");
        output.flush();
        listener.endThread();
        notify.close();
    }
}
