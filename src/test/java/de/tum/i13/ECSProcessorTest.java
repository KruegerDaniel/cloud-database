package de.tum.i13;

import de.tum.i13.KVserver.nio.SimpleNioServer;
import de.tum.i13.ecs.ECSCommandProcessor;
import de.tum.i13.ecs.ECSManager;
import de.tum.i13.shared.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.reflection.FieldSetter;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.*;

public class ECSProcessorTest {

    public static Integer portECS = 5454;
    public static ECSManager ecsMock = mock(ECSManager.class);

    @BeforeAll
    public static void serverSetup() throws InterruptedException {
        Thread thECS = new Thread(() -> {
            try {
                ECSCommandProcessor ecsCommandProcessor = new ECSCommandProcessor("data");

                try {
                    FieldSetter.setField(ecsCommandProcessor, ecsCommandProcessor.getClass().getDeclaredField("ecsManager"), ecsMock);
                } catch (NoSuchFieldException e) {

                }
                SimpleNioServer sn = new SimpleNioServer(ecsCommandProcessor);
                sn.bindSockets("127.0.0.1", portECS);
                sn.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        thECS.start(); // started the ECS
        Thread.sleep(2000);
    }

    public String doRequest(Socket s, String req) throws IOException {
        PrintWriter output = new PrintWriter(new OutputStreamWriter(
                s.getOutputStream(), StandardCharsets.UTF_8), true);
        BufferedReader input = new BufferedReader(new InputStreamReader(
                s.getInputStream(), StandardCharsets.UTF_8));

        output.write(req + "\r\n");
        output.flush();

        return input.readLine();
    }

    @Test
    public void testRegister() throws IOException {
        Socket client = new Socket();
        client.connect(new InetSocketAddress("127.0.0.1", portECS));
        doRequest(client, "");
        doRequest(client, "register 127.0.0.1 " + client.getLocalPort());
        verify(ecsMock).register("127.0.0.1", "" + client.getLocalPort());
    }

    @Test
    public void testDeregister() throws IOException {
        Socket client = new Socket();
        client.connect(new InetSocketAddress("127.0.0.1", portECS));
        String clientPort = String.valueOf(client.getLocalPort());
        when(ecsMock.removeServer("127.0.0.1", clientPort)).thenReturn(new Pair<>("127.0.0.1", client.getLocalPort()));

        doRequest(client, "");
        doRequest(client, "deregister 127.0.0.1 " + clientPort);

        verify(ecsMock).removeServer("127.0.0.1", clientPort);
    }

    @Test
    public void testMultipleRegisters() throws IOException {
        Socket client = new Socket();
        client.connect(new InetSocketAddress("127.0.0.1", portECS));
        Socket client1 = new Socket();
        client1.connect(new InetSocketAddress("127.0.0.1", portECS));

        doRequest(client, "");
        doRequest(client1, "");
        doRequest(client, "register 127.0.0.1 " + client.getLocalPort());
        doRequest(client1, "register 127.0.0.1 " + client1.getLocalPort());

        verify(ecsMock).register("127.0.0.1", "" + client.getLocalPort());
        verify(ecsMock).register("127.0.0.1", "" + client1.getLocalPort());
    }

    @Test
    public void testMultipleDeregisters() throws IOException {
        Socket client = new Socket();
        client.connect(new InetSocketAddress("127.0.0.1", portECS));
        Socket client1 = new Socket();
        client1.connect(new InetSocketAddress("127.0.0.1", portECS));
        String clientPort = String.valueOf(client.getLocalPort());
        String client1Port = String.valueOf(client1.getLocalPort());

        when(ecsMock.removeServer("127.0.0.1", clientPort)).thenReturn(new Pair<>("127.0.0.1", client.getLocalPort()));
        when(ecsMock.removeServer("127.0.0.1", client1Port)).thenReturn(new Pair<>("127.0.0.1", client1.getLocalPort()));

        doRequest(client, "");
        doRequest(client1, "");
        doRequest(client, "deregister 127.0.0.1 " + client.getLocalPort());
        doRequest(client1, "deregister 127.0.0.1 " + client1.getLocalPort());

        verify(ecsMock).removeServer("127.0.0.1", clientPort);
        verify(ecsMock).removeServer("127.0.0.1", client1Port);
    }
}
