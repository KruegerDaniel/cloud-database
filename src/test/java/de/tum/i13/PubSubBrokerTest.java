package de.tum.i13;

import de.tum.i13.KVStore.KVStore;
import de.tum.i13.KVserver.nio.StartKVServer;
import de.tum.i13.ecs.StartECS;
import de.tum.i13.pubSub.PubSubBroker;
import de.tum.i13.pubSub.Subscriber;
import de.tum.i13.shared.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.lang.reflect.Field;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PubSubBrokerTest {

    private static final Integer portECS = 5752;
    private static final Integer portKV = 5171;

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

    @Test
    public void subscriptionTest() {
        KVStore subscriber = new KVStore();

        String sid = "user";
        String interestedKey = "key";

        subscriber.buildConnection(new Pair<>("127.0.0.1", portKV));

        assertThat(subscriber.subscribe(sid, interestedKey, "7457").toString(), is(equalTo("subscribe_success " + sid + " " + interestedKey)));

    }

    @Test
    public void unsubscriptionTest() throws NoSuchFieldException, IllegalAccessException {
        PubSubBroker broker = new PubSubBroker("pubSubTest");
        String interestedKey = "key";

        String sid = "sid";
        String port = "5700";
        String address = "localhost";
        Subscriber subscriber = new Subscriber(sid, address, port);

        Map<String, List<Subscriber>> initialSubscriberMap = new HashMap<>();
        initialSubscriberMap.put(interestedKey, new ArrayList(Collections.singleton(subscriber)));

        Class<PubSubBroker> pubSubClass = PubSubBroker.class;
        Field field = pubSubClass.getDeclaredField("subscriberMap");
        field.setAccessible(true);
        field.set(broker, initialSubscriberMap);

        broker.removeSubscriber(interestedKey, subscriber.getSid());

        Map<String, List<Subscriber>> actualSubscriberMap = (Map<String, List<Subscriber>>) field.get(broker);

        initialSubscriberMap.get(interestedKey).remove(subscriber);
        Map<String, List<Subscriber>> expectedSubscriberMap = initialSubscriberMap;

        assertEquals(expectedSubscriberMap, actualSubscriberMap);
    }

    @Test
    public void nonexistentUnsubscriptionTest() throws NoSuchFieldException, IllegalAccessException {
        PubSubBroker broker = new PubSubBroker("pubSubTest");
        String interestedKey = "key";

        String sid = "sid";
        String port = "5700";
        String address = "localhost";
        Subscriber subscriber = new Subscriber(sid, address, port);

        Map<String, List<Subscriber>> expectedSubscriberMap = new HashMap<>();

        broker.removeSubscriber(interestedKey, subscriber.getSid());

        Class<PubSubBroker> pubSubClass = PubSubBroker.class;
        Field field = pubSubClass.getDeclaredField("subscriberMap");
        field.setAccessible(true);

        Map<String, List<Subscriber>> actualSubscriberMap = (Map<String, List<Subscriber>>) field.get(broker);

        assertEquals(expectedSubscriberMap, actualSubscriberMap);
    }

    @Test
    public void publicationTest() {
        KVStore publisher = new KVStore();

        String interestedKey = "key";
        String publishedValue = "value";

        publisher.buildConnection(new Pair<>("127.0.0.1", portKV));

        assertThat(publisher.publish(interestedKey, publishedValue).toString(), is(equalTo("publication_success " + interestedKey + " " + publishedValue)));
    }

    @Test
    public void doubleSubscriptionTest() throws NoSuchFieldException, IllegalAccessException {
        PubSubBroker broker = new PubSubBroker("pubSubTest");
        String interestedKey = "key";

        String sid = "sid";
        String port = "5700";
        String address = "localhost";
        Subscriber subscriber = new Subscriber(sid, address, port);

        Map<String, List<Subscriber>> expectedSubscriberMap = new HashMap<>();
        expectedSubscriberMap.put(interestedKey, new ArrayList<>(Collections.singleton(subscriber)));

        broker.addSubscriber(interestedKey, subscriber);
        broker.addSubscriber(interestedKey, subscriber);

        Class<PubSubBroker> pubSubClass = PubSubBroker.class;
        Field field = pubSubClass.getDeclaredField("subscriberMap");
        field.setAccessible(true);

        Map<String, List<Subscriber>> actualSubscriberMap = (Map<String, List<Subscriber>>) field.get(broker);

        assertEquals(expectedSubscriberMap, actualSubscriberMap);
    }

//	@Test
//	public void notificationTest() throws IOException {
//		KVStore publisher = new KVStore();
//		KVStore subscriber = new KVStore();
//
//		String sid = "user";
//		String interestedKey = "key";
//		String publishedValue = "value";
//
//
//		subscriber.buildConnection(new Pair<>("127.0.0.1", portKV));
//		publisher.buildConnection(new Pair<>("127.0.0.1", portKV));
//
//		assertThat(subscriber.subscribe(sid, interestedKey, "7458").toString(), is(equalTo("subscribe_success " + sid + " " + interestedKey)));
//		assertThat(publisher.publish(interestedKey, publishedValue).toString(), is(equalTo("publication_success " + interestedKey + " " + publishedValue)));
//	}
}
