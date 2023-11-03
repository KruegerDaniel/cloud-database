package de.tum.i13;

import de.tum.i13.KVserver.kv.persistence.PersistenceHandler;
import de.tum.i13.pubSub.Subscriber;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PersistenceTest {

    public static PersistenceHandler handler;

    @BeforeAll
    static void createNewDatabase() {
        handler = new PersistenceHandler("testDB/databasePersistenceTest.txt", true);
    }

    @Test
    public void putPersistenceTest() throws IOException {
        handler.insertInDB("foo", "bar");
        List<String> dbContent = Files.readAllLines(Paths.get("testDB/databasePersistenceTest.txt"));
        for (String pair : dbContent) {
            assertEquals("foo: bar", pair);
        }
    }

    @Test
    public void getPersistenceTest() throws IOException {
        FileWriter writer = new FileWriter(new File("testDB/databasePersistenceTest.txt"));
        writer.write("foo: bar");
        writer.flush();
        assertEquals("foo: bar", "foo: " + handler.getFromDB("foo"));
    }

    @Test
    public void updatePersistenceTest() throws IOException {
        FileWriter writer = new FileWriter(new File("testDB/databasePersistenceTest.txt"));
        writer.write("foo: bar");
        writer.flush();
        handler.updateInDB("foo", "barbar");
        List<String> dbContent = Files.readAllLines(Paths.get("testDB/databasePersistenceTest.txt"));
        for (String pair : dbContent) {
            assertEquals("foo: barbar", pair);
        }
    }

    @Test
    public void deletePersistenceTest() throws IOException {
        FileWriter writer = new FileWriter(new File("testDB/databasePersistenceTest.txt"));
        writer.write("foo: bar");
        writer.flush();
        handler.deleteFromDB("foo");
        List<String> dbContent = Files.readAllLines(Paths.get("testDB/databasePersistenceTest.txt"));
        for (String pair : dbContent) {
            assertEquals("", pair);
        }
    }

    @Test
    public void addSubscriptionTest() throws IOException {
        String testSid = "sid";
        String testAddress = "127.0.0.1";
        String testPort = "5700";
        Subscriber testSubscriber = new Subscriber(testSid, testAddress, testPort);

        String testKey = "key";

        String expectedLine = testKey + ": " + testSubscriber.toString() + ", ";

        handler.addSubscriber(testKey, testSubscriber);

        List<String> dbContent = Files.readAllLines(Paths.get("testDB/databasePersistenceTest.txt"));
        for (String actualLine : dbContent) {
            assertEquals(expectedLine, actualLine);
        }
    }

    @Test
    public void removeSubscriptionTest() throws IOException {
        String testSid1 = "sid1";
        String testAddress = "127.0.0.1";
        String testPort = "5701";
        Subscriber testSubscriber1 = new Subscriber(testSid1, testAddress, testPort);

        String testSid2 = "sid2";
        String testPort2 = "5702";
        Subscriber testSubscriber2 = new Subscriber(testSid2, testAddress, testPort2);

        String testKey = "key";

        FileWriter writer = new FileWriter("testDB/databasePersistenceTest.txt");
        writer.write(testKey + ": " + testSubscriber1.toString() + ", " + testSubscriber2.toString() + ", ");
        writer.flush();

        String expectedLine = testKey + ": " + testSubscriber2.toString() + ", ";

        handler.removeSubscriber(testKey, testSid1);

        List<String> dbContent = Files.readAllLines(Paths.get("testDB/databasePersistenceTest.txt"));
        for (String actualLine : dbContent) {
            assertEquals(expectedLine, actualLine);
        }
    }

    @AfterEach
    public void testDatabaseBreakdown() throws IOException {
        FileWriter writer = new FileWriter(new File("testDB/databasePersistenceTest.txt"));
        writer.write("");
        writer.flush();
    }
}
