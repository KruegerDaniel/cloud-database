package de.tum.i13.KVserver.kv.persistence;

import de.tum.i13.pubSub.Subscriber;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.KVHash;
import de.tum.i13.shared.Pair;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class PersistenceHandler {
    private final String filePath;
    private File dataFile;

    public PersistenceHandler(String filePath, boolean deleteCache) {
        this.filePath = filePath;
        initFile(filePath, deleteCache);
    }

    public Path getFilePath() {
        return Path.of(filePath);
    }

    /**
     * Creates the database file. The file name and extension are inside the filePath parameter.
     *
     * @param filePath The file path in String format of our database file.
     */
    private void initFile(String filePath, boolean deleteCache) {
        try {
            this.dataFile = new File(filePath);
            this.dataFile.getParentFile().mkdirs();
            boolean isFileNotPresent = this.dataFile.createNewFile();
            if (deleteCache && !isFileNotPresent) {
                writeInFile(dataFile, new ArrayList<>());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * The method updates the value of the mentioned key in the persistent database file.
     *
     * @param key   The key for which we have to change the value.
     * @param value The new value for the given key.
     * @throws IOException if we cannot read the contents of the database file, or if we cannot write in the file the changes
     *                     we have made to its content.
     */
    public void updateInDB(String key, String value) throws IOException {
        File database = new File(filePath);
        List<String> dbContent = Files.readAllLines(database.toPath());

        String newKeyValue = key + ": " + value;

        String currentKeyValue = dbContent.stream()
                .filter(kv -> kv.startsWith(key))
                .findFirst()
                .get();
        int updateIndex = dbContent.indexOf(currentKeyValue);
        dbContent.set(updateIndex, newKeyValue);

        writeInFile(database, dbContent);
    }

    /**
     * Reads through the database file and returns in/out of range KVs
     *
     * @param lowerBound
     * @param upperBound
     * @param inRange    triggers whether an inRange or outOfRange is filtered
     * @return KVs in the given range as a list
     * @throws IOException
     */
    public List<Pair<String, String>> getRangeData(String lowerBound, String upperBound, boolean inRange) throws IOException {
        File database = new File(filePath);
        List<String> dbContent = Files.readAllLines(database.toPath());
        List<Pair<String, String>> kvs = new ArrayList<>();
        dbContent.stream()
                .filter(s -> inRange == KVHash.inRange(s.substring(0, s.indexOf(": ")), lowerBound, upperBound))
                .map(s -> new Pair<>(s.substring(0, s.indexOf(": ")), s.substring(s.indexOf(": ") + 2)))
                .forEach(kvs::add);
        return kvs;
    }


    /**
     * The method inserts a new key-value pair in the persistent database file.
     *
     * @param key   The key we want to insert in the database.
     * @param value The value we want to insert in the database, for the given key.
     * @throws IOException if we cannot read the contents of the database file, or if we cannot write in the file the changes
     *                     we have made to its content.
     */
    public void insertInDB(String key, String value) throws IOException {
        File database = new File(filePath);
        List<String> dbContent = Files.readAllLines(database.toPath());

        String newKeyValue;
        newKeyValue = key + ": " + value;
        dbContent.add(newKeyValue);

        writeInFile(database, dbContent);
    }

    /**
     * The method finds the value associated with the given key in the persistent database file.
     *
     * @param key The key for which we want to get the associated value from the database.
     * @return the value associated with the given key.
     * @throws IOException if we cannot read the contents of the database file
     */
    public String getFromDB(String key) throws IOException {
        File database = new File(filePath);
        List<String> dbContent = Files.readAllLines(database.toPath());
        String currentKeyValue = dbContent.stream()
                .filter(kv -> kv.startsWith(key))
                .findFirst()
                .orElse("");

        if (currentKeyValue.isEmpty()) {
            return null;
        } else {
            int separatorIndex = currentKeyValue.indexOf(": ");
            return currentKeyValue.substring(separatorIndex + 2);
        }
    }

    /**
     * The method deletes the given key and its associated value from the persistent database file.
     *
     * @param key The key we want to delete from the database.
     * @throws IOException if we cannot read the contents of the database file, or if we cannot write in the file the changes
     *                     we have made to its content.
     */
    public String deleteFromDB(String key) throws IOException {
        File database = new File(filePath);
        List<String> dbContent = Files.readAllLines(database.toPath());
        String currentKeyValue = dbContent.stream()
                .filter(s -> s.startsWith(key))
                .findFirst()
                .orElse("");

        if (currentKeyValue.isEmpty()) {
            return null;
        }
        dbContent.remove(currentKeyValue);

        writeInFile(database, dbContent);
        return key;
    }

    public void addSubscriber(String key, Subscriber subscriber) throws IOException {
        File database = new File(filePath);
        List<String> dbContent = Files.readAllLines(database.toPath());

        String subscriberString = subscriber.toString();
        String keyLine = dbContent.stream()
                .filter(persistentKey -> persistentKey.startsWith(key))
                .findFirst().orElse("");
        if (keyLine.isEmpty()) {
            dbContent.add(key + ": " + subscriberString + ", ");
        } else if (keyLine.contains(subscriber.toString())) {
            return;
        } else {
            int keyLineIndex = dbContent.indexOf(keyLine);
            keyLine += subscriberString + ", ";
            dbContent.set(keyLineIndex, keyLine);
        }

        writeInFile(database, dbContent);
    }

    //Doesn't work if we use as sid: "sid"
    public String removeSubscriber(String key, String sid) throws IOException {
        File database = new File(filePath);
        List<String> dbContent = Files.readAllLines(database.toPath());

        if (dbContent.isEmpty()) {
            return "";
        }

        String keyLine = dbContent.stream()
                .filter(persistentKey -> persistentKey.startsWith(key))
                .findFirst().get();
        int keyLineIndex = dbContent.indexOf(keyLine);

        int sidIndex = keyLine.indexOf(sid);
        String subscriberLowerHalf = keyLine.substring(sidIndex - 5, sidIndex);
        String subscriberUpperHalf = keyLine.substring(sidIndex).substring(0, keyLine.substring(sidIndex).indexOf("},") + 3);
        String subscriberString = subscriberLowerHalf + subscriberUpperHalf;

        keyLine = keyLine.replace(subscriberString, "");

        dbContent.set(keyLineIndex, keyLine);
        writeInFile(database, dbContent);

        return subscriberString;
    }

    //Doesn't work if we use as sid: "sid"
    public String getSubscriber(String key, String sid) throws IOException {
        File database = new File(filePath);
        List<String> dbContent = Files.readAllLines(database.toPath());

        if (dbContent.isEmpty()) {
            return "";
        }

        String keyLine = dbContent.stream()
                .filter(persistentKey -> persistentKey.startsWith(key))
                .findFirst().get();

        int sidIndex = keyLine.indexOf(sid);
        String subscriberLowerHalf = keyLine.substring(sidIndex - "{sid=".length(), sidIndex);
        String subscriberUpperHalf = keyLine.substring(sidIndex).substring(0, keyLine.substring(sidIndex).indexOf("}") + 1);
        String subscriberString = subscriberLowerHalf + subscriberUpperHalf;

        return subscriberString;
    }

    /**
     * The method writes in the passed file parameter the contents of the list parameter.
     *
     * @param database  The file we want to write in.
     * @param dbContent The content that we want to write in the file.
     * @throws IOException if we cannot create our writer from the file.
     */
    private void writeInFile(File database, List<String> dbContent) throws IOException {
        BufferedWriter databaseWriter = Files.newBufferedWriter(database.toPath(), Charset.forName(Constants.TELNET_ENCODING));
        for (String s : dbContent) {
            databaseWriter.write(s + "\r");
        }
        databaseWriter.flush();
    }

    public void deleteAllData() {
        try {
            writeInFile(new File(this.filePath), new ArrayList<>());
        } catch (IOException e) {
            System.out.println("Couldn't delete database contents on shutdown.");
        }
    }
}
