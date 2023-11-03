package de.tum.i13.client;

import de.tum.i13.KVStore.KVStore;
import de.tum.i13.shared.KVMessage;
import de.tum.i13.shared.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import static de.tum.i13.shared.LogSetup.setupLogging;


public class KVClient {
    public final static Logger LOGGER = Logger.getLogger(KVClient.class.getName());

    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        Path path = Paths.get("client.log");
        LOGGER.setLevel(setupLogging(path));

        KVStore kvStore = new KVStore();
        printHelp();

        KVMessage.StatusType command;
        String data;
        for (; ; ) {
            System.out.print("T15Client> ");
            String line = reader.readLine();
            String[] body = line.split(" ");
            switch (body[0]) {
                case "connect" -> {
                    LOGGER.info("The client tries to connect to the server.");
                    printShellLine(kvStore.buildConnection(new Pair<>(body[1], Integer.parseInt(body[2]))));
                }
                case "put" -> {
                    if (body.length < 3) {
                        printShellLine("Method args incorrect");
                        LOGGER.warning("User provided wrong arguments for put");
                        break;
                    }
                    String[] value = new String[body.length - 2]; // everything from index 2 onward
                    System.arraycopy(body, 2, value, 0, value.length);
                    data = convertStrArrayToStr(value, " ");
                    LOGGER.info("User calls put " + body[1]);
                    printShellLine(kvStore.put(body[1], data).toString()); // put key and data
                }
                case "get" -> {
                    if (body.length > 2) {
                        printShellLine("Method args incorrect");
                        LOGGER.warning("User provided wrong arguments for get");
                        break;
                    }
                    LOGGER.info("User calls get " + body[1]);
                    printShellLine(kvStore.get(body[1]).toString());
                }
                case "delete" -> {
                    if (body.length != 2) {
                        printShellLine("Method args incorrect");
                        LOGGER.warning("User provided wrong arguments for delete");
                        break;
                    }
                    LOGGER.info("User calls delete " + body[1]);
                    printShellLine(kvStore.delete(body[1]).toString());
                }
                case "publish" -> {
                    if (body.length < 3) {
                        LOGGER.warning("User provides wrong arguments for subscribe");
                        printShellLine("Method args incorrect");
                        break;
                    }
                    String[] value = new String[body.length - 2]; // everything from index 2 onward
                    System.arraycopy(body, 2, value, 0, value.length);
                    data = convertStrArrayToStr(value, " ");
                    LOGGER.info("The client subscribes to " + body[1]);
                    printShellLine(kvStore.publish(body[1], data).toString());
                }
                case "subscribe" -> {
                    if (body.length != 4) {
                        LOGGER.warning("User provides wrong arguments for subscribe");
                        printShellLine("Method args incorrect");
                        break;
                    }
                    LOGGER.info("The client subscribes to " + body[1]);
                    printShellLine(kvStore.subscribe(body[1], body[2], body[3]).toString());
                }
                case "unsubscribe" -> {
                    if (body.length != 3) {
                        LOGGER.warning("User provides wrong arguments for subscribe");
                        printShellLine("Method args incorrect");
                        break;
                    }
                    LOGGER.info("The client unsubscribes from " + body[1]);
                    printShellLine(kvStore.unsubscribe(body[1], body[2]).toString());
                }
                case "logLevel" -> {
                    LOGGER.info("The client tries set a new log level for the logger.");
                    if (body.length != 2) {
                        LOGGER.warning("User provided the wrong number of logLevel arguments");
                        printShellLine("Invalid use of logLevel: logLevel {ALL | SEVERE | WARNING | INFO | CONFIG | FINE | FINER | FINEST | OFF}");
                    }
                    setLogLevel(body[1]);
                }
                case "disconnect" -> {
                    LOGGER.info("The client tries to disconnect from the server.");
                    try {
                        kvStore.closeCurrentConnection();
                    } catch (Exception e) {
                        LOGGER.severe("Error while disconnecting from server");
                    }
                }
                case "help" -> {
                    LOGGER.info("User calls help");
                    printHelp();
                }
                case "quit" -> {
                    LOGGER.info("The client will now quit the program.");
                    printShellLine("Application exit!");
                    return;
                }
                default -> {
                    LOGGER.info("The client passed an unknown command.");
                    printShellLine("Unknown command");
                }
            }
        }
    }

    private static void printHelp() {
        System.out.println("Available commands:");
        System.out.println("\tconnect <address> <port> - Tries to establish a TCP- connection to the echo KVserver based on the given KVserver address and the port number of the echo service.");
        System.out.println("\tdisconnect - Tries to disconnect from the connected KVserver.");
        System.out.println("\tput <key> <value> - Inserts or updates a Key/Value pair in the storage server.");
        System.out.println("\tget <key> - Retrieves a Key/Value pair from the database.");
        System.out.println("\tdelete <key> - Removes a Key/Value pair from the storage server.");
        System.out.println("\tpublish <key> <value> - Publish a Key/value pair. This will notify all subscribers of <key>");
        System.out.println("\tsubscribe <SID> <key> - Subscribe to a key. Any new changes to <key> will be printed");
        System.out.println("\tunsubscribe <SID> <key> - Unsubscribe from a key. Notifications will no longer be received");
        System.out.println("\tlogLevel <level> - Sets the logger to the specified log level (ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF)");
        System.out.println("\thelp - Display this help");
        System.out.println("\tquit - Tears down the active connection to the KVserver and exits the program execution.");
    }

    private static void printShellLine(String msg) {
        System.out.println("T15Client> " + msg);
    }

    private static void setLogLevel(String level) {
        try {
            Level newLevel = Level.parse(level);
            LOGGER.info("Setting LOGGER.level to " + newLevel.getName());
            printShellLine("LogLevel set from " + LOGGER.getLevel() + " to " + newLevel.getName());
            LOGGER.setLevel(newLevel);
        } catch (IllegalArgumentException e) {
            LOGGER.warning("User entered an invalid logLevel");
            printShellLine("Not a valid loglevel. Setting logLevel failed");
        }
    }

    public static String convertStrArrayToStr(String[] words, String separator) {
        if (words.length == 0) {
            return "";
        }
        if (words.length == 1) {
            return words[0];
        }
        StringBuilder str = new StringBuilder(words[0]);
        for (int i = 1; i < words.length; i++) {
            str.append(separator).append(words[i]);
        }
        return str.toString();
    }

}
