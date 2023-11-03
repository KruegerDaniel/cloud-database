package de.tum.i13.KVserver.nio;

import de.tum.i13.KVStore.ServerToServerConnection;
import de.tum.i13.KVserver.kv.KVCommandProcessor;
import de.tum.i13.KVserver.kv.KVManager;
import de.tum.i13.KVserver.kv.persistence.PersistenceHandler;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.Pair;
import de.tum.i13.shared.Range;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

public class StartKVServer {

    public static Logger logger = Logger.getLogger(StartKVServer.class.getName());

    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);
        setupLogging(cfg.logfile);
        logger.setLevel(cfg.logLevel);
        logger.info("Config: " + cfg.toString());
        logger.info("starting KVserver");

        PersistenceHandler persistenceHandler = new PersistenceHandler(cfg.dataDir + "/database.txt", true);

        KVManager kvManager = cfg.cacheStrategy; // getFromDB strategy read from CL
        kvManager.setPersistenceHandler(persistenceHandler);
        kvManager.setMaxSize(cfg.cacheSize); // set size to value read from CLA
        kvManager.setLocalWriteRange(new Range(cfg.listenaddr, cfg.port, null, null));
        kvManager.setEcs(cfg.bootstrap);
        kvManager.setRetentionTime(cfg.retentionPeriod);

        CommandProcessor kvProcessor = new KVCommandProcessor(kvManager);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kvManager.setLocalWriteRange(new Range(cfg.listenaddr, cfg.port, null, null)); // Basically removing the range

            // Send deregistration message to ECS
            String ecsReply = deregisterFromECS(cfg.bootstrap, cfg.listenaddr, cfg.port);
            // Deregistration might not have been successful due to ECS having a problem
            if (ecsReply != null) {
                logger.info("Deregistration successful. Initiate key transfer");
                kvProcessor.process(ecsReply);
            }
            // Clear out persistent database anyway
            persistenceHandler.deleteAllData();
        }));

        SimpleNioServer sn = new SimpleNioServer(kvProcessor);
        sn.bindSockets(cfg.listenaddr, cfg.port);
        try {
            new Thread(() -> {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // Try registering at the ECS, this could fail (returns null) if no bootstrap was provided
                // Or if the ECS could not be reached
                if (registerAtECS(cfg.bootstrap, cfg.listenaddr, cfg.port) == null) {
                    logger.info("ECS unreachable, check if it's started and bootstrapped properly");
                    System.exit(-1);
                }
                while (true) {
                    ServerToServerConnection s2s = new ServerToServerConnection(null);
                    String ecsConnectionAnswer = s2s.buildConnection(new Pair<>(cfg.bootstrap.getHostName(), cfg.bootstrap.getPort()));
                    if (ecsConnectionAnswer.equals("Could not connect to server")) {
                        try {
                            Thread.sleep(10000);
                            registerAtECS(cfg.bootstrap, cfg.listenaddr, cfg.port);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } else {
                        s2s.closeCurrentConnection();
                    }

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
            sn.start();
        } catch (IOException e) { // If an exception occurs, the cache will be flushed to disk
            kvManager.flushCacheToDisk();
        }
    }

    /**
     * Method that deregisters this KVServer from the External configuration server by sending a message
     * to it. The message is taken from {@link de.tum.i13.shared.MessagingProtocol} and reads DEREGISTER
     */
    private static String deregisterFromECS(InetSocketAddress address, String listenAddr, int listenPort) {
        ServerToServerConnection s2s = new ServerToServerConnection(null);
        s2s.buildConnection(new Pair<>(address.getHostName(), address.getPort()));
        String ret = null;
        try {
            ret = s2s.deregister(listenAddr, listenPort);
        } catch (IOException e) {
            logger.severe("Deregistering at ECS failed");
        }
        s2s.closeCurrentConnection();
        return ret;
    }

    /**
     * Method that deregisters this KVServer from the External configuration server by sending a message
     * to it. The message is taken from {@link de.tum.i13.shared.MessagingProtocol} and reads REGISTER
     */
    private static String registerAtECS(InetSocketAddress address, String listenAddr, int listenPort) {
        ServerToServerConnection s2s = new ServerToServerConnection(null);
        s2s.buildConnection(new Pair<>(address.getHostName(), address.getPort()));
        String reply = null;
        try { // If the registration at ECS failed, we just want to return null as the reply
            reply = s2s.register(listenAddr, listenPort);
        } catch (IOException ignored) {
            logger.severe("Registering at ECS failed");
        }
        s2s.closeCurrentConnection();
        return reply;
    }
}
