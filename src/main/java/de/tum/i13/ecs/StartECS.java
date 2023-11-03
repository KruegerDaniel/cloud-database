package de.tum.i13.ecs;

import de.tum.i13.KVserver.nio.SimpleNioServer;
import de.tum.i13.shared.Config;

import java.io.IOException;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

/**
 * Simple class containing a main method that start an External Configuration Service capable of managing multiple
 * KVServers
 */
public class StartECS {

    public static Logger logger = Logger.getLogger(StartECS.class.getName());

    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);
        setupLogging(cfg.logfile);
        logger.setLevel(cfg.logLevel);
        logger.info("Config: " + cfg.toString());
        logger.info("starting ECSServer");

        String pubSubFolder = String.valueOf(cfg.dataDir);

        ECSCommandProcessor ecsCommandProcessor = new ECSCommandProcessor(pubSubFolder);

        SimpleNioServer sn = new SimpleNioServer(ecsCommandProcessor);
        sn.bindSockets(cfg.listenaddr, cfg.port);
        sn.start();
        System.out.println("ECS shutting down");
    }
}
