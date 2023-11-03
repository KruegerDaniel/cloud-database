package de.tum.i13.shared;

import de.tum.i13.KVserver.kv.KVManager;
import picocli.CommandLine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;

public class Config {
    @CommandLine.Option(names = "-p", description = "Sets the port of the KVserver", defaultValue = "5153")
    public int port;

    @CommandLine.Option(names = "-a", description = "Which address the KVserver should listen to", defaultValue = "127.0.0.1")
    public String listenaddr;

    @CommandLine.Option(names = "-b", description = "Bootstrap broker where clients and other brokers connect first to retrieve configuration, port and ip, e.g., 192.168.1.1:5153", defaultValue = "clouddatabases.i13.in.tum.de:5153")
    public InetSocketAddress bootstrap;

    @CommandLine.Option(names = "-d", description = "Directory for files", defaultValue = "data/")
    public Path dataDir;

    @CommandLine.Option(names = "-l", description = "Logfile", defaultValue = "server.log")
    public Path logfile;

    @CommandLine.Option(names = "-h", description = "Displays help", usageHelp = true)
    public boolean usagehelp;

    @CommandLine.Option(names = "-c", description = "Size of the cache", defaultValue = "100")
    public int cacheSize;

    @CommandLine.Option(names = "-s", description = "Cache displacement strategy", defaultValue = "FIFO")
    public KVManager cacheStrategy;

    @CommandLine.Option(names = "-ll", description = "LogLevel", defaultValue = "INFO")
    public Level logLevel;

    @CommandLine.Option(names = "-r", description = "Retention period", defaultValue = "5")
    public int retentionPeriod;

    public static Config parseCommandlineArgs(String[] args) {
        Config cfg = new Config();
        CommandLine cl = new CommandLine(cfg);
        CommandLine.ParseResult parseResult = cl
                .registerConverter(InetSocketAddress.class, new InetSocketAddressTypeConverter())
                .registerConverter(KVManager.class, new KVStoreStrategyConverter())
                .registerConverter(Level.class, new LogLevelConverter())
                .parseArgs(args);

        if (!Files.exists(cfg.dataDir)) {
            try {
                Files.createDirectory(cfg.dataDir);
            } catch (IOException e) {
                System.err.println("Could not create directory");
                e.printStackTrace();
                System.exit(-1);
            }
        }

        if (cfg.usagehelp) {
            System.out.println(cl.getUsageMessage());
            System.exit(0);
        }

        if (cfg.cacheSize <= 0) {
            System.err.println("Fatal: Can not instantiate a cache <= 0");
            System.exit(-1);
        }

        if (!parseResult.errors().isEmpty()) {
            for (Exception ex : parseResult.errors()) {
                ex.printStackTrace();
            }
            CommandLine.usage(new Config(), System.out);
            System.exit(-1);
        }

        if (cfg.retentionPeriod <= 0) {
            System.err.println("Fatal: Can not instantiate a retention period <= 0");
            System.exit(-1);
        }

        return cfg;
    }

    @Override
    public String toString() {
        return "Config{" +
                "port=" + port +
                ", listenaddr='" + listenaddr + '\'' +
                ", bootstrap=" + bootstrap +
                ", cacheSize=" + cacheSize +
                ", cacheStrategy=" + cacheStrategy +
                ", dataDir=" + dataDir +
                ", logfile=" + logfile +
                ", logLevel=" + logLevel +
                ", usagehelp=" + usagehelp +
                '}';
    }
}

