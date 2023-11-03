package de.tum.i13.ecs;

import de.tum.i13.KVStore.ServerToServerConnection;
import de.tum.i13.pubSub.PubSubBroker;
import de.tum.i13.shared.KVHash;
import de.tum.i13.shared.Pair;
import de.tum.i13.shared.Range;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;


/**
 * The class is responsible for handling all of the logic of the ECS Server.
 * The offered functionality is the following:
 * registering a new KVServer to the ECS,
 * updating the metadata of the KVServers connected with the ECS,
 * setting the write lock of a successor KVServer from the logical address ring,
 * removing a KVServer from the logical address ring,
 * monitoring the servers through pinging.
 */
public class ECSManager {
    private final Map<Pair<String, Integer>, ServerToServerConnection> connectionMap;
    /**
     * The ECS is provided with a broker, which is responsible for the functionality of the pub-sub system
     */
    private final PubSubBroker broker;
    /**
     * The executor needed for pinging the servers from the logical address ring.
     * We create a separate Thread for this task.
     */
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    /**
     * The Future.
     */
    Future<?> future;
    /**
     * The up-to-date list of the metadata of the KVServers from the logical address ring.
     */
    private List<Range> metadata;

    /**
     * Instantiates a new ECS manager.
     * In a parallel thread, we initiate the procedure of pinging of the KVServers.
     */
    public ECSManager(String pubSubFolder) {
        connectionMap = new HashMap<>();
        metadata = new ArrayList<>();
        new Thread(() -> future = executor.scheduleAtFixedRate(this::monitorServers, 0, 1, TimeUnit.SECONDS)).start();
        broker = new PubSubBroker(pubSubFolder);
        try {
            broker.notifyAllSubscribers();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public PubSubBroker getBroker() {
        return broker;
    }

    /**
     * Register a new KVServer to the ECS.
     * We initially build a new connection to the new KVServer, which is followed by adding it to our up-to-date
     * connectionMap of the ECS.
     * If our connectionMap has a size greater than 1, we initiate the "balancing" of the new KVServer and it's successor
     * by assigning the new metadata, and transferring the necessary key-value pairs.
     *
     * @param ip   the ip
     * @param port the port
     */
    public void register(String ip, String port) {
        StartECS.logger.info("Registering " + ip + ":" + port);
        Pair<String, Integer> kvIp = new Pair<>(ip, Integer.parseInt(port));
        ServerToServerConnection s2s = new ServerToServerConnection(null);
        s2s.buildConnection(kvIp); // Connect to the servers open port
        connectionMap.put(kvIp, s2s); // Save the server and its connetionmanager
        metadata = KVHash.hashMetaData(new ArrayList<>(connectionMap.keySet())); // Update ECS metadata because of new server
        updateServerMetadata(); // send updated metadata to all servers
        if (connectionMap.size() > 1) {
            // transfer keys
            setSuccessorWriteLock(kvIp);
        } else {
            s2s.startServer();
        }
    }

    /**
     * The method updates the metadata of all connected KVServers.
     */
    private void updateServerMetadata() {
        StartECS.logger.info("sending metadata update to servers");
        metadata = KVHash.hashMetaData(new ArrayList<>(connectionMap.keySet())); // Update ECS metadata because of removal
        StringBuilder metadataString = new StringBuilder();
        for (Range metadatum : metadata) {
            metadataString.append(metadatum.toString());
        }
        for (ServerToServerConnection connection : connectionMap.values()) {
            connection.sendMetadata(metadataString.toString());
        }
    }

    /**
     * The method is used to initiate the key-value pairs transfer for a  server registration, or deregistration.
     * It sets the write-lock of the successor server, and initiates the transfer.
     *
     * @param kvIp
     */
    private void setSuccessorWriteLock(Pair<String, Integer> kvIp) {
        Range successor = getSuccessor(kvIp);
        int successorServerIndex = metadata.indexOf(successor);
        int currentServerIndex = Math.floorMod(successorServerIndex - 1, metadata.size());
        Range currentServer = metadata.get(currentServerIndex);
        connectionMap.get(
                new Pair<>(successor.getAddress(), successor.getPort()))
                .setWriteLock(currentServer.getAddress(), currentServer.getPort());
    }

    /**
     * The method gets the successor KVServer of the kvIp Pair from the logical address ring.
     *
     * @param kvIp
     * @return the successor
     */
    private Range getSuccessor(Pair<String, Integer> kvIp) {
        for (int i = 0; i < metadata.size(); i++) {
            if (metadata.get(i).getAddress().equals(kvIp.getLeft()) && metadata.get(i).getPort() == kvIp.getRight()) {
                return metadata.get((i + 1) % metadata.size()); // Get successor information
            }
        }
        return null; // Server is the last one in the chain
    }

    /**
     * Removes server pair from the logical address ring.
     * The removal process initiates the removal process by finding the successor KVServer of our current KVServer,
     * followed by an update to all KVServers of the affected metadata.
     *
     * @param ip   the ip
     * @param port the port
     * @return the ip-port pair of the removed KVServer
     */
    public Pair<String, Integer> removeServer(String ip, String port) {
        StartECS.logger.info("Deregistering " + ip + ":" + port);
        Pair<String, Integer> kvIp = new Pair<>(ip, Integer.parseInt(port));
        Range successor = getSuccessor(kvIp);
        connectionMap.remove(kvIp).closeCurrentConnection();
        updateServerMetadata();
        if (successor.getAddress().equals(ip) && successor.getPort() == Integer.parseInt(port)) {
            // sucessor is server that triggered remove, which means there's only one left and we set a flag (-1)
            return new Pair<>("", -1);
        } else {
            return new Pair<>(successor.getAddress(), successor.getPort());
        }
    }

    /**
     * The method has as it's main focus the monitoring of currently active servers.
     * For every entry in our connectionMap we send a new message type, namely PING.
     * If the KVServer fails to reply within 700, it means that is has unexpectedly shutdown.
     * Consequently we remove it from our connectionMap.
     * <p>
     * Using a heartbeat ping strategy instead of gossiping or other alternatives was a conscious design decision
     * made due to the lower overhead it imposes on the nodes and it's relative simplicity
     * "Simplicity is the ultimate sophistication" -Lao Tzu
     */
    private void monitorServers() {
        List<Pair<String, Integer>> unavailableServers = new ArrayList<>(); // List of all the servers
        // that could not be reached in this cycle

        ExecutorService pingExecutor = Executors.newSingleThreadExecutor();
        Future<?> futurePing;

        //StartECS.logger.info("############ Ping Run start ###########");
        // We loop over all registered servers and identify the ones which either take too long to respond (700ms)
        // Or the ones which could not be contacted at all
        for (Map.Entry<Pair<String, Integer>, ServerToServerConnection> server : connectionMap.entrySet()) {
            futurePing = pingExecutor.submit(() -> {
                try { // we have to have this block inside our lambda
                    pingServer(server.getValue());
                    //StartECS.logger.info(server.getKey() + " responded to ping");
                } catch (IOException e) { // Ping servers will throw an IOE, if pinging outright failed (not reachable)
                    unavailableServers.add(server.getKey());
                    StartECS.logger.info(server.getKey() + " did not respond to ping. Removing...");
                }
            });
            try {
                futurePing.get(700, TimeUnit.MILLISECONDS); // The future will throw TOE if we could contact the
                // Server, but it took too long to respond
            } catch (InterruptedException | ExecutionException e) {
                System.out.println("InterruptedException | ExecutionException e");
                unavailableServers.add(server.getKey());
                break; // Something went wrong with our future, try again in the next cycle
            } catch (TimeoutException e) {
                System.out.println("The server shutdown ungracefully!");
                unavailableServers.add(server.getKey());
            }
        }
        for (Pair<String, Integer> unavailableServer : unavailableServers) {
            connectionMap.remove(unavailableServer); // delete all connections that responded negatively to a ping check
        }
        if (unavailableServers.size() != 0) {
            updateServerMetadata();
        }
    }

    /**
     * Method for sending the ping message to the KVServers
     *
     * @param pingConnection Active connection to ping
     */
    private void pingServer(ServerToServerConnection pingConnection) throws IOException {
        pingConnection.ping();
    }

}
