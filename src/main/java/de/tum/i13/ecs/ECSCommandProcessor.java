package de.tum.i13.ecs;

import de.tum.i13.client.KVClient;
import de.tum.i13.pubSub.Subscriber;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.MessagingProtocol;
import de.tum.i13.shared.Pair;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * This class is reponsible for processing messages received from the KVServers.
 * Valid commands are: register or deregister.
 * Just like in KVCommandProcessor, we process the request and initiate the expected functionality for the given
 * command.
 * If the processing of the command is successful, we return the ACK message.
 */
public class ECSCommandProcessor implements CommandProcessor {

    /**
     * We use the ECSManager object to execute the different methods, for the different message types.
     */
    private final ECSManager ecsManager;

    public ECSCommandProcessor(String pubSubFolder) {
        this.ecsManager = new ECSManager(pubSubFolder);
    }

    /**
     * This method will parse the server message for a register or deregister message.
     * The proper  handling is passed on to the ECSManager.
     *
     * @param command String that contains the entire message from the KVServer
     * @return storeAnswer is a MessagingProtocol.toString() of type ACK.
     */
    @Override
    public String process(String command) {
        command = command.replace("\r\n", "");
        String[] commandArgs = command.split(" ");
        switch (commandArgs[0]) {
            case "register" -> ecsManager.register(commandArgs[1], commandArgs[2]);
            case "deregister" -> {
                Pair<String, Integer> successorPair = ecsManager.removeServer(commandArgs[1], commandArgs[2]);
                return new ECSMessage(MessagingProtocol.StatusType.SET_WRITE_LOCK,
                        new String[]{successorPair.getLeft(), "" + successorPair.getRight(), "" + 0}).toString() + "\r\n";
            }
            case "publish" -> {
                String key = commandArgs[1];
                String newValue;
                int retention;

                String[] valueArr = new String[commandArgs.length - 4];
                System.arraycopy(commandArgs, 2, valueArr, 0, valueArr.length);


                retention = Integer.parseInt(commandArgs[commandArgs.length - 1]);
                newValue = KVClient.convertStrArrayToStr(valueArr, " ");

                ecsManager.getBroker().publish(key);
                ecsManager.getBroker().storeMessage(key, newValue, retention);
                ecsManager.getBroker().notify(key, newValue, retention);
                return new ECSMessage(MessagingProtocol.StatusType.PUBLICATION_SUCCESS,
                        new String[]{key + " " + newValue}).toString() + "\r\n";
            }
            case "subscribe" -> {
                StartECS.logger.info("Message: " + command);
                String sID = commandArgs[1];
                String key = commandArgs[2];
                String port = commandArgs[3];
                String address = commandArgs[4].replace("/", "");
                Subscriber newSubscriber = new Subscriber(sID, address, port);

                ecsManager.getBroker().addSubscriber(key, newSubscriber);

                return new ECSMessage(MessagingProtocol.StatusType.SUBSCRIBE_SUCCESS,
                        new String[]{newSubscriber.getSid() + " " + key}).toString() + "\r\n";
            }
            case "unsubscribe" -> {
                String sID = commandArgs[1];
                String key = commandArgs[2];
                ecsManager.getBroker().removeSubscriber(key, sID);

                return new ECSMessage(MessagingProtocol.StatusType.UNSUBSCRIBE_SUCCESS,
                        new String[]{key}).toString() + "\r\n";
            }
        }
        return new ECSMessage(MessagingProtocol.StatusType.ACK).toString() + "\r\n"; // This can be ignored
    }

    /**
     * Notification for an accepted connection
     *
     * @param address       address of the ECS
     * @param remoteAddress the address of the KVServer
     * @return notification message
     */
    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        return "Connection to T15 ECS established\r\n";
    }

    /**
     * Logging for a closed connection
     *
     * @param address address of the KVserver
     */
    @Override
    public void connectionClosed(InetAddress address) {
        // Connection has been successfully closed
    }
}
