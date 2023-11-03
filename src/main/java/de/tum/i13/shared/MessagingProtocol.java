package de.tum.i13.shared;

public abstract class MessagingProtocol {
    StatusType statusType;
    String[] arguments;

    public MessagingProtocol(StatusType statusType, String[] arguments) {
        this.statusType = statusType;
        this.arguments = arguments;
    }

    @Override
    public String toString() {
        StringBuilder argsString = new StringBuilder();
        for (String argument : arguments) {
            argsString.append(" ").append(argument);
        }
        return statusType.toString().toLowerCase() + argsString;
    }

    public StatusType getStatusType() {
        return this.statusType;
    }

    public enum StatusType {
        /*MESSAGES KVServer <-> KVStore*/
        ERROR,
        GET, /* Get - request */
        GET_ERROR, /* requested tuple (i.e. value) not found */
        GET_SUCCESS, /* requested tuple (i.e. value) found */
        PUT, /* Put - request */
        PUT_SUCCESS, /* Put - request successful, tuple inserted */
        PUT_UPDATE, /* Put - request successful, value updated */
        PUT_ERROR, /* Put - request not successful */
        DELETE, /* Delete - request */
        DELETE_SUCCESS, /* Delete - request successful */
        SERVER_NOT_RESPONSIBLE, /* Not_responsible - key hash is out of current server's range */
        DELETE_ERROR, /* Delete - request not successful */
        KEYRANGE, /*Keyrange - request metadata */
        KEYRANGE_READ, /* Gives the ranges for servers that have copies of a certain key/value pair */
        KEYRANGE_READ_SUCCESS, /* Successful keyrange_read */
        KEYRANGE_SUCCESS, /* Keyrange_success - metadata sent */
        SERVER_STOPPED, /* Server_stopped - server currently not accepting requests */

        /*PUBSUB SERVICE*/
        PUBLISH, /*Message from KVStore to KVServer or KVServer to ECS. This is a request to publish a key value*/
        PUBLICATION_ERROR, /*Returned if any error occurs from the publish request*/
        PUBLICATION_SUCCESS, /* Success message if publish was successful*/
        SUBSCRIBE, /*Message from KVStore to KVServer or KVServer to ECS. This is a request to subscribe to a key*/
        SUBSCRIBE_SUCCESS, /* Success message if subscribe was successful*/
        NOTIFY, /*Message from ECS Pubsub service to subscriber KVStore. This is a notification that kv has been changed*/
        UNSUBSCRIBE, /*Message from KVStore to KVServer or KVServer to ECS. This is a request to unsubscribe from a key value*/
        UNSUBSCRIBE_SUCCESS, /* Success message if unsubscribe was successful*/

        /*MESSAGES KV <-> ECS and KV <-> KV */
        REGISTER, /* Register <ip> <port> */
        METADATA, /* The ECS sends out the metadata of the KVStore to the KVServer */
        DEREGISTER, /* Message for when a server wants to disconnect from the ECS */
        SET_WRITE_LOCK, /* After a new Server joins, SET_WRITE_LOCK locks the one after it and hands it the hash of the new server */
        ACK, /* Generic acknowledgement sent to servers (e.g. after registration at ECS) */
        PUT_HASH, /* This message is used to insert already hashed keys from server to server (since PUT would hash them again) */
        DELETE_HASH, /* Message used to delete already hashed keys from server to server (similar to PUT_HASH)*/
        START_SERVER, /* A KVServer receives this message, when it is ready to be started
        (i.e. when all the needed keys have been transferred to it) */
        SERVER_WRITE_LOCK, /* Server_write_lock - server currently does not allow write operations */
        PING /* Ping message sent from ECS to KVServers and back to check if the KVServer in question is still reachable */
    }
}
