package de.tum.i13.shared;

public class KVMessage extends MessagingProtocol {
    public KVMessage(StatusType statusType, String[] arguments) {
        super(statusType, arguments);
    }

    public KVMessage(StatusType statusType) {
        super(statusType, new String[0]);
    }
}
