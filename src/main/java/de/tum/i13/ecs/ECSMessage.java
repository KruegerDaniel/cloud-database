package de.tum.i13.ecs;

import de.tum.i13.shared.MessagingProtocol;

/**
 * Class used for sending ECS Messages.
 */
public class ECSMessage extends MessagingProtocol {

    public ECSMessage(MessagingProtocol.StatusType statusType, String[] arguments) {
        super(statusType, arguments);
    }

    public ECSMessage(MessagingProtocol.StatusType statusType) {
        super(statusType, new String[0]);
    }
}
