package org.springframework.data.redis.connection.stream;

import java.util.List;

public class ClaimedMessagesIds {
    private final RecordId id;
    private final List<RecordId> claimedMessages;
    private final List<RecordId> deletedMessages;

    public ClaimedMessagesIds(RecordId id, List<RecordId> claimedMessages, List<RecordId> deletedMessages) {
        this.id = id;
        this.claimedMessages = claimedMessages;
        this.deletedMessages = deletedMessages;
    }

    /**
     * @return the message id.
     */
    public RecordId getId() {
        return id;
    }

    /**
     * @return the message id as {@link String}.
     */
    public String getIdAsString() {
        return id.getValue();
    }

    /**
     * @return list of claimed messages.
     */
    List<RecordId> getClaimedMessages() {
        return claimedMessages;
    }

    /**
     * @return list of deleted messages.
     */
    List<RecordId> getDeletedMessages() {
        return deletedMessages;
    }
}
