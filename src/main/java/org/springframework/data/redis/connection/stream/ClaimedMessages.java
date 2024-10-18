package org.springframework.data.redis.connection.stream;

import java.util.List;

public class ClaimedMessages {
    private final RecordId id;
    private final List<ByteRecord> claimedMessages;
    private final List<ByteRecord> deletedMessages;

    public ClaimedMessages(RecordId id, List<ByteRecord> claimedMessages, List<ByteRecord> deletedMessages) {
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
    List<ByteRecord> getClaimedMessages() {
        return claimedMessages;
    }

    /**
     * @return list of deleted messages.
     */
    List<ByteRecord> getDeletedMessages() {
        return deletedMessages;
    }
}
