package com.bbva.kyof.vega.msg;

import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import lombok.*;

import java.util.UUID;

/**
 * Represents of a user data request message header.
 *
 * Contains additional information over the data header by including the unique request id.
 *
 * This class is not thread safe!
 */
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class MsgReqHeader extends MsgDataHeader
{
    /** Binary size of the header once it has been serialized */
    private static final int REQ_BINARY_SIZE = MsgDataHeader.BINARY_SIZE + UnsafeBufferSerializer.UUID_SIZE;

    /** Request ID of the message */
    @Getter @Setter private UUID requestId;

    /**
     * Constructor with all the arguments
     * @param instanceId instance id that sent the message
     * @param topicPublisherId the unique id of the topic publisher that sent the message
     * @param requestId the request id of the request represented by the header
     * @param sequenceNumber the sequence number of the message related to the topic publisher that sent it
     */
    MsgReqHeader(final UUID instanceId, final UUID topicPublisherId, final long sequenceNumber, final UUID requestId)
    {
        super(instanceId, topicPublisherId, sequenceNumber);
        this.requestId = requestId;
    }

    @Override
    public void toBinary(final UnsafeBufferSerializer buffer)
    {
        super.toBinary(buffer);
        buffer.writeUUID(this.requestId);
    }

    @Override
    public void fromBinary(final UnsafeBufferSerializer buffer)
    {
        super.fromBinary(buffer);
        this.requestId = buffer.readUUID();
    }

    @Override
    public int serializedSize()
    {
        return REQ_BINARY_SIZE;
    }
}