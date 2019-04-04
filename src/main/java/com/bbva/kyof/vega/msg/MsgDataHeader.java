package com.bbva.kyof.vega.msg;

import com.bbva.kyof.vega.serialization.IUnsafeSerializable;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import lombok.*;

import java.util.UUID;

/**
 * Represents the header of a data message that is sent or received.
 *
 * Contains additional information over the base header that any library message has. In this case the publisher id of the topic publisher
 * that sent the message is added.
 *
 * This class is not thread safe!
 */
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class MsgDataHeader implements IUnsafeSerializable
{
    /** Binary size of the header once it has been serialized */
    static final int BINARY_SIZE = UnsafeBufferSerializer.UUID_SIZE * 2;

    /** Identifier of the application instance ID that created the message */
    @Getter @Setter private UUID instanceId;

    /** Id of the topic publisher that sent the message */
    @Getter @Setter private UUID topicPublisherId;

    @Override
    public void toBinary(final UnsafeBufferSerializer buffer)
    {
        buffer.writeUUID(this.instanceId);
        buffer.writeUUID(this.topicPublisherId);
    }

    @Override
    public void fromBinary(final UnsafeBufferSerializer buffer)
    {
        // TopicUniqueId
        this.instanceId = buffer.readUUID();
        this.topicPublisherId = buffer.readUUID();
    }

    @Override
    public int serializedSize()
    {
        return BINARY_SIZE;
    }
}