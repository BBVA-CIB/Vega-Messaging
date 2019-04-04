package com.bbva.kyof.vega.msg;

import com.bbva.kyof.vega.serialization.IUnsafeSerializable;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import lombok.*;

import java.util.UUID;

/**
 * Represents of a user data request message header.
 *
 * Contains the request id of the original request the response belongs to.
 *
 * This class is not thread safe!
 */
@NoArgsConstructor
@EqualsAndHashCode
@AllArgsConstructor
public class MsgRespHeader implements IUnsafeSerializable
{
    /** Binary size of the header once it has been serialized */
    private static final int BINARY_SIZE = UnsafeBufferSerializer.UUID_SIZE * 2;

    /** Identifier of the application instance ID that created the message */
    @Getter @Setter private UUID instanceId;

    /** Original request id of the request that has originated this response */
    @Getter @Setter private UUID requestId;

    @Override
    public void toBinary(final UnsafeBufferSerializer buffer)
    {
        buffer.writeUUID(this.instanceId);
        buffer.writeUUID(this.requestId);
    }

    @Override
    public void fromBinary(final UnsafeBufferSerializer buffer)
    {
        // Add the boolean that indicates if there is a request ID field
        this.instanceId = buffer.readUUID();
        this.requestId = buffer.readUUID();
    }

    @Override
    public int serializedSize()
    {
        return BINARY_SIZE;
    }
}