package com.bbva.kyof.vega.msg;

import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import lombok.*;

/**
 * Represents a security response message.
 *
 * It also contains methods to sign and serialize to binary, to read from binary and to verify the signature.
 *
 * This class creates internal byte arrays to handle the serialized / deserialised code and being able to sing and verify the
 * signatures.
 *
 * Always create 1 per publisher or subscriber and reuse to prevent excessive memory allocation.
 *
 * The class is not thread safe!
 */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MsgSecurityResp extends AbstractMsgSecurity
{
    /** Encoded session key */
    @Getter @Setter private byte[] encodedSessionKey = new byte[0];

    @Override
    protected void readAdditionalFields(final UnsafeBufferSerializer buffer)
    {
        // Read the size of the encoded session key
        final int encodedSessionKeySize = buffer.readInt();

        // If the internal byte array for they key don't have the right size, create it again
        if (this.encodedSessionKey.length != encodedSessionKeySize)
        {
            this.encodedSessionKey = new byte[encodedSessionKeySize];
        }

        // Read the session key
        buffer.readBytes(this.encodedSessionKey);
    }

    @Override
    protected void writeAdditionalFields(final UnsafeBufferSerializer buffer)
    {
        buffer.writeInt(this.encodedSessionKey.length); // Write the length of the encoded session key
        buffer.writeBytes(this.encodedSessionKey, 0, this.encodedSessionKey.length);
    }
}