package com.bbva.kyof.vega.msg;

import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Represents a security request message.
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
public class MsgSecurityReq extends AbstractMsgSecurity
{
    @Override
    protected void readAdditionalFields(final UnsafeBufferSerializer buffer)
    {
        // There are no additional fields, nothing to do here
    }

    @Override
    protected void writeAdditionalFields(final UnsafeBufferSerializer buffer)
    {
        // There are no additional fields, nothing to do here
    }
}