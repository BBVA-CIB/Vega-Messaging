package com.bbva.kyof.vega.msg;

import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import lombok.*;

/**
 * Represents a security error response message.
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
public class MsgSecurityErrorResp extends AbstractMsgSecurity
{
    /** Error code, no secure publisher found with the given topic publisher id */
    public static final byte NO_SECURE_PUB_FOUND = 0;
    /** Error code, the secure subscriber is not allowed by configuration */
    public static final byte NOT_ALLOWED_BY_CONFIG = 1;
    /** Error code, cannot find the public key for hte given subscriber security id */
    public static final byte PUB_KEY_NOT_FOUND = 2;
    /** Error code, cannot validate the signature */
    public static final byte SIGNATURE_ERROR = 3;

    /** Error code */
    @Getter @Setter private byte errorCode;

    @Override
    protected void writeAdditionalFields(final UnsafeBufferSerializer buffer)
    {
        buffer.writeByte(this.errorCode);
    }

    @Override
    protected void readAdditionalFields(final UnsafeBufferSerializer buffer)
    {
        this.errorCode = buffer.readByte();
    }
}