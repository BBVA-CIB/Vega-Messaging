package com.bbva.kyof.vega.msg;

import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.crypto.RSACrypto;
import lombok.*;

import java.util.UUID;

/**
 * Base class for security messages
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
@EqualsAndHashCode(exclude = {"unsignedContent", "signature"})
@ToString(exclude = {"unsignedContent", "signature"})
public abstract class AbstractMsgSecurity
{
    /** Identifier of the application instance ID that created the message */
    @Getter @Setter private UUID instanceId;

    /** Id of the request or response */
    @Getter @Setter private UUID requestId;

    /** Id of the vega instance the message is for */
    @Getter @Setter private UUID targetVegaInstanceId;

    /** Id of the topic publisher they key belongs to */
    @Getter @Setter private UUID topicPublisherId;

    /** Security id of the message sender */
    @Getter @Setter private int senderSecurityId;

    /** Message signature */
    private byte[] signature = new byte[0];

    /** Unsigned contents of the message */
    private byte[] unsignedContent = new byte[0];

    /**
     * Read additional fields from binary
     *
     * @param buffer the buffer to read from
     */
    protected abstract void readAdditionalFields(UnsafeBufferSerializer buffer);

    /**
     * Write additional fields to binary
     *
     * @param buffer the buffer to write into
     */
    protected abstract void writeAdditionalFields(UnsafeBufferSerializer buffer);

    /**
     * Serialize the message into the given buffer and sign the contents. The serialized message will contain the original message
     * and the signature.
     *
     * @param buffer the buffer to serialize into
     * @param rsaCrypto the instance of RSACrypto with the ability to sign the messages from the current instance
     * @throws VegaException exception thrown if there is any problem in the process
     */
    public void signAndSerialize(final UnsafeBufferSerializer buffer, final RSACrypto rsaCrypto) throws VegaException
    {
        // Get the start offset
        final int startOffset = buffer.getOffset();

        // Write the fields
        buffer.writeUUID(this.instanceId);
        buffer.writeUUID(this.requestId);
        buffer.writeUUID(this.targetVegaInstanceId);
        buffer.writeUUID(this.topicPublisherId);
        buffer.writeInt(this.senderSecurityId);

        // Write additional fields
        this.writeAdditionalFields(buffer);

        // Read the unsigned contents we just write. First make sure there is enough space for it
        if (this.unsignedContent.length != buffer.getOffset())
        {
            this.unsignedContent = new byte[buffer.getOffset() - startOffset];
        }
        buffer.readBytes(startOffset, unsignedContent);

        // Sign the contents
        this.signature = rsaCrypto.sign(unsignedContent);

        // Write the size of the signature
        buffer.writeInt(this.signature.length);
        // Write the signature
        buffer.writeBytes(this.signature, 0, this.signature.length);
    }

    /**
     * Read the message from a buffer containing the binary contents.
     *
     * It will load also the signature but won't perform any verification. The verification can be done using a separate method.
     *
     * @param buffer the buffer with the binary contents of the message
     */
    public void fromBinary(final UnsafeBufferSerializer buffer)
    {
        // Calculate the start offset
        final int startOffset = buffer.getOffset();

        // Read the message fields
        this.instanceId = buffer.readUUID();
        this.requestId = buffer.readUUID();
        this.targetVegaInstanceId = buffer.readUUID();
        this.topicPublisherId = buffer.readUUID();
        this.senderSecurityId = buffer.readInt();

        // Read the additional fields
        this.readAdditionalFields(buffer);

        // Prepare the unsigned content to force the right size
        if (this.unsignedContent.length != buffer.getOffset())
        {
            this.unsignedContent = new byte[buffer.getOffset() - startOffset];
        }

        // Read all the bytes without the signature
        buffer.readBytes(startOffset, this.unsignedContent);

        // Read the signature size
        final int signatureSize = buffer.readInt();

        // Change the array size if required
        if (this.signature.length != signatureSize)
        {
            // Create the signature array and read it
            this.signature = new byte[signatureSize];
        }

        // Finally read the signature
        buffer.readBytes(this.signature);
    }

    /**
     * Verify the message signature after reading the contents from binary.
     *
     * This method uses the internal "unsignedContent" array that is filled after a binary read. Never call this method
     * unless the message has been filled with the "fromBinary" call.
     *
     * @param rsaCrypto the RSACrypto instance containing all the valid public keys and signature validators
     * @return true if valid
     * @throws VegaException exception thrown if there is a problem during the verification
     */
    public boolean verifySignature(final RSACrypto rsaCrypto) throws VegaException
    {
        return rsaCrypto.verifySignature(this.senderSecurityId, this.signature, this.unsignedContent);
    }
}
