package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.crypto.AESCrypto;
import org.agrona.DirectBuffer;

import java.nio.ByteBuffer;

/**
 * Helper class to encode the contents of a message using AES encryption.
 *
 * The class tries to reuse buffers as much as possible to improve performance.
 *
 * This class is not thread safe, and it returns always the same buffer as result, use it carefully.
 */
class AesTopicMsgEncoder
{
    /** Starting size for the reusable buffers */
    private static final int BUFFERS_START_SIZE = 1024;

    /** AESCrypto serizer helper class */
    private final AESCrypto aesCrypto;

    /** Reusable buffer to copy the unencrypted message before performing the encryption */
    private ByteBuffer unencryptedMsgBuffer = ByteBuffer.allocate(BUFFERS_START_SIZE);

    /** Reusable buffer that will contain the resultant encrypted message */
    private ByteBuffer encrypedMsgBuffer = ByteBuffer.allocate(BUFFERS_START_SIZE);

    /** Create a new encoder  */
    AesTopicMsgEncoder() throws VegaException
    {
        this.aesCrypto = AESCrypto.createNewInstance();
    }

    /**
     * Return the AES key
     * @return the AES key
     */
    byte[] getAESKey()
    {
        return this.aesCrypto.getAESKey();
    }

    /**
     * Encrypt the given message
     * @param message the message to encrypt
     * @param offset the offset where the message starts
     * @param length the length of the message
     * @return the buffer containing the encrypted message
     * @throws VegaException exception thrown if there is any issue
     */
    ByteBuffer encryptMessage(final DirectBuffer message, final int offset, final int length) throws VegaException
    {
        // Reset the unencrypted msg buffer
        this.unencryptedMsgBuffer.clear();

        // Reset the encrypted msg buffer
        this.encrypedMsgBuffer.clear();

        // Verify the message size and allocate more space if required before performing a copy of the message
        if (this.unencryptedMsgBuffer.capacity() < length)
        {
            this.unencryptedMsgBuffer = ByteBuffer.allocate(length * 2);
        }

        // First we need the copy the message contents into a byte buffer
        message.getBytes(offset, this.unencryptedMsgBuffer, length);

        // Flip to set position to 0 and limit to the right value
        this.unencryptedMsgBuffer.flip();

        // Calculate the expected encrypted msg size
        final int expectedEncryptedSize = aesCrypto.expectedEncryptedSize(this.unencryptedMsgBuffer);

        // Make sure our encrypted buffer is big enough and if not allocate more size
        if (this.encrypedMsgBuffer.capacity() < expectedEncryptedSize)
        {
            this.encrypedMsgBuffer = ByteBuffer.allocate(expectedEncryptedSize * 2);
        }

        // Perform the encryption
        this.aesCrypto.encode(this.unencryptedMsgBuffer, this.encrypedMsgBuffer);

        // Flip the result buffer to leave it ready to be sent
        this.encrypedMsgBuffer.flip();

        // Return the result
        return this.encrypedMsgBuffer;
    }
}
