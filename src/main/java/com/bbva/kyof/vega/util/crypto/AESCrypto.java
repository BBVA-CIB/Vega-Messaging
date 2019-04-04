package com.bbva.kyof.vega.util.crypto;

import com.bbva.kyof.vega.exception.VegaException;

import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Helper class to perform AES cryptography.
 *
 * This class is not thread safe!!
 */
public class AESCrypto
{
    /** Additional space for AES metadata */
    public static final int AES_METADATA_SPACE = 128;

    /** Random value for the key generation */
    private static final Random RND = new Random(System.currentTimeMillis());

    /** Size of the AES key in bytes */
    public static final int DEFAULT_KEY_SIZE = 16;

    /** The AES key that will be used to encode / decode */
    private final byte[] aesKey;
    /** The Cipher that will encode the messages */
    private final CipherWrapper encoder;
    /** The Cipher that will decode the messages */
    private final CipherWrapper decoder;

    /**
     * Create a new instance given the AES key that will be used to encode / decode
     *
     * @param key the key to encode and decode
     * @throws VegaException exception thrown if there is a problem creating the internal encoding classes
     */
    public AESCrypto(final byte[] key) throws VegaException
    {
        // Store the binary key
        this.aesKey = key.clone();

        // Create a new key
        final SecretKeySpec secretAESKey = new SecretKeySpec(this.aesKey, "AES");

        // Create the encoders / decoders
        this.encoder = new CipherWrapper(CipherWrapper.CipherMode.ENCRYPT, CipherCodecType.AES, secretAESKey);
        this.decoder = new CipherWrapper(CipherWrapper.CipherMode.DECRYPT, CipherCodecType.AES, secretAESKey);
    }

    /**
     * Create a new instance for AES encoding / decoding, the instance will use the provided key
     *
     * @return the created instance
     * @throws VegaException exception thrown if the instance cannot be created
     */
    public static AESCrypto createNewInstance() throws VegaException
    {
        // Create the random key
        final byte[] rndKey = new byte[DEFAULT_KEY_SIZE];

        synchronized (RND)
        {
            RND.nextBytes(rndKey);
        }

        return new AESCrypto(rndKey);
    }

    /**
     * Calculate the expected encrypted size of the message
     * @param msgToEncrypt the message to encrypt, it will consider the content from possition to limit
     * @return the size of the encrypted message
     */
    public int expectedEncryptedSize(final ByteBuffer msgToEncrypt)
    {
        return (msgToEncrypt.limit() - msgToEncrypt.position()) + AES_METADATA_SPACE;
    }

    /**
     * Calculate the expected encrypted size of the message
     * @param msgToEncrypt the message to encrypt
     * @return the size of the encrypted message
     */
    public int expectedEncryptedSize(final byte[] msgToEncrypt)
    {
        return msgToEncrypt.length + AES_METADATA_SPACE;
    }

    /**
     * Return the AES key used by this encoder / decoder
     *
     * @return the AES key used by this encoder / decoder
     */
    public byte[] getAESKey()
    {
        return this.aesKey;
    }

    /**
     * Encode the given message and return the AES encoding with the key used during creation of the class
     *
     * @param msg the message to be encoded
     * @return the encoded message
     * @throws VegaException exception thrown if there is a problem encoding the message
     */
    public byte[] encode(final byte[] msg) throws VegaException
    {
        return this.encoder.runCipher(msg);
    }

    /**
     * Encode the given message and return the AES encoding with the key used during creation of the class
     *
     * This method is slower than using byte arrays directly
     *
     * @param msgToEncode ByteBuffer containing the message that should be encoded
     * @param targetBuffer Buffer where the encoding result will be stored
     * @throws VegaException exception thrown if there is a problem encoding the message
     */
    public void encode(final ByteBuffer msgToEncode, final ByteBuffer targetBuffer) throws VegaException
    {
        this.encoder.runCipher(msgToEncode, targetBuffer);
    }

    /**
     * Decode the given message and return the AES decoding with the key used during creation of the class
     *
     * @param msg the message to be decoded
     * @return the decoded message
     * @throws VegaException exception thrown if there is a problem decoding the message
     */
    public byte[] decode(final byte[] msg) throws VegaException
    {
        return decoder.runCipher(msg);
    }

    /**
     * Decode the given message and return the AES decoding with the key used during creation of the class.
     *
     * This method is slower than using byte arrays directly
     *
     * @param msgToDecode ByteBuffer containing the message that should be decoded
     * @param resultBuffer The buffer where the decoded message will be stored
     * @throws VegaException exception thrown if there is an internal problem decoding the message
     */
    public void decode(final ByteBuffer msgToDecode, final ByteBuffer resultBuffer) throws VegaException
    {
        decoder.runCipher(msgToDecode, resultBuffer);
    }
}

