package com.bbva.kyof.vega.util.crypto;

import com.bbva.kyof.vega.exception.VegaException;
import lombok.Getter;

import javax.crypto.*;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

/**
 * This class acts as a Wrapper for the Java Cipher class.
 *
 * It will automatically reinitialize the internal JAva Cipher if there is a problem during an execution to ensure that
 * the cipher don't contain an invalid internal state to prevent problems with future executions.
 *
 * It also contain enum values to simplify the usage of the valid modes and codecs.
 *
 * * This class is thread safe
 */
class CipherWrapper
{
    /** The mode the cipher is going to work in (Encrypt, Decrypt) */
    private final CipherMode mode;
    /** The key used to encode or decode */
    private final Key key;
    /** The java Cipher instance that will perform the work */
    private final Cipher cipher;
    /** Lock for instance synchronization */
    private final Object lock = new Object();

    /**
     * Create a new wrapper. It will create and initialize the Java Cipher with the given mode, codec and key
     *
     * @param mode the mode for the cipher to encrypt or decrypt
     * @param codec the codec to be used that correspond to the algorithm
     * @param key the key for encoding / decoding
     * @throws VegaException exception thrown if there is a problem creating the wrapper
     */
    CipherWrapper(final CipherMode mode, final CipherCodecType codec, final Key key) throws VegaException
    {
        this.mode = mode;
        this.key = key;

        try
        {
            // Create the cipher instance
            this.cipher = Cipher.getInstance(codec.getStringValue());

            // Initialize it
            this.initializeCipher();
        }
        catch (final NoSuchAlgorithmException | NoSuchPaddingException e)
        {
            throw new VegaException("Error initializing cipher wrapper", e);
        }
    }

    /**
     * Initialize the internal cipher with the mode and key
     *
     * @throws VegaException exception thrown if the key is invalid
     */
    private void initializeCipher() throws VegaException
    {
        try
        {
            this.cipher.init(this.mode.getIntValue(), this.key);
        }
        catch (final InvalidKeyException e)
        {
            throw new VegaException("Invalid key for the cipher", e);
        }
    }

    /**
     * Run the cipher over the given array. It will encode or decode it depending on the cipher mode provided on construction.
     *
     * @param source the message to be encoded or decoded
     * @return the encoded or decoded message
     * @throws VegaException exception thrown if there is a problem running the cipher
     */
    byte[] runCipher(final byte[] source) throws VegaException
    {
        synchronized (this.lock)
        {
            try
            {
                return this.cipher.doFinal(source);
            }
            catch (final IllegalBlockSizeException | BadPaddingException e)
            {
                // If it fails the internal state may be corrupt... We have to reinitialize again
                this.initializeCipher();

                // Relaunch the exception
                throw new VegaException("Unexpected error performing AES encode", e);
            }
        }
    }

    /**
     * Run the cipher over the given byte buffer. It will encode or decode it depending on the cipher mode provided on construction and
     * store the result in the provided target buffer.
     *
     * This method is slower than using byte arrays directly
     *
     * @param source ByteBuffer containing the message to be encoded or decoded
     * @param target Buffer where the encoding or decoding result will be stored
     * @throws VegaException exception thrown if there is a problem running the cipher
     */
    void runCipher(final ByteBuffer source, final ByteBuffer target) throws VegaException
    {
        synchronized (this.lock)
        {
            try
            {
                this.cipher.doFinal(source, target);
            }
            catch (final IllegalBlockSizeException | BadPaddingException | ShortBufferException e)
            {
                // If it fails the internal state may be corrupt... We have to reinitialize again
                this.initializeCipher();

                // Relaunch the exception
                throw new VegaException("Unexpected exception performing AES encode", e);
            }
        }
    }

    /**
     * Available modes for the Cipher
     */
    enum CipherMode
    {
        /** Encrypt mode */
        ENCRYPT(Cipher.ENCRYPT_MODE),
        /** Decrypt mode */
        DECRYPT(Cipher.DECRYPT_MODE);

        /** The value of the mode in int, the Java Cipher only understand an integer for the mode */
        @Getter private final int intValue;

        /**
         * Create a new CipherMode instance given the value in integer
         * @param intValue the integer value of the mode
         */
        CipherMode(final int intValue)
        {
            this.intValue = intValue;
        }
    }
}
