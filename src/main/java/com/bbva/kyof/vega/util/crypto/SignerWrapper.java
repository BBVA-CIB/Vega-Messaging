package com.bbva.kyof.vega.util.crypto;

import com.bbva.kyof.vega.exception.VegaException;

import java.security.*;

/**
 * This class acts as a Wrapper for the Java Signature class configured to perform digital signatures
 *
 * It will automatically reinitialize the internal Java Signature if there is a problem during an execution to ensure that
 * the signature don't contain an invalid internal state to prevent problems with future executions.
 *
 * It also contain enum values to simplify the usage of the valid codecs.
 *
 * This class is thread safe
 */
class SignerWrapper
{
    /** The key used to sign */
    private final PrivateKey key;
    /** The java signature instance that will perform the work */
    private final Signature signature;
    /** Lock for instance synchronization */
    private final Object lock = new Object();

    /**
     * Create a new wrapper. It will create and initialize the Java Signature with the given codec and key
     *
     * @param codec the codec to be used that correspond to the algorithm
     * @param key the private key for the signature
     * @throws VegaException exception thrown if there is a problem creating the wrapper
     */
    SignerWrapper(final SignatureCodecType codec, final PrivateKey key) throws VegaException
    {
        this.key = key;

        try
        {
            // Create the signature instance
            this.signature = Signature.getInstance(codec.getStringValue());

            // Initialize it
            this.initializeSignature();
        }
        catch (final NoSuchAlgorithmException e)
        {
            throw new VegaException("Error initializing signature wrapper", e);
        }
    }

    /**
     * Initialize the internal signature with key
     *
     * @throws VegaException exception thrown if the key is invalid
     */
    private void initializeSignature() throws VegaException
    {
        try
        {
            this.signature.initSign(this.key);
        }
        catch (final InvalidKeyException e)
        {
            throw new VegaException("Invalid key for the signature", e);
        }
    }

    /**
     * Sign the given message
     *
     * @param msg the message to sign
     * @return the signature of the message in binary
     * @throws VegaException exception thrown if there is any problem performing the message signature
     */
    byte[] sign(final byte[] msg) throws VegaException
    {
        synchronized (this.lock)
        {
            try
            {
                this.signature.update(msg);
                return this.signature.sign();
            }
            catch (final SignatureException e)
            {
                // If there is a problem, reinitialize the signature to avoid inconsistent states
                this.initializeSignature();

                throw new VegaException("Error creating own digital signature of message", e);
            }
        }
    }
}
