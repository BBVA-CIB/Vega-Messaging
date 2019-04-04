package com.bbva.kyof.vega.util.crypto;

import com.bbva.kyof.vega.exception.VegaException;
import lombok.extern.slf4j.Slf4j;

import java.security.*;

/**
 * This class acts as a Wrapper for the Java Signature class configured to perform verifications of digital signatures
 *
 * It will automatically reinitialize the internal Java Signature if there is a problem during an execution to ensure that
 * the signature don't contain an invalid internal state to prevent problems with future executions.
 *
 * It also contain enum values to simplify the usage of the valid codecs.
 *
 * This class is thread safe
 */
@Slf4j
class VerifierWrapper
{
    /** The key used to sign */
    private final PublicKey key;
    /** The java signature instance that will perform the work */
    private final Signature signature;
    /** Lock for instance synchronization */
    private final Object lock = new Object();

    /**
     * Create a new wrapper. It will create and initialize the Java Signature with the given codec and key
     *
     * @param codec the codec to be used that correspond to the algorithm
     * @param key the public key for the signature verification
     * @throws VegaException exception thrown if there is a problem creating the wrapper
     */
    VerifierWrapper(final SignatureCodecType codec, final PublicKey key) throws VegaException
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
            this.signature.initVerify(this.key);
        }
        catch (final InvalidKeyException e)
        {
            throw new VegaException("Invalid key for the signature", e);
        }
    }

    /**
     * Verify the given signature for the message.
     *
     * @param signature the signature of the message
     * @param signedMessage the message that has been signed
     * @return true if the signature is trusted
     * @throws VegaException exception thrown if there is an unexpected issue
     */
    boolean verifySignature(final byte[] signature, final byte[] signedMessage) throws VegaException
    {
        synchronized (this.lock)
        {
            try
            {
                this.signature.update(signedMessage);
                return this.signature.verify(signature);
            }
            catch (final SignatureException e)
            {
                log.warn("Unexpected error verifying message signature", e);

                // Initialize it again to prevent invalid states
                this.initializeSignature();

                return false;
            }
        }
    }
}
