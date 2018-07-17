package com.bbva.kyof.vega.util.crypto;

import com.bbva.kyof.vega.exception.VegaException;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class for RSA encoding / decoding and to handle signatures
 *
 * The instance of the class will contain the private key of the application and the public key of the other trusted
 * applications.
 *
 * The options have been simplified to allow:
 *
 * - Sign messages with the owned private key
 * - Verify signatures coming from trusted applications (trusted public keys)
 * - Encode messages with the public key of a trusted application
 * - Decode messages that have been encoded with the owned public key
 *
 * This class is thread safe
 */
public class RSACrypto
{
    /** Decoder to decode messages encoded with the own public key */
    private final CipherWrapper ownPrivKeyDecoder;
    /** Signer that will sign messages with the own private key */
    private final SignerWrapper ownSigner;
    /** Map with all the verifiers for messages signed with private keys of trusted applications */
    private final Map<Integer, VerifierWrapper> verifiersBySecurityId = new HashMap<>();
    /** Map with all the encoders to encode messages with the public keys of trusted applications */
    private final Map<Integer, CipherWrapper> encodersBySecurityId = new HashMap<>();

    /**
     * Create a new instance of the RSA cryptography helper class
     *
     * @param ownPrivate owned private key
     * @param trustedKeys map with all the trusted keys by application id
     *
     * @throws VegaException exception thrown if there is any issue with the private key
     */
    public RSACrypto(final PrivateKey ownPrivate, final Map<Integer, PublicKey> trustedKeys) throws VegaException
    {
        // Create the decoder to decode messages encoded with the own public key
        this.ownPrivKeyDecoder = new CipherWrapper(CipherWrapper.CipherMode.DECRYPT, CipherCodecType.RSA, ownPrivate);

        // Initialize the signer to perform own digital signatures
        this.ownSigner = new SignerWrapper(SignatureCodecType.SHA_WITH_RSA, ownPrivate);

        // Register the trusted keys
        for (final Map.Entry<Integer, PublicKey> keyEntry : trustedKeys.entrySet())
        {
            this.registerTrustedKey(keyEntry.getKey(), keyEntry.getValue());
        }
    }

    /**
     * Check if the application public key has been registered
     * @param securityId the security id of hte application to register
     * @return true if it is already registered
     */
    public boolean isSecurityIdRegistered(final int securityId)
    {
        return this.encodersBySecurityId.containsKey(securityId);
    }

    /**
     * Register a trusted application and it's public key
     * @param securityId the security id of the application to register
     * @param key the public key of the application, max allowed size 128 bits
     * @throws VegaException exception thrown if the application is already registered or if there is a problem with the key
     */
    private void registerTrustedKey(final int securityId, final PublicKey key) throws VegaException
    {
        if (this.isSecurityIdRegistered(securityId))
        {
            throw new VegaException("The provided application ID has already been added: " + securityId);
        }

        this.encodersBySecurityId.put(securityId, new CipherWrapper(CipherWrapper.CipherMode.ENCRYPT, CipherCodecType.RSA, key));
        this.verifiersBySecurityId.put(securityId, new VerifierWrapper(SignatureCodecType.SHA_WITH_RSA, key));
    }

    /**
     * Encode the given message with the public key of the given application. It has to be already registered
     * @param securityId the security id of the application to register
     * @param msg the message to encode
     * @return the encoded message
     * @throws VegaException exception thrown if the application is not registered or the message cannot be encoded for any reason
     */
    public byte[] encodeWithPubKey(final int securityId, final byte[] msg) throws VegaException
    {
        final CipherWrapper cipher = this.encodersBySecurityId.get(securityId);

        if (cipher == null)
        {
            throw new VegaException("Cannot find any public key for giving application -> " + securityId);
        }

        return cipher.runCipher(msg);
    }

    /**
     * Decode the given message using our own private key
     * @param msg the message to decode
     * @return the decoded message
     * @throws VegaException exception thrown if there is any problem
     */
    public byte[] decodeWithOwnPrivKey(final byte[] msg) throws VegaException
    {
        return this.ownPrivKeyDecoder.runCipher(msg);
    }

    /**
     * Sign the given message with the owned private key
     * @param msg the message to sign
     * @return the signature of the message
     * @throws VegaException exception thrown if there is any problem performing the message signature
     */
    public byte[] sign(final byte[] msg) throws VegaException
    {
        return this.ownSigner.sign(msg);
    }

    /**
     * Verify the given signature for the message given the application ID of the application that has signed the message.
     * It has to be a registered application or it won't be able to verify the message signature
     *
     * @param securityId the security id
     * @param signature the signature of the message
     * @param signedMessage the message that has been signed
     * @return true if the signature is trusted
     * @throws VegaException exception thrown if the application is not trusted or if there is any problem trying to decode the signature
     */
    public boolean verifySignature(final int securityId, final byte[] signature, final byte[] signedMessage) throws VegaException
    {
        // Get the verifier for the application ID
        final VerifierWrapper verifier = this.verifiersBySecurityId.get(securityId);

        if (verifier == null)
        {
            throw new VegaException("The given application ID is not registered: " + securityId);
        }

        // Now verify the signature
        return verifier.verifySignature(signature, signedMessage);
    }
}