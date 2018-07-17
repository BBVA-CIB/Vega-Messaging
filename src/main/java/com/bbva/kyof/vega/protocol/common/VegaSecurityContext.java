package com.bbva.kyof.vega.protocol.common;

import com.bbva.kyof.vega.config.privkey.PrivateKeyConfig;
import com.bbva.kyof.vega.config.privkey.PrivateKeyConfigReader;
import com.bbva.kyof.vega.config.pubkey.PublicKeyConfig;
import com.bbva.kyof.vega.config.pubkey.PublicKeyConfigReader;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.crypto.RSACrypto;
import com.bbva.kyof.vega.util.crypto.RSAKeysHelper;
import lombok.Getter;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Security context containing the RSA Keys and security information for encrypted communication
 */
public class VegaSecurityContext
{
    /** Constant for security id when it is not secured */
    private static final int NON_SECURED_CONSTANT = 0;

    /** RSA Crypto helper class with all the keys */
    @Getter private RSACrypto rsaCrypto = null;

    /** Application security id */
    @Getter private int securityId = NON_SECURED_CONSTANT;

    /**
     *  Initialize the security information using the provided security params. It will ignore the call if there are no parameters.
     * @param securityParams the security parameters
     * @param topicSecurityIds security ids for all the secure configured topics
     * @throws VegaException exception thrown if there is a problem
     */
    VegaSecurityContext(final SecurityParams securityParams, final Set<Integer> topicSecurityIds) throws VegaException
    {
        // If there are no security parameters, the security id is the non secured constant
        if (securityParams == null)
        {
            this.securityId = NON_SECURED_CONSTANT;
            return;
        }

        this.securityId = securityParams.getSecurityId();

        // Load the keys depending on how the security is configured
        switch (securityParams.getKeySecurityType())
        {
            case PLAIN_KEY_FILE:
            case ENCRYPTED_KEY_FILE:
                this.loadKeysFromFile(securityParams, topicSecurityIds);
                break;
            case KEYSTORE:
                this.loadKeysFromKeystore(securityParams, topicSecurityIds);
                break;
            default:
                throw new VegaException("Unsupported security key type " + securityParams.getKeySecurityType());
        }
    }

    /**
     * Returns true if security parameters have been configured
     * @return Returns true if security parameters have been configured
     */
    public boolean hasSecurity()
    {
        return this.securityId != NON_SECURED_CONSTANT;
    }

    /**
     * Load the keys from the keystore
     *
     * @param securityParams the security parameters
     * @param topicSecurityIds the security id of all public keys to load
     * @throws VegaException exception thrown if the keys cannot be loaded
     */
    private void loadKeysFromKeystore(final SecurityParams securityParams, Set<Integer> topicSecurityIds) throws VegaException
    {
        // TODO implement the keystore key load
        throw new VegaException("Keystore load of keys is not supported");
    }

    /**
     * Load the keys from files
     *
     * @param securityParams the security parameters provided by the user
     * @param topicSecurityIds all the security ids of the secure configured topics
     * @throws VegaException exception thrown if the keys cannot be loaded
     */
    private void loadKeysFromFile(final SecurityParams securityParams, final Set<Integer> topicSecurityIds) throws VegaException
    {
        // Load public keys for all the security id's found in the configuration of the secure topics
        final Map<Integer, PublicKey> publicKeysConfigMap = this.loadPublicKeysFromFiles(
                securityParams.getPublicKeysDirPath(),
                topicSecurityIds);

        // Load the private key from file, it will decrypt it if required
        final PrivateKey privateKey = this.loadPrivateKeyFromFile(securityParams);

        // Create the helper RSA Crypto map
        this.rsaCrypto = new RSACrypto(privateKey, publicKeysConfigMap);
    }

    /**
     * Load the private key from the XML configuration file
     *
     * @param securityParams the security parameters provided by the user
     * @return the loaded key
     * @throws VegaException exception thrown if the key cannot be loaded for any reason
     */
    private PrivateKey loadPrivateKeyFromFile(final SecurityParams securityParams) throws VegaException
    {
        // Load private key configuration
        final PrivateKeyConfig privateKeyConfig = PrivateKeyConfigReader.readConfiguration(securityParams.getPrivateKeyDirPath(), securityParams.getSecurityId());

        // Make sure that the private key is encrypted if ENCRYPTED_KEY_FILE is used in the parameters
        if (securityParams.getKeySecurityType() == KeySecurityType.ENCRYPTED_KEY_FILE && !privateKeyConfig.isKeyEncrypted())
        {
            throw new VegaException("EncryptedKeyFile selected in the security parameters, but the private key file is not encrypted");
        }

        // Convert the private key depending on encryption parameter
        if (securityParams.getKeySecurityType() == KeySecurityType.ENCRYPTED_KEY_FILE)
        {
            return RSAKeysHelper.loadEncryptedPrivateKey(privateKeyConfig.getValue(), securityParams.getHexPrivateKeyPassword());
        }
        else
        {
            return RSAKeysHelper.loadPrivateKey(privateKeyConfig.getValue());
        }
    }

    /**
     * Load the public keys from the XML configuration files
     *
     * @param keysDir the directory where the keys are contained
     * @param securityIds the list of security id's for all the keys to load
     * @return the loaded keys in a map by security id
     * @throws VegaException exception thrown if the keys cannot be loaded for any reason or there is no key for a provided security id
     */
    private Map<Integer, PublicKey> loadPublicKeysFromFiles(final String keysDir, final Set<Integer> securityIds) throws VegaException
    {
        final Map<Integer, PublicKey> result = new HashMap<>();

        // For each security id provided look for the corresponding public key
        for(final Integer securityId : securityIds)
        {
            // Load key configuration
            final PublicKeyConfig publicKeyConfig = PublicKeyConfigReader.readConfiguration(keysDir, securityId);
            // Create and put the public RSA key
            result.put(securityId, RSAKeysHelper.loadPublicKey(publicKeyConfig.getValue()));
        }

        return result;
    }
}
