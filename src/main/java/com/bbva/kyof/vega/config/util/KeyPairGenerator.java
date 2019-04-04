package com.bbva.kyof.vega.config.util;

import com.bbva.kyof.vega.config.privkey.PrivateKeyConfig;
import com.bbva.kyof.vega.config.privkey.PrivateKeyConfigReader;
import com.bbva.kyof.vega.config.pubkey.PublicKeyConfig;
import com.bbva.kyof.vega.config.pubkey.PublicKeyConfigReader;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.crypto.RSAKeysHelper;
import com.bbva.kyof.vega.util.file.FilePathUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.security.KeyPair;

/**
 * Helper class to generate key pairs and store them in file
 */
@Slf4j
public final class KeyPairGenerator
{
    /**
     * Private constructor to avoid instantiation
     */
    private KeyPairGenerator()
    {
        // Nothing to do
    }

    /**
     * Generator main method to execute externally
     *
     * @param args arguments to generate the pair, the first one is the application security ID and the second the target path
     * @throws VegaException if there is any problem generating the keys
     */
    public static void main(final String [] args) throws VegaException
    {
        if (args.length != 3 && args.length != 4)
        {
            log.error("Wrong number of arguments");
            printUsage();
            throw new VegaException("Wrong number of arguments");
        }

        try
        {
            // Get the key type
            final KeyType keyType = KeyType.valueOf(args[0]);

            // Get application id
            final int appSecurityId = Integer.valueOf(args[1]);

            // Get the destination path
            final String destDirPath = args[2];

            // Verify the directory path
            FilePathUtil.verifyDirPath(destDirPath);

            // Generate the key pair
            switch (keyType)
            {
                case PLAIN:
                    generatePlainKeyPair(appSecurityId, destDirPath);
                    break;
                case ENCRYPTED:
                    generateEncryptedKeyPair(appSecurityId, destDirPath, args[3]);
                    break;
                case CERTIFICATE:
                    throw new VegaException("Certificate key par generation is not supported");
                default:
                    throw new VegaException("Invalid key type provided");
            }
        }
        catch (final VegaException | RuntimeException | IOException e)
        {
            log.error("Error generating key pair", e);
            printUsage();
            throw new VegaException(e);
        }
    }

    /**
     * Print the usage of the generator
     */
    private static void printUsage()
    {
        log.info("3 Parameters required: keyType (PLAIN, ENCRYPTED, CERTIFICATE), application security id (long) and destination directory." +
                " If ENCRYPTED type is used, a forth parameter should be provided with " +
                "a 32 character HEX string with the private key encryption password if Ej: ENCRYPTED 26548769 /home/keys A456FH4263BC3451A456FH4263BC3451");
    }

    /**
     * Generate a key pair for the given instance id in the given destination path
     *
     * @param securityId the security id the key pair belongs to
     * @param destDir the destination directory path for the generation
     *
     * @throws VegaException if there is any problem
     */
    private static void generatePlainKeyPair(final int securityId, final String destDir) throws VegaException
    {
        log.info("Generating plain key pair for security id [{}] in directory [{}]", securityId, destDir);

        // Generate a key pair
        final KeyPair keyPair = RSAKeysHelper.generateKeyPair();

        // Convert the key pair to string
        final String pubKeyString = RSAKeysHelper.savePublicKey(keyPair.getPublic());
        final String privKeyString = RSAKeysHelper.savePrivateKey(keyPair.getPrivate());

        // Marshall the keys into XML
        marshallKeyPair(securityId, destDir, pubKeyString, privKeyString, false);
    }

    /**
     * Generate a key pair for the given instance id in the given destination path.
     * The private key will be encrypted using the given password.
     *
     * @param securityId the security id the key pair belongs to
     * @param destDir the destination directory path for the generation
     * @param keyPassword a 32 characters Hexadecimal String with the key password
     *
     *
     * @throws VegaException if there is any problem
     */
    private static void generateEncryptedKeyPair(final int securityId, final String destDir, final String keyPassword) throws VegaException
    {
        log.info("Generating encrypted key pair for security id [{}] in directory [{}] and key password [{}]", securityId, destDir, keyPassword);

        // Generate a key pair
        final KeyPair keyPair = RSAKeysHelper.generateKeyPair();

        // Convert the key pair to string
        final String pubKeyString = RSAKeysHelper.savePublicKey(keyPair.getPublic());
        final String privKeyString = RSAKeysHelper.saveEncryptedPrivateKey(keyPair.getPrivate(), keyPassword);

        // Marshall the keys into XML
        marshallKeyPair(securityId, destDir, pubKeyString, privKeyString, true);
    }

    /**
     * Marshall a key pair into their corresponding XML files
     *
     * @param securityId the security id of the aplication represented by the key pair
     * @param destDir the destination directory for the generation
     * @param pubKeyString the base64 string representing the public key
     * @param privKeyString the base64 string representing the private key
     * @param isEncrypted true if the private key is encrypted
     * @throws VegaException exception thrown if there is a problem
     */
    private static void marshallKeyPair(final int securityId,
                                final String destDir,
                                final String pubKeyString,
                                final String privKeyString,
                                final boolean isEncrypted) throws VegaException
    {
        // Create the classes that represent the 2 files
        final PublicKeyConfig publicKeyConfig = PublicKeyConfig.builder().
                appSecurityId(securityId).value(pubKeyString).build();

        final PrivateKeyConfig privateKeyConfig = PrivateKeyConfig.builder().
                appSecurityId(securityId).value(privKeyString).keyEncrypted(isEncrypted).build();

        // Marshall into XML both keys
        PrivateKeyConfigReader.marshallPrivKey(privateKeyConfig, destDir);
        PublicKeyConfigReader.marshallPubKey(publicKeyConfig, destDir);
    }

    /** Key type to generate (PLAIN, ENCRYPTED, CERTIFICATE) */
    enum KeyType
    {
        /** Plain XML key pair */
        PLAIN,
        /** Plain XML key pair with the private key Encrypted with AES 128 */
        ENCRYPTED,
        /** Certificate key generation */
        CERTIFICATE
    }
}
