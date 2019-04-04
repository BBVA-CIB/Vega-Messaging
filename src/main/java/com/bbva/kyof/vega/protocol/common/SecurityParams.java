package com.bbva.kyof.vega.protocol.common;

import com.bbva.kyof.vega.exception.VegaException;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * Parameters for the Vega messaging library initialization
 */
@Builder
@ToString
public class SecurityParams
{
    /** Security type to use (Plain key file, encrypted key file or keystore certificates) */
    @Getter private final KeySecurityType keySecurityType;

    /** Instance security id */
    @Getter private final Integer securityId;

    /** (Optional, only for file security) Path to the directory containing the instance private key for security connections */
    @Getter private final String privateKeyDirPath;

    /** (Optional, only for file security) Path to the file containing the public keys for security connections */
    @Getter private final String publicKeysDirPath;

    /** (Optional, only for ENCRYPTED_FILE security) Password to decrypt the private key contents.
     * The password should be a Hex result of a 128 bytes AES key. 32 HEX characters. It will be a dissociated password that
     * will be XOR combined with the Vega secret password for encrypted key file. */
    @Getter private final String hexPrivateKeyPassword;

    /**
     * Validate the parameters checking for compulsory parameters and verifying file access.
     *
     * @throws VegaException exception thrown if there is a problem with the parameters
     */
    void validateParams() throws VegaException
    {
        // Validate security type
        if (this.keySecurityType == null)
        {
            throw new VegaException("Missing required parameter [keySecurityType]");
        }

        // Validate security id
        if (this.securityId == null)
        {
            throw new VegaException("Missing required parameter [securityId]");
        }

        // Depending on the type validate different parameters
        switch (this.keySecurityType)
        {
            case PLAIN_KEY_FILE:
                this.validateKeyPaths();
                break;
            case ENCRYPTED_KEY_FILE:
                this.validateKeyPaths();
                this.validateHexPrivKeyPassword();
                break;
            case KEYSTORE:
                throw new VegaException("Keystore security is not supported yet");
            default:
                throw new VegaException("Invalid option for parameter [keySecurityType]");
        }
    }

    /**
     * Validate the hexadecimal string for the private key password
     * @throws VegaException exception thrown if there is a problem in the validation
     */
    private void validateHexPrivKeyPassword() throws VegaException
    {
        // Check that the parameter has been provided
        if (this.hexPrivateKeyPassword == null)
        {
            throw new VegaException("Missing required parameter [hexPrivateKeyPassword] for EncryptedKeyFile security type");
        }
    }

    /**
     * Make sure the directory paths are provided for the keys if key files are used
     * @throws VegaException exception thrown if the directories are not provided
     */
    private void validateKeyPaths() throws VegaException
    {
        // Validate privateKeyFilePath
        if (this.privateKeyDirPath == null)
        {
            throw new VegaException("Missing required parameter [privateKeyDirPath]");
        }

        // Validate publicKeysFilePath
        if (this.publicKeysDirPath == null)
        {
            throw new VegaException("Missing required parameter [publicKeysDirPath]");
        }
    }
}
