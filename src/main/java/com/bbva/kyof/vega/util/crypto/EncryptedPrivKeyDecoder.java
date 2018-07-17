package com.bbva.kyof.vega.util.crypto;

import com.bbva.kyof.vega.exception.VegaException;

import javax.xml.bind.DatatypeConverter;

/**
 * Helper class to encrypt and decrypt the encrypted private keys when they are cypher with AES 128 for security
 */
final class EncryptedPrivKeyDecoder
{
    /** Library part of the dissociated key for private keys encryption in hex */
    private final static String LIBRARY_DISSOCIATED_KEY_HEX = "C10A843B7E69FD25830BCA24590CAF72";

    /** Library part of the dissociated key for private keys encryption */
    private final static byte[] LIBRARY_DISSOCIATED_KEY = DatatypeConverter.parseHexBinary(LIBRARY_DISSOCIATED_KEY_HEX);

    /** Size of the hexadecimal string with the dissociated private key */
    private final static int HEX_PRIV_KEY_PASSWRD_STRING_SIZE = 32;

    /**
     * Private constructor to avoid instantiation of utility class
     */
    private EncryptedPrivKeyDecoder()
    {
        // Nothing to do
    }

    /**
     * Decrypt the private key contained in the given byte array
     * @param userPrivKeyPassword the user part of the dissociated private key password
     * @param encryptedKey the encrypted key we want to decrypt
     * @return the decrypted key
     * @throws VegaException exception thrown if there is any problem in the process
     */
    static byte[] decryptPrivKey(final String userPrivKeyPassword, final byte[] encryptedKey) throws VegaException
    {
        // Create the full key
        final byte[] fullKey = createFullKey(userPrivKeyPassword);

        // Create an AES Crypto for the key and decode it
        final AESCrypto aesCrypto = new AESCrypto(fullKey);
        return aesCrypto.decode(encryptedKey);
    }

    static byte[] encryptPrivateKey(byte[] key, String userPrivKeyPassword) throws VegaException
    {
        // Create the full key
        final byte[] fullKey = createFullKey(userPrivKeyPassword);

        // Create an AES Crypto for the key and encode it
        final AESCrypto aesCrypto = new AESCrypto(fullKey);
        return aesCrypto.encode(key);
    }

    /**
     * Create a full key from the 16 characteres Hexadecimal String with the first part of the dissociated password
     * @param userPrivKeyPassword the first part of the password
     * @return the full key in binary
     * @throws VegaException exception thrown if there is a problem
     */
    private static byte[] createFullKey(final String userPrivKeyPassword) throws VegaException
    {
        // First validate the provided password
        EncryptedPrivKeyDecoder.validateHexPrivKeyPassword(userPrivKeyPassword);

        // Convert the password to binary
        final byte[] userPassword = DatatypeConverter.parseHexBinary(userPrivKeyPassword);

        // Perform the XOR with the library password to obtain the full key
        return EncryptedPrivKeyDecoder.xor(LIBRARY_DISSOCIATED_KEY, userPassword);
    }

    /**
     * Validate the hexadecimal string for the private key password
     * @throws VegaException exception thrown if there is a problem in the validation
     */
    private static void validateHexPrivKeyPassword(final String hexPrivateKeyPassword) throws VegaException
    {
        // Now check that the provided String is an Hexadecimal string of the right length
        if (hexPrivateKeyPassword.length() != HEX_PRIV_KEY_PASSWRD_STRING_SIZE)
        {
            throw new VegaException("[hexPrivateKeyPassword] should have " + HEX_PRIV_KEY_PASSWRD_STRING_SIZE + " characters");
        }

        if (!hexPrivateKeyPassword.matches("-?[0-9a-fA-F]+"))
        {
            throw new VegaException("[hexPrivateKeyPassword] should be an Hexadecimal string value");
        }
    }

    /**
     * Perform an XOR between the given byte arrays
     *
     * @param firstArray first array for the XOR
     * @param secondArray second array for the XOR
     * @return the result of the XOR
     */
    private static byte[] xor(byte[] firstArray, byte[] secondArray)
    {
        byte[] result = new byte[firstArray.length];

        for (int i = 0; i < result.length; i++)
        {
            result[i] = (byte) (((int) firstArray[i]) ^ ((int) secondArray[i]));
        }

        return result;
    }
}
