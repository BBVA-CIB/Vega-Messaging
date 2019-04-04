package com.bbva.kyof.vega.util.crypto;

import com.bbva.kyof.vega.exception.VegaException;

import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;

/**
 * Helper class to handle the loading, store and creation of RSA keys
 */
public final class RSAKeysHelper
{
    /** Size of the supported RSA Key */
    private static final int RSA_DEFAULT_KEY_SIZE = 1024;

    /** Private constructor to avoid instantiation */
    private RSAKeysHelper()
    {
        // Nothing to do
    }

    /** Generate a new public/private key pair
     *
     * @param keySize the size for the generated key
     * @param rsaCodec the kind of RSA codec to use
     *
     * @return the generated key pair
     * @throws VegaException if there is any problem
     */
    public static KeyPair generateKeyPair(final int keySize, final String rsaCodec) throws VegaException
    {
        try
        {
            final KeyPairGenerator keyGen = KeyPairGenerator.getInstance(rsaCodec);
            keyGen.initialize(keySize);
            return keyGen.genKeyPair();
        }
        catch (final NoSuchAlgorithmException e)
        {
            throw new VegaException("Error generating key pair", e);
        }
    }

    /**
     * Generate a new public/private key pair using default key size and codec type
     * @return  the generated key pair
     * @throws VegaException if there is a problem
     */
    public static KeyPair generateKeyPair() throws VegaException
    {
        return generateKeyPair(RSA_DEFAULT_KEY_SIZE, CipherCodecType.RSA.getStringValue());
    }

    /**
     * Load the private key encoded in base64 and convert to an internal usable PrivateKey
     * @param key64 the key in Base64 format
     * @param rsaCodec the RSA Codec to use
     * @return the internal PrivateKey decoded
     * @throws VegaException exception thrown if there is any problem
     */
    public static PrivateKey loadPrivateKey(final String key64, final String rsaCodec) throws VegaException
    {
        try
        {
            byte[] clear = Base64.getDecoder().decode(key64);
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(clear);
            KeyFactory fact = KeyFactory.getInstance(rsaCodec);
            PrivateKey priv = fact.generatePrivate(keySpec);
            Arrays.fill(clear, (byte) 0);
            return priv;
        }
        catch (final InvalidKeySpecException | NoSuchAlgorithmException e)
        {
            throw new VegaException("Error loading private key", e);
        }
    }

    /**
     * Load the private key encoded in base64 and convert to an internal usable PrivateKey. It will
     * use the default RSA codec.
     *
     * @param key64 the key in Base64 format
     * @return the internal PrivateKey decoded
     * @throws VegaException exception thrown if there is any problem
     */
    public static PrivateKey loadPrivateKey(final String key64) throws VegaException
    {
        return loadPrivateKey(key64, CipherCodecType.RSA.getStringValue());
    }

    /**
     * Load the private key encoded in base64 and encrypted with AES 128 and convert to an internal usable PrivateKey. It will
     * use the default RSA codec.
     *
     * @param encryptedKey64 the key in Base64 format encrypted with AES 128
     * @param dissociatedKeyHexPassword the dissociated AES 128 key in hexadecimal format, an XOR will be performed against the library password
     * @return the internal PrivateKey decoded
     * @throws VegaException exception thrown if there is any problem
     */
    public static PrivateKey loadEncryptedPrivateKey(final String encryptedKey64, final String dissociatedKeyHexPassword) throws VegaException
    {
        return loadEncryptedPrivateKey(encryptedKey64, CipherCodecType.RSA.getStringValue(), dissociatedKeyHexPassword);
    }

    /**
     * Load the private key encoded in base64 and encrypted with AES 128 and convert to an internal usable PrivateKey. It will
     * use the default RSA codec.
     *
     * @param encryptedKey64 the key in Base64 format encrypted with AES 128
     * @param rsaCodec the RSA Codec to use
     * @param dissociatedKeyHexPassword the dissociated AES 128 key in hexadecimal format, an XOR will be performed against the library password
     * @return the internal PrivateKey decoded
     * @throws VegaException exception thrown if there is any problem
     */
    public static PrivateKey loadEncryptedPrivateKey(final String encryptedKey64, final String rsaCodec, final String dissociatedKeyHexPassword) throws VegaException
    {
        try
        {
            // Convert the encrypted key to binary
            byte[] encryptedKey = Base64.getDecoder().decode(encryptedKey64);

            // Decode the key
            byte[] decodedKey = EncryptedPrivKeyDecoder.decryptPrivKey(dissociatedKeyHexPassword, encryptedKey);

            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(decodedKey);
            KeyFactory fact = KeyFactory.getInstance(rsaCodec);
            PrivateKey priv = fact.generatePrivate(keySpec);
            Arrays.fill(decodedKey, (byte) 0);
            return priv;
        }
        catch (final InvalidKeySpecException | NoSuchAlgorithmException e)
        {
            throw new VegaException("Error decoding and loading encrypted private key", e);
        }
    }

    /**
     * Load the public key encoded in base64 and convert to an internal usable PublicKey
     * @param key64 the key in Base64 format
     * @param rsaCodec the RSA Codec to use
     * @return the internal PrivateKey decoded
     * @throws VegaException exception thrown if there is any problem
     */
    public static PublicKey loadPublicKey(final String key64, final String rsaCodec) throws VegaException
    {
        try
        {
            byte[] data = Base64.getDecoder().decode(key64);
            X509EncodedKeySpec spec = new X509EncodedKeySpec(data);
            KeyFactory fact = KeyFactory.getInstance(rsaCodec);
            return fact.generatePublic(spec);
        }
        catch (final InvalidKeySpecException | NoSuchAlgorithmException e)
        {
            throw new VegaException("Error loading public key", e);
        }
    }

    /**
     * Load the public key encoded in base64 and convert to an internal usable PublicKey.
     * It will use the default RSA codec.
     *
     * @param key64 the key in Base64 format
     * @return the internal PrivateKey decoded
     * @throws VegaException exception thrown if there is any problem
     */
    public static PublicKey loadPublicKey(final String key64) throws VegaException
    {
        return loadPublicKey(key64, CipherCodecType.RSA.getStringValue());
    }

    /**
     * Save the private key converting it into a base64 String
     * @param priv the key to be saved
     * @param rsaCodec the RSA Codec to use
     * @return the key saved in base64 String format
     * @throws VegaException exception thrown if there is any problem
     */
    public static String savePrivateKey(final PrivateKey priv, final String rsaCodec) throws VegaException
    {
        try
        {
            final KeyFactory fact = KeyFactory.getInstance(rsaCodec);
            final PKCS8EncodedKeySpec spec = fact.getKeySpec(priv, PKCS8EncodedKeySpec.class);
            final byte[] packed = spec.getEncoded();
            final String key64 = Base64.getEncoder().encodeToString(packed);

            Arrays.fill(packed, (byte) 0);
            return key64;
        }
        catch (final InvalidKeySpecException | NoSuchAlgorithmException e)
        {
            throw new VegaException("Error saving private key", e);
        }
    }

    /**
     * Save the private key converting it into a base64 String
     * @param priv the key to be saved
     * @param keyPassword password to use to encrypt the private key, 16 character Hex String
     * @param rsaCodec the RSA Codec to use
     * @return the encrypted key saved in base64 String format
     * @throws VegaException exception thrown if there is any problem
     */
    public static String saveEncryptedPrivateKey(final PrivateKey priv, final String keyPassword, final String rsaCodec) throws VegaException
    {
        try
        {
            final KeyFactory fact = KeyFactory.getInstance(rsaCodec);
            final PKCS8EncodedKeySpec spec = fact.getKeySpec(priv, PKCS8EncodedKeySpec.class);
            final byte[] packed = spec.getEncoded();

            // Now encrypt the key
            final byte[] encrypted = EncryptedPrivKeyDecoder.encryptPrivateKey(packed, keyPassword);

            final String key64 = Base64.getEncoder().encodeToString(encrypted);

            Arrays.fill(packed, (byte) 0);
            return key64;
        }
        catch (final InvalidKeySpecException | NoSuchAlgorithmException e)
        {
            throw new VegaException("Error saving private key", e);
        }
    }

    /**
     * Save the private key converting it into a base64 String
     * It will use the default RSA codec.
     *
     * @param priv the key to be saved
     * @return the key saved in base64 String format
     * @throws VegaException exception thrown if there is any problem
     */
    public static String savePrivateKey(final PrivateKey priv) throws VegaException
    {
        return savePrivateKey(priv, CipherCodecType.RSA.getStringValue());
    }

    /**
     * Save the private key converting it into a base64 String
     * It will use the default RSA codec.
     *
     * @param priv the key to be saved
     * @param keyPassword password to use to encrypt the private key, 16 character Hex String
     * @return the key saved in base64 String format
     * @throws VegaException exception thrown if there is any problem
     */
    public static String saveEncryptedPrivateKey(final PrivateKey priv, final String keyPassword) throws VegaException
    {
        return saveEncryptedPrivateKey(priv, keyPassword, CipherCodecType.RSA.getStringValue());
    }

    /**
     * Save the public key converting it into a base64 String
     * @param publ the key to be saved
     * @param rsaCodec the RSA Codec to use
     * @return the key saved in base64 String format
     * @throws VegaException exception thrown if there is any problem
     */
    public static String savePublicKey(final PublicKey publ, final String rsaCodec) throws VegaException
    {
        try
        {
            final KeyFactory fact = KeyFactory.getInstance(rsaCodec);
            final X509EncodedKeySpec spec = fact.getKeySpec(publ, X509EncodedKeySpec.class);
            return Base64.getEncoder().encodeToString(spec.getEncoded());
        }
        catch (final InvalidKeySpecException | NoSuchAlgorithmException e)
        {
            throw new VegaException("Error saving public key", e);
        }
    }

    /**
     * Save the public key converting it into a base64 String.
     * It will use the default RSA codec.
     *
     * @param publ the key to be saved
     * @return the key saved in base64 String format
     * @throws VegaException exception thrown if there is any problem
     */
    public static String savePublicKey(final PublicKey publ) throws VegaException
    {
        return savePublicKey(publ, CipherCodecType.RSA.getStringValue());
    }
}
