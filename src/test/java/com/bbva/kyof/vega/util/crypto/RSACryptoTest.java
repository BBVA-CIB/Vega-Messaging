package com.bbva.kyof.vega.util.crypto;

import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.security.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

/**
 * For these tests we will make use of 3 applications in order to simulate all scenarios:
 * - Application 1 and 2 know and trust each other.
 * - Application 3 only trust application 1 but application 1 don't trust application 3
 * - Application 2 trust application 3 but application 3 don´ trust application 2
 */
public class RSACryptoTest
{
    private static KeyPair KEY_PAIR_1;
    private static KeyPair KEY_PAIR_2;
    private static KeyPair KEY_PAIR_3;
    private static RSACrypto CRYPTO_APP_1;
    private static RSACrypto CRYPTO_APP_2;
    private static RSACrypto CRYPTO_APP_3;
    private static byte[] TEST_MSG = new byte[16];
    private static byte[] LONG_TEST_MSG = new byte[128];

    @BeforeClass
    public static void initialize() throws VegaException, NoSuchProviderException, NoSuchAlgorithmException
    {
        KEY_PAIR_1 = RSAKeysHelper.generateKeyPair();
        KEY_PAIR_2 = RSAKeysHelper.generateKeyPair();
        KEY_PAIR_3 = RSAKeysHelper.generateKeyPair();

        // RSA Crypto public keys map
        Map<Integer, PublicKey> publicKeysMap = new HashMap<>();
        publicKeysMap.put(2, KEY_PAIR_2.getPublic());

        // Now add the public keys to ensure the trusts in the applications following the Test Class description
        CRYPTO_APP_1 = new RSACrypto(KEY_PAIR_1.getPrivate(), publicKeysMap);

        publicKeysMap.clear();
        publicKeysMap.put(1, KEY_PAIR_1.getPublic());
        publicKeysMap.put(3, KEY_PAIR_3.getPublic());

        CRYPTO_APP_2 = new RSACrypto(KEY_PAIR_2.getPrivate(), publicKeysMap);

        publicKeysMap.clear();
        publicKeysMap.put(1, KEY_PAIR_1.getPublic());

        CRYPTO_APP_3 = new RSACrypto(KEY_PAIR_3.getPrivate(),publicKeysMap);

        // Create a test message to test encoding/decoding
        final Random rnd = new Random(System.currentTimeMillis());
        rnd.nextBytes(TEST_MSG);
    }

    @Test
    public void testIsPublicKeyRegistered() throws java.lang.Exception
    {
        Assert.assertTrue(CRYPTO_APP_1.isSecurityIdRegistered(2));
        Assert.assertFalse(CRYPTO_APP_1.isSecurityIdRegistered(3));
    }

    @Test
    public void testEncodeWithPubKey() throws java.lang.Exception
    {
        // This encoding should work
        CRYPTO_APP_1.encodeWithPubKey(2, TEST_MSG);
    }

    @Test(expected = VegaException.class)
    public void testEncodeWithPubKeyNonRegisteredApp() throws java.lang.Exception
    {
        // This encoding should fail since the public key is not registered
        CRYPTO_APP_1.encodeWithPubKey(3, TEST_MSG);
    }

    @Test(expected = VegaException.class)
    public void testEncodeWithPubKeyTooLongMessage() throws java.lang.Exception
    {
        // This test should fail because the message is too long
        CRYPTO_APP_1.encodeWithPubKey(2, LONG_TEST_MSG);
    }

    @Test
    public void testDecodeWithOwnPrivKey() throws java.lang.Exception
    {
        // Encode with APP 1 public key
        final byte[] encodedMessage = CRYPTO_APP_2.encodeWithPubKey(1, TEST_MSG);
        // Decode with APP 1 private key
        final byte[] decodedMessage = CRYPTO_APP_1.decodeWithOwnPrivKey(encodedMessage);
        // Check if they are the same
        assertArrayEquals(TEST_MSG, decodedMessage);
    }

    @Test(expected = VegaException.class)
    public void testDecodeWrongMessage() throws java.lang.Exception
    {
        CRYPTO_APP_1.decodeWithOwnPrivKey(TEST_MSG);
    }

    @Test
    public void testSign() throws java.lang.Exception
    {
        // This signature should work
        final byte[] signature = CRYPTO_APP_1.sign(TEST_MSG);
        // This signature should work as well
        final byte[] signature2 = CRYPTO_APP_1.sign(LONG_TEST_MSG);

        // Both signatures should have the same length
        Assert.assertEquals(signature.length, signature2.length);

        // The signatures should be different
        Assert.assertFalse(Arrays.equals(signature, signature2));
    }

    @Test
    public void testVerifySignature() throws java.lang.Exception
    {
        // Create several signatures
        final byte[] signature = CRYPTO_APP_1.sign(TEST_MSG);
        final byte[] signature2 = CRYPTO_APP_1.sign(LONG_TEST_MSG);
        final byte[] signature3 = CRYPTO_APP_2.sign(TEST_MSG);
        final byte[] signature4 = CRYPTO_APP_3.sign(LONG_TEST_MSG);

        // Check the signatures
        Assert.assertTrue(CRYPTO_APP_2.verifySignature(1, signature, TEST_MSG));
        Assert.assertTrue(CRYPTO_APP_2.verifySignature(1, signature2, LONG_TEST_MSG));

        // Now check invalid signatures
        Assert.assertFalse(CRYPTO_APP_2.verifySignature(1, signature4, TEST_MSG));
        Assert.assertFalse(CRYPTO_APP_2.verifySignature(1, signature3, LONG_TEST_MSG));
    }

    @Test(expected = VegaException.class)
    public void testVerifyUnknownAppSignature() throws java.lang.Exception
    {
        final byte[] signature4 = CRYPTO_APP_3.sign(LONG_TEST_MSG);
        Assert.assertTrue(CRYPTO_APP_2.verifySignature(5, signature4, TEST_MSG));
    }

    @Test
    public void testVerifyMalformedSignature() throws java.lang.Exception
    {
        Assert.assertFalse(CRYPTO_APP_2.verifySignature(1, TEST_MSG, LONG_TEST_MSG));
    }

    @Test
    public void testVerifyTooLongMalformedSignature() throws java.lang.Exception
    {
        Assert.assertFalse(CRYPTO_APP_2.verifySignature(1, LONG_TEST_MSG, LONG_TEST_MSG));
    }


}