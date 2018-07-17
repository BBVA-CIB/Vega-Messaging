package com.bbva.kyof.vega.util.crypto;

import com.bbva.kyof.vega.TestConstants;
import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.security.*;

/**
 * Created by cnebrera on 10/10/2016.
 */
public class RSAKeysHelperTest
{
    private static KeyPair KEY_PAIR_1;

    @BeforeClass
    public static void initialize() throws VegaException, NoSuchProviderException, NoSuchAlgorithmException
    {
        KEY_PAIR_1 = RSAKeysHelper.generateKeyPair();
    }

    @Test
    public void testConstructor() throws Exception
    {
        Constructor<?>[] cons = RSAKeysHelper.class.getDeclaredConstructors();
        cons[0].setAccessible(true);
        cons[0].newInstance((Object[]) null);
    }

    @Test
    public void testConvertKeyToString() throws Exception
    {
        String stringConvertedPublic = RSAKeysHelper.savePublicKey(KEY_PAIR_1.getPublic());
        String stringConvertedPrivate = RSAKeysHelper.savePrivateKey(KEY_PAIR_1.getPrivate());

        PublicKey publicKey = RSAKeysHelper.loadPublicKey(stringConvertedPublic);
        PrivateKey privateKey = RSAKeysHelper.loadPrivateKey(stringConvertedPrivate);
        Assert.assertEquals(publicKey, KEY_PAIR_1.getPublic());
        Assert.assertEquals(privateKey, KEY_PAIR_1.getPrivate());
    }

    @Test
    public void testConvertEncryptedKeyToString() throws Exception
    {
        final String encryptedKey = RSAKeysHelper.saveEncryptedPrivateKey(KEY_PAIR_1.getPrivate(), TestConstants.PRIVATE_KEY_PASSWORD_HEX);
        PrivateKey privateKey = RSAKeysHelper.loadEncryptedPrivateKey(encryptedKey, TestConstants.PRIVATE_KEY_PASSWORD_HEX);

        Assert.assertEquals(privateKey, KEY_PAIR_1.getPrivate());
    }

    @Test(expected = VegaException.class)
    public void testWrongKeyPairGen() throws Exception
    {
        RSAKeysHelper.generateKeyPair(128, "LOL");
    }

    @Test(expected = VegaException.class)
    public void testLoadPrivateKeyFail() throws Exception
    {
        RSAKeysHelper.loadPrivateKey("lololol");
    }

    @Test(expected = VegaException.class)
    public void testLoadPrivateKeyFail2() throws Exception
    {
        String stringConvertedPrivate = RSAKeysHelper.savePrivateKey(KEY_PAIR_1.getPrivate());
        RSAKeysHelper.loadPrivateKey(stringConvertedPrivate, "lol");
    }

    @Test(expected = VegaException.class)
    public void testLoadPublicKeyFail() throws Exception
    {
        RSAKeysHelper.loadPublicKey("lololol");
    }

    @Test(expected = VegaException.class)
    public void testLoadPublicKeyFail2() throws Exception
    {
        String stringConvertedPublic = RSAKeysHelper.savePublicKey(KEY_PAIR_1.getPublic());
        RSAKeysHelper.loadPublicKey(stringConvertedPublic, "lol");
    }

    @Test(expected = VegaException.class)
    public void testSavePrivateKeyFail() throws Exception
    {
        RSAKeysHelper.savePrivateKey(KEY_PAIR_1.getPrivate(), "lol");
    }

    @Test(expected = VegaException.class)
    public void testSavePublicKeyFail2() throws Exception
    {
        RSAKeysHelper.savePublicKey(KEY_PAIR_1.getPublic(), "lol");
    }
}