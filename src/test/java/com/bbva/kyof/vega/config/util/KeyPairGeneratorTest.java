package com.bbva.kyof.vega.config.util;

import com.bbva.kyof.vega.TestConstants;
import com.bbva.kyof.vega.config.privkey.PrivateKeyConfig;
import com.bbva.kyof.vega.config.privkey.PrivateKeyConfigReader;
import com.bbva.kyof.vega.config.pubkey.PublicKeyConfig;
import com.bbva.kyof.vega.config.pubkey.PublicKeyConfigReader;
import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 * Created by cnebrera on 10/10/2016.
 */
public class KeyPairGeneratorTest
{
    private static final String KEYS_DIR = "keys";
    private static final String DEST_DIR = "keygentest";

    private final static int APP_SECURITY_ID = 99999;
    private final static int ENCRYPTED_APP_SECURITY_ID = 88888;

    private String destPath;

    @Test
    public void testConstructor() throws Exception
    {
        Constructor<?>[] cons = KeyPairGenerator.class.getDeclaredConstructors();
        cons[0].setAccessible(true);
        cons[0].newInstance((Object[]) null);
    }

    @Before
    public void before()
    {
        // Get the paths, prepare directories and delete old files
        // Path to the testing key files
        final String keysPath = KeyPairGeneratorTest.class.getClassLoader().getResource(KEYS_DIR).getPath();

        // We want to create a new directory at the same level if it doesn't exists
        this.destPath = new File(keysPath).getParentFile().getAbsolutePath() + File.separator + DEST_DIR;

        // Create the directory if it doesn't exists
        final File destPathFile = new File(destPath);
        if (!destPathFile.exists())
        {
            destPathFile.mkdirs();
        }

        // Remove the keys if they already exists
        final String privKeyPath = PrivateKeyConfigReader.createPrivKeyFileFullPath(destPath, APP_SECURITY_ID);
        final String pubKeyPath = PublicKeyConfigReader.createPubKeyFileFullPath(destPath, APP_SECURITY_ID);
        final String encryptPrivKeyPath = PrivateKeyConfigReader.createPrivKeyFileFullPath(destPath, ENCRYPTED_APP_SECURITY_ID);
        final String encryptPubKeyPath = PublicKeyConfigReader.createPubKeyFileFullPath(destPath, ENCRYPTED_APP_SECURITY_ID);

        final File privKeyFile = new File(privKeyPath);
        final File pubKeyFile = new File(pubKeyPath);
        final File encryptPrivKeyFile = new File(encryptPrivKeyPath);
        final File encryptPubKeyFile = new File(encryptPubKeyPath);

        if (privKeyFile.exists())
        {
            privKeyFile.delete();
        }
        if (pubKeyFile.exists())
        {
            pubKeyFile.delete();
        }
        if (encryptPrivKeyFile.exists())
        {
            encryptPrivKeyFile.delete();
        }
        if (encryptPubKeyFile.exists())
        {
            encryptPubKeyFile.delete();
        }
    }

    @Test
    public void generateKeyPair() throws Exception
    {
        // Call main to generate the key pair
        KeyPairGenerator.main(new String[]{"PLAIN", Long.toString(APP_SECURITY_ID), destPath});
        this.loadAndVerifyGenFiles(APP_SECURITY_ID, false);
    }

    @Test
    public void generateEncryptedKeyPair() throws Exception
    {
        // Call main to generate the key pair
        KeyPairGenerator.main(new String[]{"ENCRYPTED", Long.toString(ENCRYPTED_APP_SECURITY_ID), destPath, TestConstants.PRIVATE_KEY_PASSWORD_HEX});
        this.loadAndVerifyGenFiles(ENCRYPTED_APP_SECURITY_ID, true);
    }

    @Test(expected = VegaException.class)
    public void testMainWrongParams() throws VegaException, IOException
    {
        final String[] args = new String[]{Long.toString(APP_SECURITY_ID)};
        KeyPairGenerator.main(args);
    }

    @Test(expected = VegaException.class)
    public void testMainWrongParams2() throws VegaException, IOException
    {
        final String[] args2 = new String[]{Long.toString(APP_SECURITY_ID), "PLAIN", destPath};
        KeyPairGenerator.main(args2);
    }

    @Test(expected = VegaException.class)
    public void testMainWrongParams3() throws VegaException, IOException
    {
        final String[] args2 = new String[]{"HELLO", "HELLO"};
        KeyPairGenerator.main(args2);
    }

    private void loadAndVerifyGenFiles(final long applicationId, final boolean isEncrypted) throws VegaException
    {
        // Now try to load the generated public key
        final PublicKeyConfig pubConfig = PublicKeyConfigReader.readConfiguration(destPath, applicationId);
        final PrivateKeyConfig privateConfig = PrivateKeyConfigReader.readConfiguration(destPath, applicationId);

        Assert.assertTrue(pubConfig.getAppSecurityId() == applicationId);
        Assert.assertNotNull(pubConfig.getValue());

        Assert.assertTrue(privateConfig.getAppSecurityId() == applicationId);
        Assert.assertNotNull(privateConfig.getValue());
        Assert.assertTrue(isEncrypted == privateConfig.isKeyEncrypted());
    }
}