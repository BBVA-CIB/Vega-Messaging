package com.bbva.kyof.vega.config.pubkey;

import com.bbva.kyof.vega.config.privkey.PrivateKeyConfigReaderTest;
import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Constructor;

/**
 * Created by cnebrera on 10/10/2016.
 */
public class PublicKeyConfigReaderTest
{
    private static final String KEYS_PATH = PrivateKeyConfigReaderTest.class.getClassLoader().getResource("keys").getPath();
    private static final String DEST_DIR = "publicKeyKeyGentest";

    private final static int APP_SECURITY_ID = 11111;

    private String destPath;

    @Before
    public void before()
    {
        // We want to create a new directory at the same level if it doesn't exists
        this.destPath = new File(KEYS_PATH).getParentFile().getAbsolutePath() + File.separator + DEST_DIR;

        // Create the directory if it doesn't exists
        final File destPathFile = new File(destPath);
        if (!destPathFile.exists())
        {
            destPathFile.mkdirs();
        }

        // Remove the keys if they already exists
        final String writeKeyFile = PublicKeyConfigReader.createPubKeyFileFullPath(destPath, APP_SECURITY_ID);

        final File pubKeyFile = new File(writeKeyFile);

        if (pubKeyFile.exists())
        {
            pubKeyFile.delete();
        }
    }

    @Test
    public void testConstructor() throws Exception
    {
        Constructor<?>[] cons = PublicKeyConfigReader.class.getDeclaredConstructors();
        cons[0].setAccessible(true);
        cons[0].newInstance((Object[]) null);
    }

    @Test
    public void readConfiguration() throws Exception
    {
        final PublicKeyConfig readedConfig1 = PublicKeyConfigReader.readConfiguration(KEYS_PATH, 11111);
        final PublicKeyConfig readedConfig2 = PublicKeyConfigReader.readConfiguration(KEYS_PATH, 22222);

        Assert.assertTrue(readedConfig1.getAppSecurityId() == 11111);
        Assert.assertTrue(readedConfig2.getAppSecurityId() == 22222);

        Assert.assertNotNull(readedConfig1.getValue());
        Assert.assertNotNull(readedConfig2.getValue());
    }

    @Test(expected = VegaException.class)
    public void readConfigurationNonExistingId() throws Exception
    {
        PublicKeyConfigReader.readConfiguration(KEYS_PATH, 12111);
    }

    @Test(expected = VegaException.class)
    public void readConfigurationWrongInternalIdInFile() throws Exception
    {
        PublicKeyConfigReader.readConfiguration(KEYS_PATH, 55555);
    }

    @Test
    public void readMarshall() throws Exception
    {
        final PublicKeyConfig readedConfig1 = PublicKeyConfigReader.readConfiguration(KEYS_PATH, APP_SECURITY_ID);

        PublicKeyConfigReader.marshallPubKey(readedConfig1, destPath);

        final PublicKeyConfig readedConfig2 = PublicKeyConfigReader.readConfiguration(destPath, APP_SECURITY_ID);

        Assert.assertTrue(readedConfig1.getAppSecurityId() == readedConfig2.getAppSecurityId());
        Assert.assertEquals(readedConfig1.getValue(), readedConfig2.getValue());
    }
}