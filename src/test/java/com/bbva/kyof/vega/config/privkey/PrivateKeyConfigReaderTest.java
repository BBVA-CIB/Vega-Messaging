package com.bbva.kyof.vega.config.privkey;

import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Constructor;

/**
 * Created by cnebrera on 10/10/2016.
 */
public class PrivateKeyConfigReaderTest
{
    private static final String PRIV_KEYS_PATH = PrivateKeyConfigReaderTest.class.getClassLoader().getResource("keys").getPath();
    private static final String DEST_DIR = "privateKeyKeyGentest";
    private final static int APP_SECURITY_ID = 11111;

    private String destPath;

    @Before
    public void before()
    {
        // We want to create a new directory at the same level if it doesn't exists
        this.destPath = new File(PRIV_KEYS_PATH).getParentFile().getAbsolutePath() + File.separator + DEST_DIR;

        // Create the directory if it doesn't exists
        final File destPathFile = new File(destPath);
        if (!destPathFile.exists())
        {
            destPathFile.mkdirs();
        }

        // Remove the keys if they already exists
        final String writeKeyFile = PrivateKeyConfigReader.createPrivKeyFileFullPath(destPath, APP_SECURITY_ID);

        final File privKeyFile = new File(writeKeyFile);

        if (privKeyFile.exists())
        {
            privKeyFile.delete();
        }
    }

    @Test
    public void testConstructor() throws Exception
    {
        Constructor<?>[] cons = PrivateKeyConfigReader.class.getDeclaredConstructors();
        cons[0].setAccessible(true);
        cons[0].newInstance((Object[]) null);
    }

    @Test
    public void readConfiguration() throws Exception
    {
        final PrivateKeyConfig readedConfig1 = PrivateKeyConfigReader.readConfiguration(PRIV_KEYS_PATH, 11111);
        final PrivateKeyConfig readedConfig2 = PrivateKeyConfigReader.readConfiguration(PRIV_KEYS_PATH, 22222);

        Assert.assertTrue(readedConfig1.getAppSecurityId() == 11111);
        Assert.assertTrue(readedConfig2.getAppSecurityId() == 22222);

        Assert.assertFalse(readedConfig1.isKeyEncrypted());
        Assert.assertFalse(readedConfig2.isKeyEncrypted());

        Assert.assertNotNull(readedConfig1.getValue());
        Assert.assertNotNull(readedConfig2.getValue());
    }

    @Test(expected = VegaException.class)
    public void readConfigurationNonExistingId() throws Exception
    {
        PrivateKeyConfigReader.readConfiguration(PRIV_KEYS_PATH, 12111);
    }

    @Test(expected = VegaException.class)
    public void readConfigurationWrongInternalIdInFile() throws Exception
    {
        PrivateKeyConfigReader.readConfiguration(PRIV_KEYS_PATH, 55555);
    }

    @Test
    public void readMarshall() throws Exception
    {
        final PrivateKeyConfig readedConfig1 = PrivateKeyConfigReader.readConfiguration(PRIV_KEYS_PATH, APP_SECURITY_ID);

        PrivateKeyConfigReader.marshallPrivKey(readedConfig1, destPath);

        final PrivateKeyConfig readedConfig2 = PrivateKeyConfigReader.readConfiguration(destPath, APP_SECURITY_ID);

        Assert.assertTrue(readedConfig1.getAppSecurityId() == readedConfig2.getAppSecurityId());
        Assert.assertEquals(readedConfig1.getValue(), readedConfig2.getValue());
    }
}