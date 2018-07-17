package com.bbva.kyof.vega.protocol.common;

import com.bbva.kyof.vega.TestConstants;
import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;

/**
 * Created by cnebrera on 14/10/2016.
 */
public class SecurityParamsTest
{
    private static final String KEYS_DIR = SecurityParamsTest.class.getClassLoader().getResource("keys").getPath();

    private SecurityParams plainParams = null;
    private SecurityParams encryptedKeyParams = null;

    @org.junit.Before
    public void setUp() throws Exception
    {
        plainParams = SecurityParams.builder().keySecurityType(KeySecurityType.PLAIN_KEY_FILE).securityId(11111).privateKeyDirPath(KEYS_DIR).publicKeysDirPath(KEYS_DIR).build();
        plainParams.validateParams();

        encryptedKeyParams = SecurityParams.builder().keySecurityType(KeySecurityType.ENCRYPTED_KEY_FILE).securityId(11111).
                privateKeyDirPath(KEYS_DIR).
                publicKeysDirPath(KEYS_DIR).
                hexPrivateKeyPassword(TestConstants.PRIVATE_KEY_PASSWORD_HEX).build();
        encryptedKeyParams.validateParams();
    }

    @org.junit.Test(expected = VegaException.class)
    public void unsuportedKeystore() throws Exception
    {
        final SecurityParams keystoreKeyParams = SecurityParams.builder().keySecurityType(KeySecurityType.KEYSTORE).securityId(11111).privateKeyDirPath(KEYS_DIR).publicKeysDirPath(KEYS_DIR).build();
        keystoreKeyParams.validateParams();
    }

    @org.junit.Test
    public void checkToStringMethod() throws Exception
    {
        Assert.assertNotNull(plainParams.toString());
    }

    @org.junit.Test
    public void testGetters() throws Exception
    {
        Assert.assertEquals(plainParams.getKeySecurityType(), KeySecurityType.PLAIN_KEY_FILE);
        Assert.assertEquals(plainParams.getSecurityId(), new Integer(11111));
        Assert.assertEquals(plainParams.getPrivateKeyDirPath(), KEYS_DIR);
        Assert.assertEquals(plainParams.getPublicKeysDirPath(), KEYS_DIR);

        Assert.assertEquals(encryptedKeyParams.getKeySecurityType(), KeySecurityType.ENCRYPTED_KEY_FILE);
        Assert.assertEquals(encryptedKeyParams.getHexPrivateKeyPassword(), TestConstants.PRIVATE_KEY_PASSWORD_HEX);
    }

    @org.junit.Test(expected = VegaException.class)
    public void testWtongValidationNullSecId() throws Exception
    {
        final SecurityParams params = SecurityParams.builder().build();
        params.validateParams();
    }

    @org.junit.Test(expected = VegaException.class)
    public void testWtongValidationNullPrivKey() throws Exception
    {
        final SecurityParams params = SecurityParams.builder().keySecurityType(KeySecurityType.PLAIN_KEY_FILE).securityId(11111).build();
        params.validateParams();
    }

    @org.junit.Test(expected = VegaException.class)
    public void testWtongValidationNullPubKey() throws Exception
    {
        final SecurityParams params = SecurityParams.builder().keySecurityType(KeySecurityType.PLAIN_KEY_FILE).securityId(11111).privateKeyDirPath("path").build();
        params.validateParams();
    }

    @org.junit.Test(expected = VegaException.class)
    public void testMissingSecurityId() throws Exception
    {
        final SecurityParams params = SecurityParams.builder().keySecurityType(KeySecurityType.PLAIN_KEY_FILE).privateKeyDirPath(KEYS_DIR).publicKeysDirPath("WrongPath").build();
        params.validateParams();
    }

    @org.junit.Test(expected = VegaException.class)
    public void testMissingKeyPath1() throws Exception
    {
        final SecurityParams params = SecurityParams.builder().keySecurityType(KeySecurityType.PLAIN_KEY_FILE).securityId(11111).build();
        params.validateParams();
    }

    @org.junit.Test(expected = VegaException.class)
    public void testMissingKeyPath2() throws Exception
    {
        final SecurityParams params = SecurityParams.builder().keySecurityType(KeySecurityType.PLAIN_KEY_FILE).securityId(11111).privateKeyDirPath(KEYS_DIR).build();
        params.validateParams();
    }

    @org.junit.Test(expected = VegaException.class)
    public void testMissingHexKeyPassword() throws Exception
    {
        final SecurityParams params = SecurityParams.builder().keySecurityType(KeySecurityType.ENCRYPTED_KEY_FILE).securityId(11111).privateKeyDirPath(KEYS_DIR).publicKeysDirPath("WrongPath").build();
        params.validateParams();
    }
}