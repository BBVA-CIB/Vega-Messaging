package com.bbva.kyof.vega.protocol.common;

import com.bbva.kyof.vega.TestConstants;
import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by cnebrera on 11/08/16.
 */
public class VegaSecurityContextTest
{
    private static final String KEYS_DIR = SecurityParamsTest.class.getClassLoader().getResource("keys").getPath();

    private final Set<Integer> topicSecurityIds = new HashSet<>();
    private final Set<Integer> encryptedTopicSecurityIds = new HashSet<>();

    @Before
    public void before()
    {
        topicSecurityIds.add(11111);
        topicSecurityIds.add(22222);
        encryptedTopicSecurityIds.add(88888);
        encryptedTopicSecurityIds.add(99999);
    }

    @Test
    public void testNoSecurity() throws VegaException
    {
        final VegaSecurityContext securityContext = new VegaSecurityContext(null, null);
        Assert.assertFalse(securityContext.hasSecurity());
    }

    @Test
    public void testPlainSecurity() throws VegaException
    {
        final SecurityParams plainParams = SecurityParams.builder().
                keySecurityType(KeySecurityType.PLAIN_KEY_FILE).
                securityId(11111).
                privateKeyDirPath(KEYS_DIR).
                publicKeysDirPath(KEYS_DIR).build();

        plainParams.validateParams();

        final VegaSecurityContext securityContext = new VegaSecurityContext(plainParams, topicSecurityIds);

        Assert.assertTrue(securityContext.hasSecurity());
        Assert.assertTrue(securityContext.getSecurityId() == 11111);
        Assert.assertTrue(securityContext.getRsaCrypto().isSecurityIdRegistered(11111));
        Assert.assertTrue(securityContext.getRsaCrypto().isSecurityIdRegistered(22222));
        Assert.assertFalse(securityContext.getRsaCrypto().isSecurityIdRegistered(99999));
    }

    @Test
    public void testEncryptedSecurity() throws VegaException
    {
        final SecurityParams plainParams = SecurityParams.builder().
                keySecurityType(KeySecurityType.ENCRYPTED_KEY_FILE).
                securityId(88888).
                privateKeyDirPath(KEYS_DIR).
                publicKeysDirPath(KEYS_DIR).
                hexPrivateKeyPassword(TestConstants.PRIVATE_KEY_PASSWORD_HEX).build();

        plainParams.validateParams();

        final VegaSecurityContext securityContext = new VegaSecurityContext(plainParams, encryptedTopicSecurityIds);

        Assert.assertTrue(securityContext.hasSecurity());
        Assert.assertTrue(securityContext.getSecurityId() == 88888);
        Assert.assertTrue(securityContext.getRsaCrypto().isSecurityIdRegistered(88888));
        Assert.assertTrue(securityContext.getRsaCrypto().isSecurityIdRegistered(99999));
        Assert.assertFalse(securityContext.getRsaCrypto().isSecurityIdRegistered(11111));
    }

    @Test(expected = VegaException.class)
    public void testEncryptedSecurityWithNonEncryptedPrivKey() throws VegaException
    {
        final SecurityParams plainParams = SecurityParams.builder().
                keySecurityType(KeySecurityType.ENCRYPTED_KEY_FILE).
                securityId(11111).
                privateKeyDirPath(KEYS_DIR).
                publicKeysDirPath(KEYS_DIR).
                hexPrivateKeyPassword(TestConstants.PRIVATE_KEY_PASSWORD_HEX).build();

        plainParams.validateParams();

        VegaSecurityContext securityContext = new VegaSecurityContext(plainParams, encryptedTopicSecurityIds);
    }
}