package com.bbva.kyof.vega.protocol;

import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.protocol.common.KeySecurityType;
import com.bbva.kyof.vega.protocol.common.SecurityParams;
import com.bbva.kyof.vega.protocol.common.VegaInstanceParams;
import org.junit.Assert;

/**
 * Testing methods for the {@link VegaInstanceParams}
 *
 * Created by XE52727 on 23/05/2016.
 */
public class VegaInstanceParamsTest
{
    private static final String VALID_CONFIG = VegaInstanceParamsTest.class.getClassLoader().getResource("config/validConfiguration.xml").getPath();
    private static final String INVALID_CONFIG = VALID_CONFIG.replace("validConfiguration.xml", "akjdshaksjhdas.xml");
    private static final String KEYS_DIR_PATH = VegaInstanceParamsTest.class.getClassLoader().getResource("keys").getPath();

    @org.junit.Test
    public void testGetters() throws Exception
    {
        final VegaInstanceParams params = VegaInstanceParams.builder().instanceName("InstanceName").configurationFile(VALID_CONFIG).build();
        params.validateParams();

        Assert.assertEquals(params.getConfigurationFile(), VALID_CONFIG);
        Assert.assertEquals(params.getInstanceName(), "InstanceName");
        Assert.assertNotNull(params.toString());
    }

    @org.junit.Test
    public void testGettersWithSecurity() throws Exception
    {
        final SecurityParams securityParams = SecurityParams.builder().
                keySecurityType(KeySecurityType.PLAIN_KEY_FILE).
                securityId(11111).
                privateKeyDirPath(KEYS_DIR_PATH).
                publicKeysDirPath(KEYS_DIR_PATH).
                build();

        final VegaInstanceParams params = VegaInstanceParams.builder().instanceName("InstanceName").configurationFile(VALID_CONFIG).securityParams(securityParams).build();
        params.validateParams();

        Assert.assertEquals(params.getConfigurationFile(), VALID_CONFIG);
        Assert.assertEquals(params.getInstanceName(), "InstanceName");
        Assert.assertEquals(params.getSecurityParams(), securityParams);
        Assert.assertNotNull(params.toString());
    }

    @org.junit.Test(expected = VegaException.class)
    public void testNullAppName() throws Exception
    {
        final VegaInstanceParams params = VegaInstanceParams.builder().build();
        params.validateParams();
    }

    @org.junit.Test(expected = VegaException.class)
    public void testNullConfigFile() throws Exception
    {
        final VegaInstanceParams params = VegaInstanceParams.builder().instanceName("InstanceName").build();
        params.validateParams();
    }

    @org.junit.Test(expected = VegaException.class)
    public void testInvalidConfigFile() throws Exception
    {
        final VegaInstanceParams params = VegaInstanceParams.builder().instanceName("InstanceName").configurationFile(INVALID_CONFIG).build();
        params.validateParams();
    }

    @org.junit.Test(expected = VegaException.class)
    public void testInvalidSecurity() throws Exception
    {
        final SecurityParams securityParams = SecurityParams.builder().build();
        final VegaInstanceParams params = VegaInstanceParams.builder().instanceName("InstanceName").configurationFile(VALID_CONFIG).securityParams(securityParams).build();
        params.validateParams();
    }
}