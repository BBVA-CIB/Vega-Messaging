package com.bbva.kyof.vega.protocol.common;

import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 11/11/2016.
 */
public class VegaInstanceParamsTest
{
    /** File containing the configuration */
    private static final String validConfigFile = VegaInstanceParamsTest.class.getClassLoader().getResource("config/validConfiguration.xml").getPath();

    @Test
    public void testGettersSetters()
    {
        final SecurityParams securityParams = SecurityParams.builder().build();

        final VegaInstanceParams params = VegaInstanceParams.builder().instanceName("instance").
                configurationFile("config").
                securityParams(securityParams).
                unmanagedMediaDriver(null).build();

        Assert.assertEquals(params.getInstanceName(), "instance");
        Assert.assertEquals(params.getConfigurationFile(), "config");
        Assert.assertEquals(params.getSecurityParams(), securityParams);
        Assert.assertEquals(params.getUnmanagedMediaDriver(), null);

        params.toString();
    }

    @Test(expected = VegaException.class)
    public void testValidationFailDueToInstanceName() throws VegaException
    {
        final VegaInstanceParams params = VegaInstanceParams.builder().build();
        params.validateParams();
    }

    @Test(expected = VegaException.class)
    public void testValidationFailDueToConfig() throws VegaException
    {
        final VegaInstanceParams params = VegaInstanceParams.builder().instanceName("instanceName").build();
        params.validateParams();
    }

    @Test(expected = VegaException.class)
    public void testValidationFailDueToNotFoundConfig() throws VegaException
    {
        final VegaInstanceParams params = VegaInstanceParams.builder().instanceName("instanceName").configurationFile("no").build();
        params.validateParams();
    }

    @Test(expected = VegaException.class)
    public void testValidationFailDueToInvalidSecurityParams() throws VegaException
    {
        final VegaInstanceParams params = VegaInstanceParams.builder().
                instanceName("instanceName").
                configurationFile(validConfigFile)
                .securityParams(SecurityParams.builder().build()).build();
        params.validateParams();
    }

    @Test
    public void testValidationOk() throws VegaException
    {
        final VegaInstanceParams params = VegaInstanceParams.builder().
                instanceName("instanceName").
                configurationFile(validConfigFile)
                .build();
        params.validateParams();
    }
}