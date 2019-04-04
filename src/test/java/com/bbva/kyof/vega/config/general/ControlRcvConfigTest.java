package com.bbva.kyof.vega.config.general;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by cnebrera on 24/10/2016.
 */
public class ControlRcvConfigTest
{
    private ControlRcvConfig.ControlRcvConfigBuilder emptyBuilder;
    private ControlRcvConfig.ControlRcvConfigBuilder minimumBuilder;

    @Before
    public void setUp()
    {
        this.emptyBuilder = ControlRcvConfig.builder();
        this.minimumBuilder = ControlRcvConfig.builder();
    }

    @Test
    public void empyConstructor() throws Exception
    {
        new ControlRcvConfig();
    }

    @Test
    public void validateEmptyBuilder() throws Exception
    {
        // Validation should work, there are no compulsory parameters for this configuration
        this.emptyBuilder.build().completeAndValidateConfig();
    }

    @Test
    public void validateDefaultParams() throws Exception
    {
        final ControlRcvConfig config = this.minimumBuilder.build();
        config.completeAndValidateConfig();

        // Common parameters
        Assert.assertTrue(config.getMaxPort() == ControlRcvConfig.DEFAULT_MAX_PORT);
        Assert.assertTrue(config.getMinPort() == ControlRcvConfig.DEFAULT_MIN_PORT);
        Assert.assertTrue(config.getNumStreams() == ControlRcvConfig.DEFAULT_NUM_STREAMS);
        Assert.assertNotNull(config.getSubnetAddress());
    }
}