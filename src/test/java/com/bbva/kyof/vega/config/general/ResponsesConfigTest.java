package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by cnebrera on 01/08/16.
 */
public class ResponsesConfigTest
{
    private ResponsesConfig.ResponsesConfigBuilder emptyBuilder;
    private ResponsesConfig.ResponsesConfigBuilder minimumBuilder;

    @Before
    public void setUp()
    {
        this.emptyBuilder = ResponsesConfig.builder();
        this.minimumBuilder = ResponsesConfig.builder();
        this.minimumBuilder.rcvPoller("poller1");
    }

    @Test
    public void empyConstructor() throws Exception
    {
        new ResponsesConfig();
    }

    @Test(expected = VegaException.class)
    public void validateEmptyBuilder() throws Exception
    {
        // Validation should fail, it is missing compulsory param poller name
        this.emptyBuilder.build().completeAndValidateConfig();
    }

    @Test
    public void validateDefaultParams() throws Exception
    {
        final ResponsesConfig config = this.minimumBuilder.build();
        config.completeAndValidateConfig();

        // Common parameters
        Assert.assertEquals(config.getRcvPoller(), "poller1");
        Assert.assertTrue(config.getMaxPort() == ResponsesConfig.DEFAULT_MAX_PORT);
        Assert.assertTrue(config.getMinPort() == ResponsesConfig.DEFAULT_MIN_PORT);
        Assert.assertTrue(config.getNumStreams() == ResponsesConfig.DEFAULT_NUM_STREAMS);
        Assert.assertNotNull(config.getSubnetAddress());
    }
}