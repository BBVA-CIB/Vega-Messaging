package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.net.SubnetAddress;
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
    public void empyConstructor()
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
        Assert.assertEquals(ResponsesConfig.DEFAULT_MAX_PORT, (int) config.getMaxPort());
        Assert.assertEquals(ResponsesConfig.DEFAULT_MIN_PORT, (int) config.getMinPort());
        Assert.assertEquals(ResponsesConfig.DEFAULT_NUM_STREAMS, (int) config.getNumStreams());
        Assert.assertNotNull(config.getSubnetAddress());
    }

    @Test
    public void testAlternativeUnicastHostname() throws VegaException
    {
        //by default...
        ResponsesConfig config = minimumBuilder.build();

        Assert.assertNull(config.getHostname());
        Assert.assertNull(config.getIsResolveHostname());
        config.completeAndValidateConfig();
        Assert.assertNotNull(config.getHostname());
        Assert.assertNotNull(config.getIsResolveHostname());
        Assert.assertEquals(ConfigUtils.EMPTY_HOSTNAME, config.getHostname());
        Assert.assertEquals(Boolean.FALSE, config.getIsResolveHostname());


        //resolved
        SubnetAddress subnetAddress =  ConfigUtils.getFullMaskSubnetFromStringOrDefault(null);
        final String resolved = subnetAddress.getIpAddres().getHostName();
        minimumBuilder.isResolveHostname(true);
        config =  minimumBuilder.build();
        config.completeAndValidateConfig();
        Assert.assertEquals(resolved, config.getHostname());

        //by conf
        final String hostname = "test-autodisc-hostname";
        minimumBuilder.hostname(hostname);
        config =  minimumBuilder.build();
        Assert.assertEquals(hostname, config.getHostname());
    }
}