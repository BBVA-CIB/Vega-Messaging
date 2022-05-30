package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.net.SubnetAddress;
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
    public void empyConstructor()
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
        Assert.assertEquals(ControlRcvConfig.DEFAULT_MAX_PORT, (int) config.getMaxPort());
        Assert.assertEquals(ControlRcvConfig.DEFAULT_MIN_PORT, (int) config.getMinPort());
        Assert.assertEquals(ControlRcvConfig.DEFAULT_NUM_STREAMS, (int) config.getNumStreams());
        Assert.assertNotNull(config.getSubnetAddress());
    }

    @Test
    public void testAlternativeUnicastHostname() throws VegaException
    {
        //by default...
        ControlRcvConfig config = minimumBuilder.build();

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