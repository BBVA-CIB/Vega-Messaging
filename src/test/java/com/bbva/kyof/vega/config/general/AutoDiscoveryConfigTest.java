package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.config.general.AutoDiscoType;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by cnebrera on 01/08/16.
 */
public class AutoDiscoveryConfigTest
{
    private AutoDiscoveryConfig.AutoDiscoveryConfigBuilder emptyBuilder;
    private AutoDiscoveryConfig.AutoDiscoveryConfigBuilder minimumMcastBuilder;
    private AutoDiscoveryConfig.AutoDiscoveryConfigBuilder minimumUcastBuilder;

    @Before
    public void setUp()
    {
        this.emptyBuilder = AutoDiscoveryConfig.builder();
        this.minimumMcastBuilder = AutoDiscoveryConfig.builder();
        this.minimumMcastBuilder.autoDiscoType(AutoDiscoType.MULTICAST);
        this.minimumUcastBuilder = AutoDiscoveryConfig.builder();
        this.minimumUcastBuilder.autoDiscoType(AutoDiscoType.UNICAST_DAEMON);
    }

    @Test
    public void empyConstructor() throws Exception
    {
        new AutoDiscoveryConfig();
    }

    @Test(expected = VegaException.class)
    public void validateEmptyBuilder() throws Exception
    {
        // Validation should fail, it is missing compulsory param auto discovery type
        this.emptyBuilder.build().completeAndValidateConfig();
    }

    @Test
    public void validateMcastMinBuilderAndTestDefaultParams() throws Exception
    {
        final AutoDiscoveryConfig config = this.minimumMcastBuilder.build();
        config.completeAndValidateConfig();

        // Common parameters
        Assert.assertEquals(config.getAutoDiscoType(), AutoDiscoType.MULTICAST);
        Assert.assertEquals(config.getDefaultStreamId(), AutoDiscoveryConfig.DEFAULT_STREAM_ID);
        Assert.assertTrue(config.getRefreshInterval() == AutoDiscoveryConfig.DEFAULT_REFRESH_INTERVAL);
        Assert.assertNotNull(config.getSubnetAddress());
        Assert.assertTrue(config.getTimeout() == AutoDiscoveryConfig.DEFAULT_EXPIRATION_TIMEOUT);

        // Multicast parameters
        Assert.assertEquals(config.getMulticastAddress(), AutoDiscoveryConfig.DEFAULT_MULTICAST_ADDRESS);
        Assert.assertTrue(config.getMulticastPort() == AutoDiscoveryConfig.DEFAULT_MULTICAST_PORT);

    }

    @Test(expected = VegaException.class)
    public void validateUnicastEmptyBuilder() throws Exception
    {
        // Validation should fail, it is missing compulsory params unicast daemon address
        this.minimumUcastBuilder.build().completeAndValidateConfig();
    }

    @Test
    public void validateUcastMinBuilderAndTestDefaultParams() throws Exception
    {
        // Set a daemon address to prevent it from failint
        this.minimumUcastBuilder.resolverDaemonAddress("192.168.1.1");
        final AutoDiscoveryConfig unicastConfig = this.minimumUcastBuilder.build();

        unicastConfig.completeAndValidateConfig();

        // Common parameters
        Assert.assertEquals(unicastConfig.getAutoDiscoType(), AutoDiscoType.UNICAST_DAEMON);
        Assert.assertEquals(unicastConfig.getDefaultStreamId(), AutoDiscoveryConfig.DEFAULT_STREAM_ID);
        Assert.assertTrue(unicastConfig.getRefreshInterval() == AutoDiscoveryConfig.DEFAULT_REFRESH_INTERVAL);
        Assert.assertNotNull(unicastConfig.getSubnetAddress());
        Assert.assertTrue(unicastConfig.getTimeout() == AutoDiscoveryConfig.DEFAULT_EXPIRATION_TIMEOUT);

        // Unicast parameters
        Assert.assertEquals(unicastConfig.getUnicastInfoArray().get(0).getResolverDaemonAddress(), "192.168.1.1");
        Assert.assertTrue(unicastConfig.getUnicastInfoArray().get(0).getResolverDaemonPort() == AutoDiscoveryConfig.DEFAULT_RESOLVER_DAEMON_PORT);
        Assert.assertTrue(unicastConfig.getUnicastResolverRcvNumStreams() == AutoDiscoveryConfig.DEFAULT_UNI_RSV_RCV_NUM_STREAMS);
        Assert.assertTrue(unicastConfig.getUnicastResolverRcvPortMax() == AutoDiscoveryConfig.DEFAULT_UNI_RSV_RCV_MAX_PORT);
        Assert.assertTrue(unicastConfig.getUnicastResolverRcvPortMin() == AutoDiscoveryConfig.DEFAULT_UNI_RSV_RCV_MIN_PORT);
    }
}