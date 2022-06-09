package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.config.general.AutoDiscoType;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

/**
 * Created by cnebrera on 01/08/16.
 */
public class AutoDiscoveryConfigTest
{
    public static final String TEST_RESOLVER_DAEMON_ADDRESS = "192.168.1.1";
    public static final int TEST_RESOLVER_DAEMON_PORT = 37000;
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
    public void empyConstructor()
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
        Assert.assertEquals(AutoDiscoveryConfig.DEFAULT_REFRESH_INTERVAL, (long) config.getRefreshInterval());
        Assert.assertNotNull(config.getSubnetAddress());
        Assert.assertEquals(AutoDiscoveryConfig.DEFAULT_EXPIRATION_TIMEOUT, (long) config.getTimeout());

        // Multicast parameters
        Assert.assertEquals(config.getMulticastAddress(), AutoDiscoveryConfig.DEFAULT_MULTICAST_ADDRESS);
        Assert.assertEquals(AutoDiscoveryConfig.DEFAULT_MULTICAST_PORT, (int) config.getMulticastPort());

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
        this.minimumUcastBuilder.unicastInfoArray(Collections.singletonList(new UnicastInfo(TEST_RESOLVER_DAEMON_ADDRESS, TEST_RESOLVER_DAEMON_PORT)));
        final AutoDiscoveryConfig unicastConfig = this.minimumUcastBuilder.build();

        unicastConfig.completeAndValidateConfig();

        // Common parameters
        Assert.assertEquals(unicastConfig.getAutoDiscoType(), AutoDiscoType.UNICAST_DAEMON);
        Assert.assertEquals(unicastConfig.getDefaultStreamId(), AutoDiscoveryConfig.DEFAULT_STREAM_ID);
        Assert.assertEquals(AutoDiscoveryConfig.DEFAULT_REFRESH_INTERVAL, (long) unicastConfig.getRefreshInterval());
        Assert.assertNotNull(unicastConfig.getSubnetAddress());
        Assert.assertEquals(AutoDiscoveryConfig.DEFAULT_EXPIRATION_TIMEOUT, (long) unicastConfig.getTimeout());

        // Unicast parameters
        Assert.assertEquals(unicastConfig.getUnicastInfoArray().get(0).getResolverDaemonAddress(), TEST_RESOLVER_DAEMON_ADDRESS);
        Assert.assertEquals(TEST_RESOLVER_DAEMON_PORT, (int) unicastConfig.getUnicastInfoArray().get(0).getResolverDaemonPort());
        Assert.assertEquals(AutoDiscoveryConfig.DEFAULT_UNI_RSV_RCV_NUM_STREAMS, (int) unicastConfig.getUnicastResolverRcvNumStreams());
        Assert.assertEquals(AutoDiscoveryConfig.DEFAULT_UNI_RSV_RCV_MAX_PORT, (int) unicastConfig.getUnicastResolverRcvPortMax());
        Assert.assertEquals(AutoDiscoveryConfig.DEFAULT_UNI_RSV_RCV_MIN_PORT, (int) unicastConfig.getUnicastResolverRcvPortMin());
    }

    @Test
    public void testHostname() throws VegaException
    {
        //default hostname as EMPTY
        final AutoDiscoveryConfig.AutoDiscoveryConfigBuilder builder = AutoDiscoveryConfig.builder();
        builder.autoDiscoType(AutoDiscoType.UNICAST_DAEMON);
        builder.unicastInfoArray(Collections.singletonList(new UnicastInfo(TEST_RESOLVER_DAEMON_ADDRESS,37000)));

        AutoDiscoveryConfig config =  builder.build();
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
        builder.isResolveHostname(true);
        config =  builder.build();
        config.completeAndValidateConfig();
        Assert.assertEquals(resolved, config.getHostname());

        //by conf
        final String hostname = "test-autodisc-hostname";
        builder.hostname(hostname);
        config =  builder.build();
        Assert.assertEquals(hostname, config.getHostname());
    }
}