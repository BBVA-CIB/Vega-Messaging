package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 01/08/16.
 */
public class TopicTemplateConfigTest
{
    @Test
    public void empyConstructor()
    {
        new TopicTemplateConfig();
    }

    @Test(expected = VegaException.class)
    public void validateMissingName() throws Exception
    {
        // Should fail, it is missing the name
        final TopicTemplateConfig invalidConfig = TopicTemplateConfig.builder().build();
        invalidConfig.completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void validateMissingRcvPoller() throws Exception
    {
        // Should fail, it is missing the transport type
        final TopicTemplateConfig invalidConfig = TopicTemplateConfig.builder().name("aname").build();
        invalidConfig.completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void validateMissingTransportType() throws Exception
    {
        // Should fail, it is missing the transport type
        final TopicTemplateConfig invalidConfig = TopicTemplateConfig.builder().name("aname").rcvPoller("poller").build();
        invalidConfig.completeAndValidateConfig();
    }

    @Test
    public void testUnicastDefaultParams() throws Exception
    {
        final TopicTemplateConfig unicastConfig = TopicTemplateConfig.builder().
                name("aname").
                rcvPoller("poller").
                transportType(TransportMediaType.UNICAST).
                build();

        unicastConfig.completeAndValidateConfig();

        Assert.assertEquals(unicastConfig.getName(), "aname");
        Assert.assertEquals(unicastConfig.getRcvPoller(), "poller");
        Assert.assertEquals(unicastConfig.getTransportType(), TransportMediaType.UNICAST);
        Assert.assertEquals(TopicTemplateConfig.DEFAULT_UCAST_MIN_PORT, (int) unicastConfig.getMinPort());
        Assert.assertEquals(TopicTemplateConfig.DEFAULT_UCAST_MAX_PORT, (int) unicastConfig.getMaxPort());
        Assert.assertEquals(TopicTemplateConfig.DEFAULT_STREAMS_PER_PORT, (int) unicastConfig.getNumStreamsPerPort());
        Assert.assertNotNull(unicastConfig.getSubnetAddress());
    }

    @Test
    public void testMcastDefaultParams() throws Exception
    {
        final TopicTemplateConfig mcastConfig = TopicTemplateConfig.builder().
                name("aname").
                rcvPoller("poller").
                transportType(TransportMediaType.MULTICAST).
                build();

        mcastConfig.completeAndValidateConfig();

        Assert.assertEquals(mcastConfig.getName(), "aname");
        Assert.assertEquals(mcastConfig.getRcvPoller(), "poller");
        Assert.assertEquals(mcastConfig.getTransportType(), TransportMediaType.MULTICAST);
        Assert.assertEquals(mcastConfig.getMulticastAddressLow(), TopicTemplateConfig.DEFAULT_MULTICAST_LOW);
        Assert.assertEquals(mcastConfig.getMulticastAddressHigh(), TopicTemplateConfig.DEFAULT_MULTICAST_HIGH);
        Assert.assertEquals(TopicTemplateConfig.DEFAULT_MCAST_MIN_PORT, (int) mcastConfig.getMinPort());
        Assert.assertEquals(TopicTemplateConfig.DEFAULT_MCAST_MAX_PORT, (int) mcastConfig.getMaxPort());
        Assert.assertEquals(TopicTemplateConfig.DEFAULT_STREAMS_PER_PORT, (int) mcastConfig.getNumStreamsPerPort());
        Assert.assertNotNull(mcastConfig.getSubnetAddress());
    }

    @Test
    public void testAlternativeUnicastHostname() throws VegaException
    {
        TopicTemplateConfig.TopicTemplateConfigBuilder builder = new TopicTemplateConfig.TopicTemplateConfigBuilder();
        //by default...
        TopicTemplateConfig config = builder.name("aname").
                rcvPoller("poller").
                transportType(TransportMediaType.UNICAST).
                build();

        Assert.assertNull(config.getHostname());
        Assert.assertNull(config.getIsResolveHostname());
        config.completeAndValidateConfig();
        Assert.assertNotNull(config.getHostname());
        Assert.assertNotNull(config.getIsResolveHostname());
        Assert.assertEquals(ConfigUtils.EMPTY_HOSTNAME, config.getHostname());
        Assert.assertEquals(Boolean.FALSE, config.getIsResolveHostname());


        //resolved
        SubnetAddress subnetAddress = ConfigUtils.getFullMaskSubnetFromStringOrDefault(null);
        final String resolved = subnetAddress.getIpAddres().getHostName();
        builder.isResolveHostname(true);
        config = builder.build();
        config.completeAndValidateConfig();
        Assert.assertEquals(resolved, config.getHostname());

        //by conf
        final String hostname = "test-autodisc-hostname";
        builder.hostname(hostname);
        config = builder.build();
        Assert.assertEquals(hostname, config.getHostname());
    }
}