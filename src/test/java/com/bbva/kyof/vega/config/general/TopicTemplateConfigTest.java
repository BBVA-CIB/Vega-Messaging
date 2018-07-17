package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 01/08/16.
 */
public class TopicTemplateConfigTest
{
    @Test
    public void empyConstructor() throws Exception
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
        Assert.assertTrue(unicastConfig.getMinPort() == TopicTemplateConfig.DEFAULT_UCAST_MIN_PORT);
        Assert.assertTrue(unicastConfig.getMaxPort() == TopicTemplateConfig.DEFAULT_UCAST_MAX_PORT);
        Assert.assertTrue(unicastConfig.getNumStreamsPerPort() == TopicTemplateConfig.DEFAULT_STREAMS_PER_PORT);
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
        Assert.assertTrue(mcastConfig.getMinPort() == TopicTemplateConfig.DEFAULT_MCAST_MIN_PORT);
        Assert.assertTrue(mcastConfig.getMaxPort() == TopicTemplateConfig.DEFAULT_MCAST_MAX_PORT);
        Assert.assertTrue(mcastConfig.getNumStreamsPerPort() == TopicTemplateConfig.DEFAULT_STREAMS_PER_PORT);
        Assert.assertNotNull(mcastConfig.getSubnetAddress());
    }
}