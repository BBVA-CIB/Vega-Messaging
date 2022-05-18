package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 11/08/16.
 */
public class AeronSubscriberParamsTest
{
    @Test
    public void testMethods()
    {
        final AeronSubscriberParams params1 = new AeronSubscriberParams(TransportMediaType.UNICAST, 5678, 34, 12, new SubnetAddress("192.168.1.1/24"),"subscriber_host_1");
        final AeronSubscriberParams params2 = new AeronSubscriberParams(TransportMediaType.UNICAST, 5678, 34, 12, new SubnetAddress("192.168.1.2/24"),"subscriber_host_1");
        final AeronSubscriberParams params3 = new AeronSubscriberParams(TransportMediaType.UNICAST, 5678, 34, 12, new SubnetAddress("192.168.1.1/24"),"subscriber_host_1");

        Assert.assertEquals(5678, params1.getIpAddress());
        Assert.assertEquals(12, params1.getStreamId());
        Assert.assertEquals(34, params1.getPort());
        Assert.assertEquals("subscriber_host_1", params1.getHostname());
        Assert.assertSame(params1.getTransportType(), TransportMediaType.UNICAST);
        Assert.assertEquals(params1.getSubnetAddress(), new SubnetAddress("192.168.1.1/24"));
        Assert.assertEquals(params1, params3);
        Assert.assertNotEquals(params1, params2);
        Assert.assertNotEquals(params1, null);
        Assert.assertNotEquals(params1, new Object());
        Assert.assertNotNull(params1.toString());
        Assert.assertEquals(params1.hashCode(), params3.hashCode());
        Assert.assertNotEquals(params1.hashCode(), params2.hashCode());
    }
}