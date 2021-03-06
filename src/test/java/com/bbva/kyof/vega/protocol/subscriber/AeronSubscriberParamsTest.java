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
        final AeronSubscriberParams params1 = new AeronSubscriberParams(TransportMediaType.UNICAST, 5678, 34, 12, new SubnetAddress("192.168.1.1/24"));
        final AeronSubscriberParams params2 = new AeronSubscriberParams(TransportMediaType.UNICAST, 5678, 34, 12, new SubnetAddress("192.168.1.2/24"));
        final AeronSubscriberParams params3 = new AeronSubscriberParams(TransportMediaType.UNICAST, 5678, 34, 12, new SubnetAddress("192.168.1.1/24"));

        Assert.assertTrue(params1.getIpAddress() == 5678);
        Assert.assertTrue(params1.getStreamId() == 12);
        Assert.assertTrue(params1.getPort() == 34);
        Assert.assertTrue(params1.getTransportType() == TransportMediaType.UNICAST);
        Assert.assertEquals(params1.getSubnetAddress(), new SubnetAddress("192.168.1.1/24"));
        Assert.assertEquals(params1, params3);
        Assert.assertNotEquals(params1, params2);
        Assert.assertNotEquals(params1, null);
        Assert.assertNotEquals(params1, new Object());
        Assert.assertNotNull(params1.toString());
        Assert.assertTrue(params1.hashCode() == params3.hashCode());
        Assert.assertFalse(params1.hashCode() == params2.hashCode());
    }
}