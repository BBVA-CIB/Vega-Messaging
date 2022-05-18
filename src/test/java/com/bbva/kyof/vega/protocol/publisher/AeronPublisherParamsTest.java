package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.TestConstants;
import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 11/08/16.
 */
public class AeronPublisherParamsTest
{
    @Test
    public void testMethods()
    {
        String hostname = "host_server_1";
        final AeronPublisherParams params1 = new AeronPublisherParams(TransportMediaType.UNICAST, 5678, 34, 12, new SubnetAddress("192.168.1.1/24"), hostname);
        final AeronPublisherParams params2 = new AeronPublisherParams(TransportMediaType.UNICAST, 5678, 34, 12, new SubnetAddress("192.168.1.2/24"), hostname);
        final AeronPublisherParams params3 = new AeronPublisherParams(TransportMediaType.UNICAST, 5678, 34, 12, new SubnetAddress("192.168.1.1/24"), hostname);

        Assert.assertEquals(5678, params1.getIpAddress());
        Assert.assertEquals(12, params1.getStreamId());
        Assert.assertEquals(34, params1.getPort());
        Assert.assertEquals( hostname, params1.getHostname());
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