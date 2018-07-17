package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.config.general.TransportMediaType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 01/08/16.
 */
public class TransportMediaTypeTest
{
    @Test
    public void valueAndFromValue() throws Exception
    {
        Assert.assertEquals(TransportMediaType.UNICAST.value(), "UNICAST");
        Assert.assertEquals(TransportMediaType.MULTICAST.value(), "MULTICAST");
        Assert.assertEquals(TransportMediaType.IPC.value(), "IPC");
        Assert.assertEquals(TransportMediaType.fromValue("UNICAST"), TransportMediaType.UNICAST);
        Assert.assertEquals(TransportMediaType.fromValue("MULTICAST"), TransportMediaType.MULTICAST);
        Assert.assertEquals(TransportMediaType.fromValue("IPC"), TransportMediaType.IPC);
    }
}