package com.bbva.kyof.vega.config.general;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 01/08/16.
 */
public class AutoDiscoTypeTest
{
    @Test
    public void valueAndFromValue() throws Exception
    {
        Assert.assertEquals(AutoDiscoType.MULTICAST.value(), "MULTICAST");
        Assert.assertEquals(AutoDiscoType.UNICAST_DAEMON.value(), "UNICAST_DAEMON");
        Assert.assertEquals(AutoDiscoType.fromValue("MULTICAST"), AutoDiscoType.MULTICAST);
        Assert.assertEquals(AutoDiscoType.fromValue("UNICAST_DAEMON"), AutoDiscoType.UNICAST_DAEMON);
    }
}