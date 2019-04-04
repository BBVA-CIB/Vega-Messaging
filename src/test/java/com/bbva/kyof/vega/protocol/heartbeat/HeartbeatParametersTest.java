package com.bbva.kyof.vega.protocol.heartbeat;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 05/10/2016.
 */
public class HeartbeatParametersTest
{
    @Test
    public void testGettersSettersDefault()
    {
        final HeartbeatParameters parameters = HeartbeatParameters.builder().build();

        // Check the default parameters
        Assert.assertTrue(parameters.getHeartbeatRate() == 1000);
        Assert.assertTrue(parameters.getHeartbeatTimeout() == 1000);
        Assert.assertTrue(parameters.getMaxClientConnChecks() == 3);

        System.out.println(parameters.toString());
    }

    @Test
    public void testGettersSetters()
    {
        final HeartbeatParameters parameters = HeartbeatParameters.builder().heartbeatRate(100).heartbeatTimeout(20).maxClientConnChecks(5).build();

        // Check the default parameters
        Assert.assertTrue(parameters.getHeartbeatRate() == 100);
        Assert.assertTrue(parameters.getHeartbeatTimeout() == 20);
        Assert.assertTrue(parameters.getMaxClientConnChecks() == 5);
    }
}