package com.bbva.kyof.vega.autodiscovery.model;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 02/08/16.
 */
public class AutoDiscTransportTypeTest
{
    @Test
    public void valueOf()
    {
        Assert.assertEquals(AutoDiscTransportType.valueOf("PUB_IPC"), AutoDiscTransportType.PUB_IPC);
        Assert.assertEquals(AutoDiscTransportType.valueOf("PUB_MUL"), AutoDiscTransportType.PUB_MUL);
        Assert.assertEquals(AutoDiscTransportType.valueOf("PUB_UNI"), AutoDiscTransportType.PUB_UNI);
        Assert.assertEquals(AutoDiscTransportType.valueOf("SUB_IPC"), AutoDiscTransportType.SUB_IPC);
        Assert.assertEquals(AutoDiscTransportType.valueOf("SUB_UNI"), AutoDiscTransportType.SUB_UNI);
        Assert.assertEquals(AutoDiscTransportType.valueOf("SUB_MUL"), AutoDiscTransportType.SUB_MUL);
    }

    @Test
    public void isPubSub()
    {
        Assert.assertTrue(AutoDiscTransportType.PUB_IPC.isPublisher());
        Assert.assertTrue(AutoDiscTransportType.PUB_MUL.isPublisher());
        Assert.assertTrue(AutoDiscTransportType.PUB_UNI.isPublisher());
        Assert.assertFalse(AutoDiscTransportType.SUB_IPC.isPublisher());
        Assert.assertFalse(AutoDiscTransportType.SUB_MUL.isPublisher());
        Assert.assertFalse(AutoDiscTransportType.SUB_UNI.isPublisher());

        Assert.assertTrue(AutoDiscTransportType.SUB_IPC.isSubscriber());
        Assert.assertTrue(AutoDiscTransportType.SUB_MUL.isSubscriber());
        Assert.assertTrue(AutoDiscTransportType.SUB_UNI.isSubscriber());
        Assert.assertFalse(AutoDiscTransportType.PUB_IPC.isSubscriber());
        Assert.assertFalse(AutoDiscTransportType.PUB_MUL.isSubscriber());
        Assert.assertFalse(AutoDiscTransportType.PUB_UNI.isSubscriber());
    }

    @Test
    public void fromByteTest()
    {
        Assert.assertEquals(AutoDiscTransportType.fromByte(AutoDiscTransportType.PUB_IPC.getByteValue()), AutoDiscTransportType.PUB_IPC);
        Assert.assertEquals(AutoDiscTransportType.fromByte(AutoDiscTransportType.PUB_MUL.getByteValue()), AutoDiscTransportType.PUB_MUL);
        Assert.assertEquals(AutoDiscTransportType.fromByte(AutoDiscTransportType.PUB_UNI.getByteValue()), AutoDiscTransportType.PUB_UNI);
        Assert.assertEquals(AutoDiscTransportType.fromByte(AutoDiscTransportType.SUB_IPC.getByteValue()), AutoDiscTransportType.SUB_IPC);
        Assert.assertEquals(AutoDiscTransportType.fromByte(AutoDiscTransportType.SUB_MUL.getByteValue()), AutoDiscTransportType.SUB_MUL);
        Assert.assertEquals(AutoDiscTransportType.fromByte(AutoDiscTransportType.SUB_UNI.getByteValue()), AutoDiscTransportType.SUB_UNI);
        Assert.assertNull(AutoDiscTransportType.fromByte((byte)55));
    }
}