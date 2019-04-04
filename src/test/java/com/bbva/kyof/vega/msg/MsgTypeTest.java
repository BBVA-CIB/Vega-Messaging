package com.bbva.kyof.vega.msg;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;

/**
 * Created by cnebrera on 02/08/16.
 */
public class MsgTypeTest
{
    @Test
    public void testContructor() throws Exception
    {
        Constructor<?>[] cons = MsgType.class.getDeclaredConstructors();
        cons[0].setAccessible(true);
        cons[0].newInstance((Object[]) null);
    }

    @Test
    public void testToString()
    {
        Assert.assertEquals(MsgType.toString(MsgType.DATA), "DATA");
        Assert.assertEquals(MsgType.toString(MsgType.DATA_REQ), "DATA_REQ");
        Assert.assertEquals(MsgType.toString(MsgType.RESP), "RESP");
        Assert.assertEquals(MsgType.toString(MsgType.AUTO_DISC_TOPIC), "AUTO_DISC_TOPIC");
        Assert.assertEquals(MsgType.toString(MsgType.AUTO_DISC_TOPIC_SOCKET), "AUTO_DISC_TOPIC_SOCKET");
        Assert.assertEquals(MsgType.toString(MsgType.AUTO_DISC_INSTANCE), "AUTO_DISC_INSTANCE");
        Assert.assertEquals(MsgType.toString(MsgType.HEARTBEAT_REQ), "HEARTBEAT_REQ");
        Assert.assertEquals(MsgType.toString(MsgType.CONTROL_SECURITY_REQ), "CONTROL_SECURITY_REQ");
        Assert.assertEquals(MsgType.toString(MsgType.CONTROL_SECURITY_RESP), "CONTROL_SECURITY_RESP");
        Assert.assertEquals(MsgType.toString(MsgType.ENCRYPTED_DATA), "ENCRYPTED_DATA");
        Assert.assertEquals(MsgType.toString(MsgType.AUTO_DISC_DAEMON_CLIENT_INFO), "AUTO_DISC_DAEMON_CLIENT_INFO");
        Assert.assertEquals(MsgType.toString(MsgType.CONTROL_SECURITY_ERROR_RESP), "CONTROL_SECURITY_ERROR_RESP");
        Assert.assertEquals(MsgType.toString((byte)55), "UNKNOWN");
    }
}