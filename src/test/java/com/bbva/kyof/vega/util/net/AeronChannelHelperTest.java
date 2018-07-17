package com.bbva.kyof.vega.util.net;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import org.junit.AfterClass;

/**
 * Created by cnebrera on 11/08/16.
 */
public class AeronChannelHelperTest
{

    @AfterClass
    public static void afterClass() throws Exception
    {
        Field reliable = AeronChannelHelper.class.getDeclaredField("reliable");
        reliable.setAccessible(true);
        reliable.setBoolean(null, true);
    }
    @Test
    public void createIpcChannelString()
    {
        Assert.assertEquals(AeronChannelHelper.createIpcChannelString(), "aeron:ipc");
    }

    @Test
    public void createMulticastChannelString() throws Exception
    {
        final SubnetAddress subnetAddress = InetUtil.getDefaultSubnet();

        if (subnetAddress == null)
        {
            Assert.fail("No default subnet address found");
        }

        final int ip = InetUtil.convertIpAddressToInt("192.68.1.1");
        final String multicastChannel = AeronChannelHelper.createMulticastChannelString(ip, 35001, subnetAddress);

        Field reliable = AeronChannelHelper.class.getDeclaredField("reliable");
        reliable.setAccessible(true);
        reliable.setBoolean(null, true);

        Assert.assertEquals(multicastChannel, "aeron:udp?endpoint=192.68.1.1:35001|interface=" + subnetAddress.toString());

        final String multicastChannel2 = AeronChannelHelper.createMulticastChannelString("192.68.1.1", 35001, subnetAddress);
        Assert.assertEquals(multicastChannel2, "aeron:udp?endpoint=192.68.1.1:35001|interface=" + subnetAddress.toString());

        reliable.setBoolean(null, false);

        final String multicastChannel3 = AeronChannelHelper.createMulticastChannelString(ip, 35001, subnetAddress);
        final String multicastChannel4 = AeronChannelHelper.createMulticastChannelString(ip, 35001, subnetAddress);


        Assert.assertEquals(multicastChannel3, "aeron:udp?endpoint=192.68.1.1:35001|interface=" + subnetAddress.toString()+"|reliable=false");
        Assert.assertEquals(multicastChannel4, "aeron:udp?endpoint=192.68.1.1:35001|interface=" + subnetAddress.toString()+"|reliable=false");
    }

    @Test
    public void createUnicastChannelString() throws Exception
    {
        final SubnetAddress subnetAddress = InetUtil.getDefaultSubnet();

        if (subnetAddress == null)
        {
            Assert.fail("No default subnet address found");
        }

        final int ip = InetUtil.convertIpAddressToInt("192.68.1.1");

        Field reliable = AeronChannelHelper.class.getDeclaredField("reliable");
        reliable.setAccessible(true);
        reliable.setBoolean(null, true);

        final String unicastChannel = AeronChannelHelper.createUnicastChannelString(ip, 35001, subnetAddress);

        Assert.assertEquals(unicastChannel, "aeron:udp?endpoint=192.68.1.1:35001|interface=" + subnetAddress.toString());

        final String unicastChannel2 = AeronChannelHelper.createUnicastChannelString("192.68.1.1", 35001, subnetAddress);
        Assert.assertEquals(unicastChannel2, "aeron:udp?endpoint=192.68.1.1:35001|interface=" + subnetAddress.toString());

        reliable.setBoolean(null, false);

        final String unicastChannel3 = AeronChannelHelper.createUnicastChannelString(ip, 35001, subnetAddress);

        Assert.assertEquals(unicastChannel3, "aeron:udp?endpoint=192.68.1.1:35001|interface=" + subnetAddress.toString()+"|reliable=false");

        final String unicastChannel4 = AeronChannelHelper.createUnicastChannelString("192.68.1.1", 35001, subnetAddress);
        Assert.assertEquals(unicastChannel4, "aeron:udp?endpoint=192.68.1.1:35001|interface=" + subnetAddress.toString()+"|reliable=false");

    }


    @Test(expected = IllegalArgumentException.class)
    public void selectMcastIpFromWrongRange()
    {
        AeronChannelHelper.selectMcastIpFromRange("topic", "223.0.0.1", "223.0.0.1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void selectMcastIpFromWrongRang2()
    {
        AeronChannelHelper.selectMcastIpFromRange("topic", "223.0.0.2", "223.0.0.3");
    }

    @Test
    public void selectMcastIpFromRange()
    {
        // Try with diferent topic names
        boolean firstSelected = false;
        boolean secondSelected = false;
        boolean thridSelected = false;

        String selectedIp;
        for (int i = 0; i < 100; i++)
        {
            selectedIp = AeronChannelHelper.selectMcastIpFromRange("topic" + i, "223.0.0.1", "223.0.0.6");
            switch (selectedIp)
            {
                case "223.0.0.1":
                    firstSelected = true;
                    break;
                case "223.0.0.3":
                    secondSelected = true;
                    break;
                case "223.0.0.5":
                    thridSelected = true;
                    break;
            }
        }

        Assert.assertTrue(firstSelected && secondSelected && thridSelected);
    }

    @Test
    public void selectMcastIpFromRange2()
    {
        String selectedIp = AeronChannelHelper.selectMcastIpFromRange("topic", "223.0.0.1", "223.0.0.2");
        Assert.assertEquals("223.0.0.1", selectedIp);
    }

    @Test
    public void selectPortFromRange()
    {
        // Try with diferent topic names
        boolean firstSelected = false;
        boolean secondSelected = false;
        boolean thridSelected = false;

        int selectedPort;
        for (int i = 0; i < 100; i++)
        {
            selectedPort = AeronChannelHelper.selectPortFromRange("topic" + i, 1, 3);
            if (selectedPort == 1)
            {
                firstSelected = true;
            }
            else if (selectedPort == 2)
            {
                secondSelected = true;
            }
            else if (selectedPort == 3)
            {
                thridSelected = true;
            }
        }

        Assert.assertTrue(firstSelected && secondSelected && thridSelected);
    }

    @Test
    public void selectPortFromRange1()
    {
        int selectedPort = AeronChannelHelper.selectPortFromRange("topic", 1, 1);
        Assert.assertEquals(1, selectedPort);
    }

    @Test
    public void selectStreamFromRange()
    {
        // Try with diferent topic names
        boolean firstSelected = false;
        boolean secondSelected = false;
        boolean thridSelected = false;

        int selectedStream;
        for (int i = 0; i < 100; i++)
        {
            selectedStream = AeronChannelHelper.selectStreamFromRange("topic" + i, 3);
            if (selectedStream == 2)
            {
                firstSelected = true;
            }
            else if (selectedStream == 3)
            {
                secondSelected = true;
            }
            else if (selectedStream == 4)
            {
                thridSelected = true;
            }
            else
            {
                Assert.fail();
            }
        }

        Assert.assertTrue(firstSelected && secondSelected && thridSelected);
    }

    @Test
    public void selectStreamFromRange1()
    {
        int selectedStream = AeronChannelHelper.selectStreamFromRange("topic", 1);
        Assert.assertEquals(2, selectedStream);
    }

    @Test
    public void testConstructor() throws Exception
    {
        Constructor<?>[] cons = AeronChannelHelper.class.getDeclaredConstructors();
        cons[0].setAccessible(true);
        cons[0].newInstance((Object[]) null);
    }
}