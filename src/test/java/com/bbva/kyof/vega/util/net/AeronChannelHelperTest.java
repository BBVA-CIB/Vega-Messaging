package com.bbva.kyof.vega.util.net;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;

/**
 * Created by cnebrera on 11/08/16.
 */
public class AeronChannelHelperTest
{
    @Test
    public void createIpcChannelString() throws Exception
    {
        Assert.assertEquals(AeronChannelHelper.createIpcChannelString(), "aeron:ipc");
    }

    @Test
    public void createMulticastChannelString() throws Exception
    {
        final SubnetAddress subnetAddress = InetUtil.getDefaultSubnet();
        final int ip = InetUtil.convertIpAddressToInt("192.68.1.1");
        final String multicastChannel = AeronChannelHelper.createMulticastChannelString(ip, 35001, subnetAddress);

        Assert.assertEquals(multicastChannel, "aeron:udp?endpoint=192.68.1.1:35001|interface=" + subnetAddress.toString());

        final String multicastChannel2 = AeronChannelHelper.createMulticastChannelString("192.68.1.1", 35001, subnetAddress);
        Assert.assertEquals(multicastChannel2, "aeron:udp?endpoint=192.68.1.1:35001|interface=" + subnetAddress.toString());
    }

    @Test
    public void createUnicastChannelString() throws Exception
    {
        final SubnetAddress subnetAddress = InetUtil.getDefaultSubnet();
        final int ip = InetUtil.convertIpAddressToInt("192.68.1.1");
        final String unicastChannel = AeronChannelHelper.createUnicastChannelString(ip, 35001, subnetAddress);

        Assert.assertEquals(unicastChannel, "aeron:udp?endpoint=192.68.1.1:35001|interface=" + subnetAddress.toString());

        final String unicastChannel2 = AeronChannelHelper.createUnicastChannelString("192.68.1.1", 35001, subnetAddress);
        Assert.assertEquals(unicastChannel2, "aeron:udp?endpoint=192.68.1.1:35001|interface=" + subnetAddress.toString());
    }


    @Test(expected = IllegalArgumentException.class)
    public void selectMcastIpFromWrongRange() throws Exception
    {
        AeronChannelHelper.selectMcastIpFromRange("topic", "223.0.0.1", "223.0.0.1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void selectMcastIpFromWrongRang2() throws Exception
    {
        AeronChannelHelper.selectMcastIpFromRange("topic", "223.0.0.2", "223.0.0.3");
    }

    @Test
    public void selectMcastIpFromRange() throws Exception
    {
        // Try with diferent topic names
        boolean firstSelected = false;
        boolean secondSelected = false;
        boolean thridSelected = false;

        String selectedIp;
        for (int i = 0; i < 100; i++)
        {
            selectedIp = AeronChannelHelper.selectMcastIpFromRange("topic" + i, "223.0.0.1", "223.0.0.6");
            if (selectedIp.equals("223.0.0.1"))
            {
                firstSelected = true;
            }
            else if (selectedIp.equals("223.0.0.3"))
            {
                secondSelected = true;
            }
            else if (selectedIp.equals("223.0.0.5"))
            {
               thridSelected = true;
            }
        }

        Assert.assertTrue(firstSelected && secondSelected && thridSelected);
    }

    @Test
    public void selectMcastIpFromRange2() throws Exception
    {
        String selectedIp = AeronChannelHelper.selectMcastIpFromRange("topic", "223.0.0.1", "223.0.0.2");
        Assert.assertTrue(selectedIp.equals("223.0.0.1"));
    }

    @Test
    public void selectPortFromRange() throws Exception
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
    public void selectPortFromRange1() throws Exception
    {
        int selectedPort = AeronChannelHelper.selectPortFromRange("topic", 1, 1);
        Assert.assertTrue(selectedPort == 1);
    }

    @Test
    public void selectStreamFromRange() throws Exception
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
                Assert.assertFalse(true);
            }
        }

        Assert.assertTrue(firstSelected && secondSelected && thridSelected);
    }

    @Test
    public void selectStreamFromRange1() throws Exception
    {
        int selectedStream = AeronChannelHelper.selectStreamFromRange("topic", 1);
        Assert.assertTrue(selectedStream == 2);
    }

    @Test
    public void testConstructor() throws Exception
    {
        Constructor<?>[] cons = AeronChannelHelper.class.getDeclaredConstructors();
        cons[0].setAccessible(true);
        cons[0].newInstance((Object[]) null);
    }
}