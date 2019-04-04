package com.bbva.kyof.vega.util.net;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;

/**
 *  * Class create to test {@link SubnetAddress}
 *
 * Created by cnebrera on 16/06/16.
 */
public class SubnetAddressTest
{
    private static final String TEST_ADDRESS_1_STRING = "192.168.1.0/24";
    private static final String TEST_ADDRESS_2_STRING = "192.168.1.0";
    private static final String TEST_ADDRESS_3_STRING = "192.168.1.0/32";
    private static final String TEST_ADDRESS_4_STRING = "192.168.1.4";
    private static final String TEST_ADDRESS_5_STRING = "192.168.2.4";
    private static final String TEST_INCORRECT_ADDRESS_STRING = "300.168.1.0/24";
    private static final String IP6_ADDRESS = "2001:0db8:85a3:0000:0000:8a2e:0370:7334";


    @Test
    public void testConstructors()
    {
        new SubnetAddress(TEST_ADDRESS_2_STRING, 24);
        new SubnetAddress(TEST_ADDRESS_3_STRING);
    }

    @Test
    public void correctAddress() throws Exception
    {
        final SubnetAddress correctAddress = new SubnetAddress(TEST_ADDRESS_1_STRING);

        Assert.assertEquals(correctAddress.toString(), TEST_ADDRESS_1_STRING);
        Assert.assertEquals(correctAddress.getMask(), 24);
        Assert.assertEquals(correctAddress.getIpAddres(), InetUtil.getInetAddressFromString(TEST_ADDRESS_2_STRING));

        final SubnetAddress correctAddressNoMask = new SubnetAddress(TEST_ADDRESS_2_STRING);

        Assert.assertEquals(correctAddressNoMask.toString(), TEST_ADDRESS_3_STRING);
        Assert.assertEquals(correctAddressNoMask.getMask(), 32);
        Assert.assertEquals(correctAddressNoMask.getIpAddres(), InetUtil.getInetAddressFromString(TEST_ADDRESS_2_STRING));
    }

    @Test(expected = IllegalArgumentException.class)
    public void incorrectAddressException() throws Exception
    {
        new SubnetAddress(TEST_INCORRECT_ADDRESS_STRING);
    }

    @Test
    public void checkIfMatch() throws Exception
    {
        final SubnetAddress address1 = new SubnetAddress(TEST_ADDRESS_1_STRING);

        Assert.assertTrue(address1.checkIfMatch(InetUtil.getInetAddressFromString(TEST_ADDRESS_4_STRING)));
        Assert.assertFalse(address1.checkIfMatch(InetUtil.getInetAddressFromString(TEST_ADDRESS_5_STRING)));

        // Test check if match with a non ip4 address
        Assert.assertFalse(address1.checkIfMatch(InetAddress.getByName(IP6_ADDRESS)));
    }
}