package com.bbva.kyof.vega.util.net;

import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.util.List;


/**
 * Class create to test {@link InetUtil}
 *
 * Created by XE52727 on 12/07/2016.
 */
public class InetUtilTest
{
    private static final String MULTICAST_ADDRESS_1_STRING = "224.168.0.1";
    private static final String TEST_ADDRESS_1_STRING = "192.168.0.1";
    private static final String TEST_ADDRESS_2_STRING = "192.172.4.255";
    private static final String TEST_ADDRESS_3_STRING = "233.168.1.255";
    private static final String INVALID_HOST_ADDRESS_STRING = "333.555.66.44";
    private static final String IP6_ADDRESS = "2001:0db8:85a3:0000:0000:8a2e:0370:7334";

    private static InetAddress TEST_ADDRESS_1;
    private static InetAddress TEST_ADDRESS_2;
    private static InetAddress TEST_ADDRESS_3;

    @BeforeClass
    public static void setUp() throws Exception
    {
        TEST_ADDRESS_1 = InetUtil.getInetAddressFromString(TEST_ADDRESS_1_STRING);
        TEST_ADDRESS_2 = InetUtil.getInetAddressFromString(TEST_ADDRESS_2_STRING);
        TEST_ADDRESS_3 = InetUtil.getInetAddressFromString(TEST_ADDRESS_3_STRING);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getAddressFromInvalidString() throws Exception
    {
        InetUtil.getInetAddressFromString(INVALID_HOST_ADDRESS_STRING);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getAddressFromIp6String() throws Exception
    {
        InetUtil.getInetAddressFromString(IP6_ADDRESS);
    }

    @Test
    public void checkValidAddressAndPort() throws Exception
    {
        // Multicast
        Assert.assertTrue(InetUtil.isValidMulticastAddress(MULTICAST_ADDRESS_1_STRING));
        Assert.assertFalse(InetUtil.isValidMulticastAddress(TEST_ADDRESS_1_STRING));
        Assert.assertFalse(InetUtil.isValidMulticastAddress(INVALID_HOST_ADDRESS_STRING));

        // Unicast
        Assert.assertFalse(InetUtil.isValidUnicastAddress(MULTICAST_ADDRESS_1_STRING));
        Assert.assertTrue(InetUtil.isValidUnicastAddress(TEST_ADDRESS_1_STRING));
        Assert.assertFalse(InetUtil.isValidUnicastAddress(INVALID_HOST_ADDRESS_STRING));

        // Port
        Assert.assertTrue(InetUtil.isValidPortNumber(12345));
        Assert.assertFalse(InetUtil.isValidPortNumber(70000));
        Assert.assertFalse(InetUtil.isValidPortNumber(-10));
        Assert.assertFalse(InetUtil.isValidPortNumber(0));
    }

    @Test
    public void intConvertTest() throws Exception
    {
        // Convert the addresses to Int
        int intAddress1 = InetUtil.convertIpAddressToInt(TEST_ADDRESS_1);
        int intAddress2 = InetUtil.convertIpAddressToInt(TEST_ADDRESS_2);
        int intAddress3 = InetUtil.convertIpAddressToInt(TEST_ADDRESS_3);

        // Convert back to InetAddress
        Assert.assertEquals(TEST_ADDRESS_1, InetUtil.convertIntToInetAddress(intAddress1));
        Assert.assertEquals(TEST_ADDRESS_2, InetUtil.convertIntToInetAddress(intAddress2));
        Assert.assertEquals(TEST_ADDRESS_3, InetUtil.convertIntToInetAddress(intAddress3));
    }

    @Test
    public void intConvertTestFromString() throws Exception
    {
        // Convert the addresses to Int from String
        int intAddress1 = InetUtil.convertIpAddressToInt(TEST_ADDRESS_1_STRING);
        int intAddress2 = InetUtil.convertIpAddressToInt(TEST_ADDRESS_2_STRING);
        int intAddress3 = InetUtil.convertIpAddressToInt(TEST_ADDRESS_3_STRING);

        // Convert back to InetAddress
        Assert.assertEquals(TEST_ADDRESS_1, InetUtil.convertIntToInetAddress(intAddress1));
        Assert.assertEquals(TEST_ADDRESS_2, InetUtil.convertIntToInetAddress(intAddress2));
        Assert.assertEquals(TEST_ADDRESS_3, InetUtil.convertIntToInetAddress(intAddress3));

        // Convert back to String address
        Assert.assertEquals(TEST_ADDRESS_1_STRING, InetUtil.convertIntToIpAddress(intAddress1));
        Assert.assertEquals(TEST_ADDRESS_2_STRING, InetUtil.convertIntToIpAddress(intAddress2));
        Assert.assertEquals(TEST_ADDRESS_3_STRING, InetUtil.convertIntToIpAddress(intAddress3));
    }

    @Test
    public void increaseDecreaseAddress() throws Exception
    {
        // Convert address to int
        int intAddress1 = InetUtil.convertIpAddressToInt("192.168.0.0");

        // Now increase it
        intAddress1 += 10;

        // And check
        Assert.assertEquals(InetUtil.getInetAddressFromString("192.168.0.10"), InetUtil.convertIntToInetAddress(intAddress1));

        // Now increase again
        intAddress1 += 245;

        // Check again
        Assert.assertEquals(InetUtil.getInetAddressFromString("192.168.0.255"), InetUtil.convertIntToInetAddress(intAddress1));

        // Increase again
        intAddress1 += 1;

        // Check again
        Assert.assertEquals(InetUtil.getInetAddressFromString("192.168.1.0"), InetUtil.convertIntToInetAddress(intAddress1));
    }


    @Test
    public void findFirstInterfaceAddressForSubnet() throws Exception
    {
        final InterfaceAddress result = InetUtil.getDefaultInterfaceAddress();

        Assert.assertNotNull(result);
        Assert.assertTrue(result.getAddress() instanceof Inet4Address);
    }

    @Test
    public void findFirstInterfaceAddressWithSubnet() throws Exception
    {
        // Create the subnet with the found interface
        final SubnetAddress validSubnet = this.createSubnetFromFirstInterface();

        // Find again using the subnet, it should be the same as if we look without subnet
        final InterfaceAddress resultWithSubnet = InetUtil.findFirstInterfaceAddressForSubnet(validSubnet);

        Assert.assertNotNull(resultWithSubnet);
        Assert.assertEquals(resultWithSubnet, InetUtil.findFirstInterfaceAddressForSubnet(validSubnet));

        // Create now a subnet that won't match any interface (using a multicast address)
        final SubnetAddress invalidSubnet = new SubnetAddress("224.10.9.7/32");

        // Find again using the subnet, it should be empty
        final InterfaceAddress resultWithInvalidSubnet = InetUtil.findFirstInterfaceAddressForSubnet(invalidSubnet);
        Assert.assertNull(resultWithInvalidSubnet);
    }

    @Test
    public void findAllInterfaceAddresses() throws Exception
    {
        final List<InterfaceAddress> result = InetUtil.getAllInterfaceAddresses();

        Assert.assertTrue(result.size() > 0);
        Assert.assertTrue(result.get(0).getAddress() instanceof Inet4Address);
        Assert.assertEquals(result.get(0).getAddress(), this.createSubnetFromFirstInterface().getIpAddres());
    }

    @Test
    public void findAllInterfaceAddressesWithSubnet() throws Exception
    {
        final SubnetAddress validSubnet = createSubnetFromFirstInterface();

        final List<InterfaceAddress> result = InetUtil.getAllInterfaceAddresses();

        Assert.assertTrue(result.size() > 0);
        Assert.assertTrue(result.get(0).getAddress() instanceof Inet4Address);
        Assert.assertEquals(result.get(0), InetUtil.findFirstInterfaceAddressForSubnet(validSubnet));

        // Now try with an invalid subnet
        final SubnetAddress invalidSubnet = new SubnetAddress("223.0.1.2/32");

        final InterfaceAddress invalidResult = InetUtil.findFirstInterfaceAddressForSubnet(invalidSubnet);
        Assert.assertNull(invalidResult);
    }

    @Test
    public void getDefaultSubnet() throws Exception
    {
        final SubnetAddress defaultSubnet = InetUtil.getDefaultSubnet();
        Assert.assertNotNull(defaultSubnet);
        Assert.assertTrue(defaultSubnet.getMask() == SubnetAddress.FULL_MASK);
        Assert.assertNotNull(defaultSubnet.getIpAddres());
    }

    @Test
    public void validateSubnetAndConvertToFullMask() throws Exception
    {
        final SubnetAddress defaultSubnet = InetUtil.getDefaultSubnet();
        final SubnetAddress convertedAddress = InetUtil.validateSubnetAndConvertToFullMask(defaultSubnet);

        // They should be the same since they are both 32 bit submasks
        Assert.assertEquals(defaultSubnet, convertedAddress);

        // Now try with something invalid
        Assert.assertNull(InetUtil.validateSubnetAndConvertToFullMask(new SubnetAddress("1.0.0.0/32")));
    }

    /** Create a valid subnetAddress from the first valid interface **/
    private SubnetAddress createSubnetFromFirstInterface() throws VegaException
    {
        // Get the first interface, we will use it to get the subnet
        final InterfaceAddress result = InetUtil.getDefaultInterfaceAddress();

        // Create the subnet with the found interface
        return new SubnetAddress(result.getAddress().getHostAddress() + "/" + result.getNetworkPrefixLength());
    }
}