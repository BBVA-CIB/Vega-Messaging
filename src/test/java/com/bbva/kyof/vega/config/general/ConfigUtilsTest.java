package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;

/**
 * Created by cnebrera on 01/08/16.
 */
public class ConfigUtilsTest
{
    @Test
    public void testConstructor() throws Exception
    {
        Constructor<?>[] cons = ConfigUtils.class.getDeclaredConstructors();
        cons[0].setAccessible(true);
        cons[0].newInstance((Object[]) null);
    }

    @Test(expected = VegaException.class)
    public void invalidPortRange() throws Exception
    {
        ConfigUtils.validatePortRange(56, 24);
    }

    @Test
    public void validPortRange() throws Exception
    {
        ConfigUtils.validatePortRange(56, 124);
    }

    @Test(expected = VegaException.class)
    public void invalidPortNumber() throws Exception
    {
        ConfigUtils.validatePortNumber(-56);
    }

    @Test(expected = VegaException.class)
    public void invalidPortNumber2() throws Exception
    {
        ConfigUtils.validatePortNumber(80000);
    }

    @Test
    public void validPortNumber() throws Exception
    {
        ConfigUtils.validatePortNumber(45678);
    }

    @Test
    public void validUnicastAddress() throws Exception
    {
        ConfigUtils.validateUnicastAddress("192.168.0.1");
    }

    @Test(expected = VegaException.class)
    public void invalidUnicastAddress() throws Exception
    {
        ConfigUtils.validateUnicastAddress("khjsdfjhks");
    }

    @Test(expected = VegaException.class)
    public void invalidateMulticastAddress() throws Exception
    {
        ConfigUtils.validateMulticastAddress("khjsdfjhks");
    }

    @Test(expected = VegaException.class)
    public void invalidateMulticastPairAddress() throws Exception
    {
        ConfigUtils.validateMulticastAddress("224.0.0.2");
    }

    @Test
    public void validMulticastAddress() throws Exception
    {
        ConfigUtils.validateMulticastAddress("224.0.0.1");
    }

    @Test(expected = VegaException.class)
    public void invalidateControlMulticastAddress() throws Exception
    {
        ConfigUtils.validateControlMulticastAddress("khjsdfjhks");
    }

    @Test(expected = VegaException.class)
    public void invalidateControlMulticastPairAddress() throws Exception
    {
        ConfigUtils.validateControlMulticastAddress("224.0.0.1");
    }

    @Test
    public void validControlMulticastAddress() throws Exception
    {
        ConfigUtils.validateControlMulticastAddress("224.0.0.2");
    }

    @Test(expected = VegaException.class)
    public void invalidMulticastAddressRange() throws Exception
    {
        ConfigUtils.validateMulticastAddressRange("224.0.0.7", "224.0.0.4");
    }

    @Test
    public void validMulticastAddressRange() throws Exception
    {
        ConfigUtils.validateMulticastAddressRange("224.0.0.3", "224.0.0.4");
    }

    @Test
    public void getFullMaskSubnetFromStringOrDefault() throws Exception
    {
        ConfigUtils.getFullMaskSubnetFromStringOrDefault(null);
    }

    @Test(expected = VegaException.class)
    public void getFullMaskSubnetFromStringOrDefaultMalformed() throws Exception
    {
        ConfigUtils.getFullMaskSubnetFromStringOrDefault("asdasdasd");
    }

    @Test(expected = VegaException.class)
    public void getFullMaskSubnetFromStringOrDefaultNonMatch() throws Exception
    {
        ConfigUtils.getFullMaskSubnetFromStringOrDefault("1.0.0.1/32");
    }

    @Test
    public void getFullMaskSubnetFromStringOrDefaultWithNotNull() throws Exception
    {
        // Get the default subnet
        final SubnetAddress subnet = InetUtil.getDefaultSubnet();

        // Change the mask
        final SubnetAddress newSubnet = new SubnetAddress(subnet.getIpAddres().getHostAddress(), 24);

        final SubnetAddress foundSubnet = ConfigUtils.getFullMaskSubnetFromStringOrDefault(newSubnet.toString());

        // It should be the original one
        Assert.assertEquals(subnet, foundSubnet);
    }
}