package com.bbva.kyof.vega.autodiscovery.sniffer;

import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.driver.MediaDriver;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 05/08/16.
 */
public class SnifferCommandLineParserTest
{
    private final static SubnetAddress SUBNET = InetUtil.getDefaultSubnet();

    @Test
    public void parseCommandLine() throws Exception
    {
        final MediaDriver mediaDriver = MediaDriver.launchEmbedded();
        final SubnetAddress subnetAddress = InetUtil.getDefaultSubnet();

        // Try with some parameters
        String[] VALID_CONFIG = new String[]{"-p", "35001", "-sn", SUBNET.toString(), "-t", "10000", "-ip", "225.0.0.1"};

        SnifferCommandLineParser parser = new SnifferCommandLineParser();
        SnifferParameters parameters = parser.parseCommandLine(VALID_CONFIG);

        Assert.assertEquals(parameters.getIpAddress(), SnifferParameters.DEFAULT_MULTICAST_ADDRESS);
        Assert.assertTrue(SnifferParameters.DEFAULT_STREAM_ID == 10);
        Assert.assertTrue(parameters.getPort() == 35001);
        Assert.assertEquals(parameters.getSubnetAddress(), subnetAddress);
        Assert.assertTrue(parameters.getTimeout() == 10000);

        // Try with the "long version"
        VALID_CONFIG = new String[]{"-port", "35001", "-subnet", SUBNET.toString(), "-timeout", "10000", "-ipAddress", "225.0.0.1"};

        parameters = parser.parseCommandLine(VALID_CONFIG);

        Assert.assertEquals(parameters.getIpAddress(), SnifferParameters.DEFAULT_MULTICAST_ADDRESS);
        Assert.assertTrue(SnifferParameters.DEFAULT_STREAM_ID == 10);
        Assert.assertTrue(parameters.getPort() == 35001);
        Assert.assertEquals(parameters.getSubnetAddress(), subnetAddress);
        Assert.assertTrue(parameters.getTimeout() == 10000);

        // Finally try with default parameters

        parameters = parser.parseCommandLine(new String [] {});

        Assert.assertEquals(parameters.getIpAddress(), SnifferParameters.DEFAULT_MULTICAST_ADDRESS);
        Assert.assertTrue(SnifferParameters.DEFAULT_STREAM_ID == 10);
        Assert.assertTrue(parameters.getPort() == SnifferParameters.DEFAULT_MULTICAST_PORT);
        Assert.assertEquals(parameters.getSubnetAddress(), subnetAddress);
        Assert.assertTrue(parameters.getTimeout() == SnifferParameters.DEFAULT_CLIENT_TIMEOUT);
        Assert.assertTrue(parameters.getIpAddress() == SnifferParameters.DEFAULT_MULTICAST_ADDRESS);

        parameters.toString();
    }

    @Test(expected = SnifferException.class)
    public void parseWrongCommandLine() throws Exception
    {
        // Try with wrong parameters
        String[] commandLine = new String[]{"-skljdfsdf", "35000", "-sn", "sdfsdfsd", "-t", "10000"};

        SnifferCommandLineParser parser = new SnifferCommandLineParser();
        parser.parseCommandLine(commandLine);
    }

    @Test(expected = SnifferException.class)
    public void parseWrongCommandLine2() throws Exception
    {
        final SubnetAddress subnetAddress = InetUtil.getDefaultSubnet();

        // Try with wrong parameters
        String[] commandLine = new String[]{"-p", "0", "-sn",subnetAddress.toString(), "-t", "10000"};

        SnifferCommandLineParser parser = new SnifferCommandLineParser();
        parser.parseCommandLine(commandLine);
    }

    @Test(expected = SnifferException.class)
    public void parseWrongCommandLine3() throws Exception
    {
        // Try with wrong parameters
        String [] commandLine = new String [] {"-p", "35000", "-sn", "1.1.1.1/32", "-t", "10000"};
        SnifferCommandLineParser parser = new SnifferCommandLineParser();
        parser.parseCommandLine(commandLine);
    }

    @Test(expected = SnifferException.class)
    public void parseWrongCommandLine4() throws Exception
    {
        // Try with wrong parameters
        String [] commandLine = new String [] {"-p", "35000", "-sn", "1.1.1.1/32", "-t", "0"};
        SnifferCommandLineParser parser = new SnifferCommandLineParser();
        parser.parseCommandLine(commandLine);
    }
}