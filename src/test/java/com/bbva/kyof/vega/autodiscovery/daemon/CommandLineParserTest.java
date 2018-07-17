package com.bbva.kyof.vega.autodiscovery.daemon;

import com.bbva.kyof.vega.autodiscovery.exception.AutodiscException;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 05/08/16.
 */
public class CommandLineParserTest
{
    // Configuration
    private static final String EXTERNAL_DRIVER_DIR = CommandLineParserTest.class.getClassLoader().getResource("externalDriverDir").getPath();
    private static final String EMBEDDED_DRIVER_CONFIG = CommandLineParserTest.class.getClassLoader().getResource("driverConfig/config.prop").getPath();

    @Test
    public void parseCommandLine() throws Exception
    {
        final SubnetAddress subnetAddress = InetUtil.getDefaultSubnet();

        // Try with some parameters
        String [] commandLine = new String [] {"-p", "1400", "-sn", subnetAddress.toString(), "-ed", "-ct", "1000"};

        CommandLineParser parser = new CommandLineParser();
        DaemonParameters parameters = parser.parseCommandLine(commandLine);

        Assert.assertEquals(parameters.getIpAddress(), subnetAddress.getIpAddres().getHostAddress());
        Assert.assertTrue(DaemonParameters.DEFAULT_STREAM_ID == 10);
        Assert.assertTrue(parameters.getPort() == 1400);
        Assert.assertEquals(parameters.getSubnetAddress(), subnetAddress);
        Assert.assertEquals(parameters.getAeronDriverType(), DaemonParameters.AeronDriverType.EMBEDDED);
        Assert.assertTrue(parameters.getClientTimeout() == 1000);

        // Try with the "long version"
        commandLine = new String [] {"-port", "1400", "-subnet", subnetAddress.toString(), "-embeddedDriver", "-clientTimeout", "1000"};

        parameters = parser.parseCommandLine(commandLine);

        Assert.assertEquals(parameters.getIpAddress(), subnetAddress.getIpAddres().getHostAddress());
        Assert.assertTrue(DaemonParameters.DEFAULT_STREAM_ID == 10);
        Assert.assertTrue(parameters.getPort() == 1400);
        Assert.assertEquals(parameters.getSubnetAddress(), subnetAddress);
        Assert.assertEquals(parameters.getAeronDriverType(), DaemonParameters.AeronDriverType.EMBEDDED);
        Assert.assertTrue(parameters.getClientTimeout() == 1000);

        // Try embedded low latency driver
        commandLine = new String [] {"-port", "1400", "-subnet", subnetAddress.toString(), "-edll", "-clientTimeout", "1000"};
        parameters = parser.parseCommandLine(commandLine);
        Assert.assertEquals(parameters.getAeronDriverType(), DaemonParameters.AeronDriverType.LOWLATENCY_EMBEDDED);

        // Try long embedded low latency driver
        commandLine = new String [] {"-port", "1400", "-subnet", subnetAddress.toString(), "-embeddedDriverLowLatency", "-clientTimeout", "1000"};
        parameters = parser.parseCommandLine(commandLine);
        Assert.assertEquals(parameters.getAeronDriverType(), DaemonParameters.AeronDriverType.LOWLATENCY_EMBEDDED);

        // Try external driver directory
        commandLine = new String [] {"-port", "1400", "-subnet", subnetAddress.toString(), "-exdd", EXTERNAL_DRIVER_DIR, "-clientTimeout", "1000"};
        parameters = parser.parseCommandLine(commandLine);
        Assert.assertEquals(parameters.getExternalDriverDir(), EXTERNAL_DRIVER_DIR);

        // Try embedded driver config
        commandLine = new String [] {"-port", "1400", "-subnet", subnetAddress.toString(), "ed", "-edcf", EMBEDDED_DRIVER_CONFIG, "-clientTimeout", "1000"};
        parameters = parser.parseCommandLine(commandLine);
        Assert.assertEquals(parameters.getEmbeddedDriverConfigFile(), EMBEDDED_DRIVER_CONFIG);

        // Finally try with default parameters
        commandLine = new String [] {};
        parameters = parser.parseCommandLine(commandLine);

        Assert.assertEquals(parameters.getIpAddress(), subnetAddress.getIpAddres().getHostAddress());
        Assert.assertTrue(DaemonParameters.DEFAULT_STREAM_ID == 10);
        Assert.assertTrue(parameters.getPort() == DaemonParameters.DEFAULT_PORT);
        Assert.assertEquals(parameters.getSubnetAddress(), subnetAddress);
        Assert.assertEquals(parameters.getAeronDriverType(), DaemonParameters.AeronDriverType.EXTERNAL);
        Assert.assertTrue(parameters.getClientTimeout() == DaemonParameters.DEFAULT_CLIENT_TIMEOUT);

        parameters.toString();
    }

    @Test(expected = AutodiscException.class)
    public void parseWrongCommandLine() throws Exception
    {
        // Try with wrong parameters
        String[] commandLine = new String[]{"-skljdfsdf", "1400", "-sn", "sdfsdfsd", "-ed", "-ct", "1000"};

        CommandLineParser parser = new CommandLineParser();
        parser.parseCommandLine(commandLine);
    }

    @Test(expected = AutodiscException.class)
    public void parseWrongCommandLine2() throws Exception
    {
        final SubnetAddress subnetAddress = InetUtil.getDefaultSubnet();

        // Try with wrong parameters
        String [] commandLine = new String [] {"-p", "1400", "-sn", subnetAddress.toString(), "-ed", "-edll", "-ct", "1000"};

        CommandLineParser parser = new CommandLineParser();
       parser.parseCommandLine(commandLine);
    }

    @Test(expected = AutodiscException.class)
    public void parseWrongCommandLine3() throws Exception
    {
        // Try with wrong parameters
        String [] commandLine = new String [] {"-p", "1400", "-sn", "1.1.1.1/32", "-ed", "-ct", "1000"};

        CommandLineParser parser = new CommandLineParser();
        parser.parseCommandLine(commandLine);
    }

    @Test(expected = AutodiscException.class)
    public void parseWrongCommandLine4() throws Exception
    {
        // Try with wrong parameters
        String [] commandLine = new String [] {"-p", "1400", "-sn", "perico", "-ed", "-ct", "1000"};

        CommandLineParser parser = new CommandLineParser();
        parser.parseCommandLine(commandLine);
    }
}