package com.bbva.kyof.vega.autodiscovery.sniffer;

import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import org.junit.Test;

/**
 * Test stand alone sniffer launcher
 */
public class SnifferLauncherTest
{
    private final static SubnetAddress SUBNET = InetUtil.getDefaultSubnet();

    @Test
    public void main() throws Exception
    {
        String[] VALID_CONFIG = new String[]{"-p", "35001", "-sn", SUBNET.toString(), "-t", "10000", "-ip", "225.0.0.1"};

        // Launch a signal to stop in 5 seconds
        new Thread(() ->
        {
            try
            {
                Thread.sleep(5000);
                SnifferLauncher.stop();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }).start();

        SnifferLauncher.main(VALID_CONFIG);
    }
}