package com.bbva.kyof.vega.autodiscovery.daemon;

import org.junit.Ignore;
import org.junit.Test;
import sun.misc.Signal;

/**
 * Created by cnebrera on 05/08/16.
 */
public class UnicastDaemonLauncherTest
{
    @Ignore
    @Test
    public void main() throws Exception
    {
        String [] commandLine = new String [] {"-p", "1400", "-ed", "-ct", "1000"};

        // Launch a signal to stop in 5 seconds
        new Thread(() ->
        {
            try
            {
                Thread.sleep(5000);
                Signal.raise(new Signal("INT"));
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }).start();

        UnicastDaemonLauncher.main(commandLine);
    }
}