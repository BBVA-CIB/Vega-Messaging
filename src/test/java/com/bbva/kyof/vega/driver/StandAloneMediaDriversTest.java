package com.bbva.kyof.vega.driver;

import lombok.extern.slf4j.Slf4j;
import org.junit.Ignore;
import org.junit.Test;
import sun.misc.Signal;

/**
 * Created by cnebrera on 29/07/16.
 */
@Slf4j
public class StandAloneMediaDriversTest
{
    @Ignore
    @Test
    public void testDriverExitStop() throws Exception
    {
        log.info("Waiting 10 seconds to allow first media driver to free all resources");
        Thread.sleep(10000);

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

        // Start the driver
        StandAloneMediaDriver.main(new String[0]);
    }

    @Ignore
    @Test
    public void testLowLatencyDriverExitStop() throws Exception
    {
        log.info("Waiting 10 seconds to allow first media driver to free all resources");
        Thread.sleep(10000);

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

        // Start the driver
        StandAloneLowLatencyMediaDriver.main(new String[0]);
    }
}