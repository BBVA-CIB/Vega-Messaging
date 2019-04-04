package com.bbva.kyof.vega.driver;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import lombok.extern.slf4j.Slf4j;
import org.agrona.SystemUtil;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.SigIntBarrier;

/**
 * Media driver  prepared to be launched in stand alone mode. <p>
 *
 * Configured for low latency communications using an internal dedicated threading model and BusySpin idle strategy for internal threads
 */
@Slf4j
public final class StandAloneLowLatencyMediaDriver
{
    /** Private constructor to avoid instantiation */
    private StandAloneLowLatencyMediaDriver()
    {
        // Nothing to do here
    }

    /**
     * Launch the driver
     * @param args driver argument property files
     */
    public static void main(final String[] args)
    {
        SystemUtil.loadPropertiesFiles(args);

        final MediaDriver.Context ctx = new MediaDriver.Context()
                .threadingMode(ThreadingMode.DEDICATED)
                .conductorIdleStrategy(new BusySpinIdleStrategy())
                .receiverIdleStrategy(new BusySpinIdleStrategy())
                .senderIdleStrategy(new BusySpinIdleStrategy());

        try (final MediaDriver ignored = MediaDriver.launch(ctx))
        {
            log.info("Driver started...");

            new SigIntBarrier().await();
        }
        ctx.close();
        log.info("Driver stopped...");
    }
}
