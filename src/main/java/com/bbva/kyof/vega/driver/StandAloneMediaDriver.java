package com.bbva.kyof.vega.driver;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import lombok.extern.slf4j.Slf4j;
import org.agrona.SystemUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.SigIntBarrier;

/**
 * Media driver  prepared to be launched in stand alone mode. <p>
 *
 * Configured to reduce the usage of CPU. Not optimized for Low Latency.
 */
@Slf4j
public final class StandAloneMediaDriver
{
    /** Private constructor to avoid instantiation */
    private StandAloneMediaDriver()
    {
        // Nothing to do
    }

    /**
     * Launch the driver
     * @param args driver argument property files
     */
    public static void main(final String[] args)
    {
        SystemUtil.loadPropertiesFiles(args);

        final MediaDriver.Context ctx = new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .conductorIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1))
                .receiverIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1))
                .senderIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1));

        try (final MediaDriver ignored = MediaDriver.launch(ctx))
        {
            log.info("Driver started...");

            new SigIntBarrier().await();
        }
        ctx.close();
        log.info("Driver stopped...");
    }
}
