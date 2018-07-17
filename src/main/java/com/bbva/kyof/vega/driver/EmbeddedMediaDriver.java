package com.bbva.kyof.vega.driver;

import io.aeron.CommonContext;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import lombok.extern.slf4j.Slf4j;
import org.agrona.SystemUtil;
import org.agrona.concurrent.BackoffIdleStrategy;

/**
 * Embedded media driver not optimized for low latency communications. It uses a shared threading model and BackoffIdleStrategy
 * for the internal threads.
 */
@Slf4j
public class EmbeddedMediaDriver extends AbstractEmbeddedMediaDriver
{
    /**
     * Constructor to create the media driver
     *
     * @param configFile configuration fiel for the driver, it can be null to use default configuration
     */
    public EmbeddedMediaDriver(final String configFile)
    {
        super(configFile);
    }

    @Override
    public MediaDriver createMediaDriver(final String driverConfigFile)
    {
        log.info("Creating embedded media driver with configuration file [{}]", driverConfigFile);

        if (driverConfigFile == null)
        {
            SystemUtil.loadPropertiesFiles(new String[0]);
        }
        else
        {
            SystemUtil.loadPropertiesFiles(new String[]{driverConfigFile});
        }

        final MediaDriver.Context ctx = new MediaDriver.Context()
                .aeronDirectoryName(CommonContext.generateRandomDirName())
                .threadingMode(ThreadingMode.SHARED)
                .conductorIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1))
                .receiverIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1))
                .senderIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1));

        return MediaDriver.launch(ctx);
    }
}
