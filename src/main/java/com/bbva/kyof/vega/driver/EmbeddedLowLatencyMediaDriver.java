package com.bbva.kyof.vega.driver;

import io.aeron.CommonContext;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import lombok.extern.slf4j.Slf4j;
import org.agrona.SystemUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;

/**
 * Media driver configured for low latency communications and prepared to be launched embedded in the application.
 *
 * It uses a dedicated threading model and consume lot of CPU to ensure the fastest communications possible.
 */
@Slf4j
public class EmbeddedLowLatencyMediaDriver extends AbstractEmbeddedMediaDriver
{
    /**
     * Constructor to create the media driver
     *
     * @param configFile configuration fiel for the driver, it can be null to use default configuration
     */
    public EmbeddedLowLatencyMediaDriver(final String configFile)
    {
        super(configFile);
    }

    @Override
    public MediaDriver createMediaDriver(final String driverConfigFile)
    {
        log.info("Creating embedded low latency media driver with configuration file [{}]", driverConfigFile);

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
                .threadingMode(ThreadingMode.DEDICATED)
                .conductorIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1))
                .receiverIdleStrategy(new BusySpinIdleStrategy())
                .senderIdleStrategy(new BusySpinIdleStrategy());

        return MediaDriver.launch(ctx);
    }
}
