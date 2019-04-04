package com.bbva.kyof.vega.driver;

import com.bbva.kyof.vega.config.general.AeronDriverType;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import lombok.extern.slf4j.Slf4j;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.io.Closeable;

/**
 * Embedded media driver
 */
@Slf4j
public class EmbeddedMediaDriver implements Closeable
{
    /** The created Aeron Media Driver */
    private final MediaDriver driver;

    /**
     * Constructor to create the media driver
     *
     * @param driverConfigFile configuration field for the driver, it can be null to use default configuration
     * @param driverType type of the driver to use
     */
    public EmbeddedMediaDriver(final String driverConfigFile, final AeronDriverType driverType)
    {
        log.info("Creating embedded media driver with configuration file [{}] and type [{}]", driverConfigFile, driverType);

        if (driverConfigFile == null)
        {
            SystemUtil.loadPropertiesFiles(new String[0]);
        }
        else
        {
            SystemUtil.loadPropertiesFiles(new String[]{driverConfigFile});
        }

        // Create the driver context
        final MediaDriver.Context ctx = new MediaDriver.Context();

        // Get the Aeron directory property, it may have been settled by config file, java system property, etc
        final String aeronDirectory = System.getProperty("aeron.dir");

        // If not settled use a random one
        if (aeronDirectory == null || aeronDirectory.isEmpty())
        {
            ctx.aeronDirectoryName(Aeron.Context.generateRandomDirName());
        }

        switch (driverType)
        {
            case EXTERNAL:
                throw new IllegalArgumentException("Embedded driver cannot be created with external driver type ");
            case EMBEDDED:
                ctx.threadingMode(ThreadingMode.SHARED);
                ctx.conductorIdleStrategy(new SleepingMillisIdleStrategy(1));
                ctx.receiverIdleStrategy(new SleepingMillisIdleStrategy(1));
                ctx.senderIdleStrategy(new SleepingMillisIdleStrategy(1));
                break;
            case BACK_OFF_EMBEDDED:
                ctx.threadingMode(ThreadingMode.SHARED);
                ctx.conductorIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1));
                ctx.receiverIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1));
                ctx.senderIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1));
                break;
            case LOWLATENCY_EMBEDDED:
                ctx.threadingMode(ThreadingMode.DEDICATED);
                ctx.conductorIdleStrategy(new BusySpinIdleStrategy());
                ctx.receiverIdleStrategy(new BusySpinIdleStrategy());
                ctx.senderIdleStrategy(new BusySpinIdleStrategy());
                break;
            default:
                throw new IllegalArgumentException("Driver type was not found!");
        }

        this.driver = MediaDriver.launch(ctx);
    }

    /** @return the directory name where the driver is writing */
    public String getDriverDirectoryName()
    {
        return driver.aeronDirectoryName();
    }

    /** Stop the media driver */
    public void stop()
    {
        CloseHelper.quietClose(this.driver);
    }

    @Override
    public void close()
    {
        log.info("Stopping embedded media driver");
        this.stop();
    }
}
