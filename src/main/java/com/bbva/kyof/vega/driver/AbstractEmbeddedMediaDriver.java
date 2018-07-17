package com.bbva.kyof.vega.driver;

import io.aeron.driver.MediaDriver;
import lombok.extern.slf4j.Slf4j;
import org.agrona.CloseHelper;

import java.io.Closeable;

/**
 * Base class for embedded media driver implementations
 */
@Slf4j
public abstract class AbstractEmbeddedMediaDriver implements Closeable
{
    /** The created Aeron Media Driver */
    private final MediaDriver driver;

    /**
     * Constructor to create the media driver
     *
     * @param configFile configuration fiel for the driver, it can be null to use default configuration
     */
    AbstractEmbeddedMediaDriver(final String configFile)
    {
        this.driver = this.createMediaDriver(configFile);
    }

    /**
     * Implement in order to instantiate the proper media driver depending on the driver type
     *
     * @param configFile configuration fiel for the driver, it can be null to use default configuration
     *
     * @return the created Aeron Media Driver
     */
    public abstract MediaDriver createMediaDriver(String configFile);

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
