package com.bbva.kyof.vega.driver;

import com.bbva.kyof.vega.config.general.AeronDriverType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 29/07/16.
 */
public class EmbeddedMediaDriverTest
{
    @Test
    public void testEmbeddedDriver()
    {
        final EmbeddedMediaDriver driver = new EmbeddedMediaDriver(null, AeronDriverType.EMBEDDED);
        Assert.assertNotNull(driver.getDriverDirectoryName());
        driver.close();
    }

    @Test
    public void testLowLatencyEmbeddedDriver()
    {
        final EmbeddedMediaDriver driver = new EmbeddedMediaDriver(null, AeronDriverType.LOWLATENCY_EMBEDDED);
        Assert.assertNotNull(driver.getDriverDirectoryName());
        driver.close();
    }

    @Test
    public void tesBackOffEmbeddedDriver()
    {
        final EmbeddedMediaDriver driver = new EmbeddedMediaDriver(null, AeronDriverType.BACK_OFF_EMBEDDED);
        Assert.assertNotNull(driver.getDriverDirectoryName());
        driver.close();
    }
}