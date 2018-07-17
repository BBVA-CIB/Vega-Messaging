package com.bbva.kyof.vega.driver;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 29/07/16.
 */
public class EmbeddedMediaDriverTest
{
    @Test
    public void testDriver()
    {
        final EmbeddedMediaDriver driver = new EmbeddedMediaDriver(null);
        Assert.assertNotNull(driver.getDriverDirectoryName());
        driver.close();
    }
}