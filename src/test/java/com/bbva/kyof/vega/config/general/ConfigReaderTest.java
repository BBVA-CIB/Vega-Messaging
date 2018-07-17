package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Test;

import java.lang.reflect.Constructor;

/**
 * Testing methods for the {@link ConfigReader}
 * Created by XE52727 on 23/05/2016.
 */
public class ConfigReaderTest
{
   
    /** File containing the configuration */
    private static final String validConfigFile = ConfigReaderTest.class.getClassLoader().getResource("config/validConfiguration.xml").getPath();

    /** Malformed configuration file */
    private static final String malformedConfigFile = ConfigReaderTest.class.getClassLoader().getResource("config/malformedConfiguration.xml").getPath();

    @Test
    public void loadValidConfig() throws Exception
    {
        ConfigReader.readConfiguration(validConfigFile);
    }

    @Test(expected = VegaException.class)
    public void loadInvalidConfig() throws Exception
    {
        ConfigReader.readConfiguration(malformedConfigFile);
    }

    @Test
    public void testConstructor() throws Exception
    {
        Constructor<?>[] cons = ConfigReader.class.getDeclaredConstructors();
        cons[0].setAccessible(true);
        cons[0].newInstance((Object[]) null);
    }
}
