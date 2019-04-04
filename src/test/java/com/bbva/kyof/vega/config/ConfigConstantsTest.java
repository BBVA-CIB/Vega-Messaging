package com.bbva.kyof.vega.config;

import com.bbva.kyof.vega.config.privkey.PrivateKeyConfigReader;
import org.junit.Test;

import java.lang.reflect.Constructor;

/**
 * Created by cnebrera on 11/10/2016.
 */
public class ConfigConstantsTest
{
    @Test
    public void testConstructor() throws Exception
    {
        Constructor<?>[] cons = ConfigConstants.class.getDeclaredConstructors();
        cons[0].setAccessible(true);
        cons[0].newInstance((Object[]) null);
    }
}