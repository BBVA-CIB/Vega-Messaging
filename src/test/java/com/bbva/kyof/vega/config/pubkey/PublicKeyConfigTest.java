package com.bbva.kyof.vega.config.pubkey;

import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 11/10/2016.
 */
public class PublicKeyConfigTest
{
    @Test
    public void testBuilder() throws VegaException
    {
        final PublicKeyConfig config = PublicKeyConfig.builder().appSecurityId(11111).value("anInvalidValue").build();

        Assert.assertEquals(config.getAppSecurityId(), 11111);
        Assert.assertEquals(config.getValue(), "anInvalidValue");

        config.completeAndValidateConfig();
    }

    @Test
    public void testConstructors() throws VegaException
    {
        final PublicKeyConfig config = new PublicKeyConfig("anInvalidValue", 11111);
        final PublicKeyConfig config2 = new PublicKeyConfig();

        Assert.assertEquals(config.getAppSecurityId(), 11111);
        Assert.assertNotNull(config.getValue());
        Assert.assertEquals(config.getValue(), "anInvalidValue");

        Assert.assertEquals(config2.getAppSecurityId(), 0);
        Assert.assertNull(config2.getValue());

        config.completeAndValidateConfig();
    }
}