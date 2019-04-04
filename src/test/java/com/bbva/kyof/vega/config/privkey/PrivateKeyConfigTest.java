package com.bbva.kyof.vega.config.privkey;

import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 11/10/2016.
 */
public class PrivateKeyConfigTest
{
    @Test(expected = VegaException.class)
    public void testBuilder() throws VegaException
    {
        final PrivateKeyConfig config = PrivateKeyConfig.builder().appSecurityId(11111).build();

        Assert.assertEquals(config.getAppSecurityId(), 11111);
        Assert.assertNull(config.getValue());
        Assert.assertEquals(config.isKeyEncrypted(), false);

        config.completeAndValidateConfig();
    }

    @Test
    public void testConstructors() throws VegaException
    {
        final PrivateKeyConfig config = new PrivateKeyConfig("anInvalidValue", 11111, false);
        final PrivateKeyConfig config2 = new PrivateKeyConfig();

        Assert.assertEquals(config.getAppSecurityId(), 11111);
        Assert.assertEquals(config.getValue(), "anInvalidValue");
        Assert.assertEquals(config.isKeyEncrypted(), false);

        Assert.assertEquals(config2.getAppSecurityId(), 0);
        Assert.assertNull(config2.getValue());
        Assert.assertNull(config2.getValue());

        config.completeAndValidateConfig();
    }
}