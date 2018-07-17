package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.config.general.AeronDriverType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 01/08/16.
 */
public class AeronDriverTypeTest
{
    @Test
    public void valueAndFromValue() throws Exception
    {
        Assert.assertEquals(AeronDriverType.EMBEDDED.value(), "EMBEDDED");
        Assert.assertEquals(AeronDriverType.EXTERNAL.value(), "EXTERNAL");
        Assert.assertEquals(AeronDriverType.fromValue("EMBEDDED"), AeronDriverType.EMBEDDED);
        Assert.assertEquals(AeronDriverType.fromValue("EXTERNAL"), AeronDriverType.EXTERNAL);
    }
}