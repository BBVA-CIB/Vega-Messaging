package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.config.general.IdleStrategyType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 01/08/16.
 */
public class IdleStrategyTypeTest
{
    @Test
    public void valueAndFromValue() throws Exception
    {
        Assert.assertEquals(IdleStrategyType.BUSY_SPIN.value(), "BUSY_SPIN");
        Assert.assertEquals(IdleStrategyType.SLEEP_NANOS.value(), "SLEEP_NANOS");
        Assert.assertEquals(IdleStrategyType.fromValue("BUSY_SPIN"), IdleStrategyType.BUSY_SPIN);
        Assert.assertEquals(IdleStrategyType.fromValue("SLEEP_NANOS"), IdleStrategyType.SLEEP_NANOS);
    }
}