package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.exception.VegaException;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 01/08/16.
 */
public class RcvPollerConfigTest
{
    @Test
    public void emptyConstructor() throws Exception
    {
        new RcvPollerConfig();
    }

    @Test(expected = VegaException.class)
    public void configMissingName() throws Exception
    {
        final RcvPollerConfig config = RcvPollerConfig.builder().build();
        config.completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void configMissingIdleStrategy() throws Exception
    {
        final RcvPollerConfig config = RcvPollerConfig.builder().name("name").build();
        config.completeAndValidateConfig();
    }

    @Test
    public void validConfigDefaultParams() throws Exception
    {
        final RcvPollerConfig config = RcvPollerConfig.builder().name("name").idleStrategyType(IdleStrategyType.BUSY_SPIN).build();
        config.completeAndValidateConfig();
        Assert.assertTrue(config.getIdleStrategy() instanceof BusySpinIdleStrategy);
        Assert.assertEquals(config.getName(), "name");
        Assert.assertTrue(config.getMaxFragmentsPerPoll() == RcvPollerConfig.DEFAULT_MAX_FRAGMENTS_POLL);
    }

    @Test(expected = VegaException.class)
    public void validConfigSleepIdleMissingTime() throws Exception
    {
        final RcvPollerConfig config = RcvPollerConfig.builder().name("name").idleStrategyType(IdleStrategyType.SLEEP_NANOS).build();
        config.completeAndValidateConfig();
    }

    @Test
    public void validConfigSleep() throws Exception
    {
        final RcvPollerConfig config = RcvPollerConfig.builder().name("name").
                idleStrategyType(IdleStrategyType.SLEEP_NANOS).idleStrategySleepTime(10L)
                .build();

        config.completeAndValidateConfig();

        Assert.assertTrue(config.getIdleStrategy() instanceof SleepingIdleStrategy);
    }
}