package com.bbva.kyof.vega.util.threads;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by cnebrera on 29/07/16.
 */
public class RecurrentTaskTest
{
    /** Default sleep time 1 millisecond */
    private final static long DEFAULT_SLEEP_IDLE = TimeUnit.MILLISECONDS.toNanos(500);

    /** Idle strategy to wait between consecutive action executions */
    private final IdleStrategy idleStrategy = new SleepingIdleStrategy(DEFAULT_SLEEP_IDLE);

    @Test
    public void start() throws Exception
    {
        final AtomicInteger numActions = new AtomicInteger(0);

        final RecurrentTask recurrentTask = new RecurrentTask(idleStrategy)
        {
            @Override
            public int action()
            {
                return numActions.getAndIncrement();
            }

            @Override
            public void cleanUp()
            {
            }
        };

        recurrentTask.start("test");

        // Wait for a second
        Thread.sleep(1000);

        // Stop
        recurrentTask.close();

        // There should lot of executions
        Assert.assertTrue(numActions.get() > 100);
    }
}