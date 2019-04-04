package com.bbva.kyof.vega.util.threads;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by cnebrera on 05/10/2016.
 */
public class BlockCancelTaskTest
{
    private Timer timer;

    @Before
    public void before()
    {
        this.timer = new Timer("TestTimer");
    }

    @After
    public void after()
    {
        this.timer.cancel();
        this.timer.purge();
    }

    @Test
    public void testRunAndCancel() throws Exception
    {
        final AtomicInteger actionsRun = new AtomicInteger(0);

        final BlockCancelTask task = new BlockCancelTask()
        {
            @Override
            public void action()
            {
                actionsRun.getAndIncrement();
            }
        };

        this.timer.scheduleAtFixedRate(task, 0, 1000);

        Thread.sleep(1500);

        int currentRuns = actionsRun.get();
        task.cancel();

        Thread.sleep(1000);

        Assert.assertTrue(currentRuns == actionsRun.get() || currentRuns == actionsRun.get() + 1);
    }
}