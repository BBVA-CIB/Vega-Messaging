package com.bbva.kyof.vega.util.threads;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class created to test {@link RecurrentRunner}
 *
 * Created by XE52727 on 04/07/2016.
 */
public class RecurrentRunnerTest
{
    /** Default sleep time 1 millisecond */
    private final static long DEFAULT_SLEEP_IDLE = TimeUnit.MILLISECONDS.toNanos(500);

    /** Idle strategy to wait between consecutive action executions */
    private final IdleStrategy idleStrategy = new SleepingIdleStrategy(DEFAULT_SLEEP_IDLE);

    @Test
    public void testRunnerThatNeverExecute() throws Exception
    {
        final RunnerImplementation runner = new RunnerImplementation(idleStrategy, true, 0);

        // Start the thread
        new Thread(runner).start();

        // Wait for a second
        Thread.sleep(1000);

        // Get the number of executions is lowered, since it never executes it always applies sleep strategy
        int numberOfExecutions = runner.totalActionsTaken.get();

        // Check should stop
        Assert.assertFalse(runner.shouldStop());

        // Stop
        runner.close();

        // Check should stop
        Assert.assertTrue(runner.shouldStop());

        // There should be few executions
        Assert.assertTrue(numberOfExecutions < 5);
        Assert.assertTrue(numberOfExecutions > 0);
    }

    @Test
    public void testRunnerAlwaysExecute() throws Exception
    {
        final RunnerImplementation runner = new RunnerImplementation(idleStrategy, false, 0);

        // Start the thread
        new Thread(runner).start();

        // Wait for a second
        Thread.sleep(1000);

        // Since it always execute, this value should be high, sleep is never applied
        int numberOfActions = runner.totalActionsTaken.get();

        // Stop
        runner.close();

        // Check
        Assert.assertTrue(numberOfActions > 100);
        Assert.assertTrue(runner.totalActionsTaken.get() == 0);
    }

    @Test
    public void testRunnerLongStopTime() throws Exception
    {
        final RunnerImplementation runner = new RunnerImplementation(idleStrategy, false, 3000);

        // Start the thread
        new Thread(runner).start();

        // Give it some time to start
        Thread.sleep(1000);

        // Stop and calculate the time it takes
        final long startTime = System.currentTimeMillis();
        runner.close();
        final long totalStopTime = System.currentTimeMillis() - startTime;

        // Check
        Assert.assertTrue(totalStopTime > 1500);
    }

    private class RunnerImplementation extends RecurrentRunner
    {
        final AtomicInteger totalActionsTaken = new AtomicInteger(0);
        private final boolean neverExecute;
        private final int sleepMillisPerAction;

        RunnerImplementation(final IdleStrategy idleStrategy, final boolean neverExecute, final int sleepMillisPerAction)
        {
            super(idleStrategy);
            this.neverExecute = neverExecute;
            this.sleepMillisPerAction = sleepMillisPerAction;
        }

        @Override
        public int action()
        {
            this.totalActionsTaken.getAndIncrement();

            // Simulate that only one of each 2 actions is actually executed
            if (this.neverExecute)
            {
                return 0;
            }
            else
            {
                try
                {
                    Thread.sleep(sleepMillisPerAction);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }

                return this.totalActionsTaken.get();
            }
        }

        @Override
        public void cleanUp()
        {
            this.totalActionsTaken.set(0);
        }
    }
}