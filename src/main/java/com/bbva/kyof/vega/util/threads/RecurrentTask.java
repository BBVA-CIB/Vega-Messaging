package com.bbva.kyof.vega.util.threads;

import org.agrona.concurrent.IdleStrategy;

/**
 * Created by cnebrera on 18/05/16.
 */
public abstract class RecurrentTask extends RecurrentRunner
{
    /**
     * Create a new recurrent task
     *
     * @param idleStrategy idle strategy to wait between consecutive action executions. Null to avoid waiting.
     */
    public RecurrentTask(final IdleStrategy idleStrategy)
    {
        super(idleStrategy);
    }

    /**
     * Start the recurrent task
     * @param threadName the name of the task thread
     */
    public void start(final String threadName)
    {
        new Thread(this, threadName).start();
    }
}
