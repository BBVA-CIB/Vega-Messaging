package com.bbva.kyof.vega.util.threads;

import lombok.Getter;

import java.util.TimerTask;

/**
 * Base class for a TimerTask in which we ensure that the cancel wont return until
 * the task is really cancelled and wont be executed again
 *
 * */
public abstract class BlockCancelTask extends TimerTask
{
    /** Synchronization lock */
    private final Object lock = new Object();

    /** A boolean to control when the task is canceled */
    @Getter private volatile boolean isCanceled = false;

    /**
     * Constructor
     */
    public BlockCancelTask()
    {
        super();

        this.isCanceled = false;
    }

    @Override
    public boolean cancel()
    {
        this.isCanceled = true;

        synchronized(this.lock)
        {
            return super.cancel();
        }
    }

    @Override
    public void run()
    {
        synchronized(this.lock)
        {
            if(this.isCanceled)
            {
                return;
            }

            this.action();
        }
    }

    /**
     * Action to execute
     */
    public abstract void action();
}
