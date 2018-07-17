package com.bbva.kyof.vega.util.threads;


import lombok.extern.slf4j.Slf4j;
import org.agrona.concurrent.IdleStrategy;

import java.io.Closeable;

/**
 *  
 * This class implements a runnable that will continuously execute an action until
 * it is stopped.
 */
@Slf4j
public abstract class RecurrentRunner implements Runnable, Closeable
{
	/** Sleep time while checking for stop */
	private static final int SLEEP_TIME = 1;
	/** Idle strategy between consecutive action executions. If null it wont be applied */
	private final IdleStrategy idleStrategy;
	/** True if the task should close */
	private volatile boolean shouldStopRunner = false;
	/** True if the task has been stopped */
	private volatile boolean stopped = false;

    /**
     * Create a new recurrent runner
     * @param idleStrategy idle strategy to wait between consecutive action executions. Null to avoid waiting.
     */
	public RecurrentRunner(final IdleStrategy idleStrategy)
	{
		this.idleStrategy = idleStrategy;
	}

	@Override
	public void close()
	{
		this.shouldStopRunner = true;

		while(!this.stopped)
		{
			try
			{
				Thread.sleep(SLEEP_TIME);
			}
			catch (final InterruptedException e)
			{
				log.error("Thread unexpectedly interrupted", e);
			}
		}
	}

	/**
	 * Return true if it should stop
	 *
	 * @return true if should stop
     */
    public boolean shouldStop()
    {
        return this.shouldStopRunner;
    }

	@Override
	public void run()
	{
		while(!this.shouldStopRunner)
		{
			this.idleStrategy.idle(this.action());
		}

		this.stopped = true;
		this.cleanUp();
	}

	/**
	 * Action to execute.
     *
     * @return the number of internal actions taken, if bigger than 0 the idle strategy is applied
	 */
	public abstract int action();

	/**
	 * Method called after stopping the runner
	 */
	public abstract void cleanUp();
}
