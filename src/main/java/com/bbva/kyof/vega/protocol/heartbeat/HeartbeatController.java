package com.bbva.kyof.vega.protocol.heartbeat;

import lombok.extern.slf4j.Slf4j;

import java.util.Timer;

/**
 * This class contains the CheckClientConnectedTask and HeartbeatTask in order to control
 * all the events related to heartbeats. It handle the start / stop of the different tasks
 * 
 * @author avh
 *
 */
@Slf4j
public class HeartbeatController
{
	/** Timer that handles the heartbeat tasks */
	private final Timer timer;

	/** The task that does the periodic heartbeat send */
	private final SendHeartbeatTask sendHeartbeatTask;

	/**
	 * Create a new controller and launch the tasks
	 *
	 * @param timer vega context
	 * @param topicName name of the topic the controller belongs to
	 * @param sender instance that can send heartbeats
	 * @param listener user listener for heartbeats events
	 * @param parameters user parameters for the heartbeat mechanism
	 */
	public HeartbeatController(final Timer timer, final String topicName, final IHeartbeatSender sender, final IClientConnectionListener listener, final HeartbeatParameters parameters)
	{
		log.info("Activate sending Heartbeats to Topic [{}] with parameters [{}]" , topicName, parameters);

        // Store the timer
        this.timer = timer;

		// Start the heartbeats
		this.sendHeartbeatTask = new SendHeartbeatTask(topicName, parameters, sender, listener);
        this.timer.schedule(this.sendHeartbeatTask, 0, parameters.getHeartbeatRate());
	}
	
	/**
	 * Stop the heartbeat sending
	 */
	public void stop()
	{
		// Stop the tasks
		this.sendHeartbeatTask.cancel();
		// Purge the timer
        this.timer.purge();
	}
}