package com.bbva.kyof.vega.protocol.heartbeat;

import com.bbva.kyof.vega.msg.IRcvResponse;
import com.bbva.kyof.vega.msg.IResponseListener;
import com.bbva.kyof.vega.msg.ISentRequest;
import com.bbva.kyof.vega.util.threads.BlockCancelTask;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This task class manages the sending of heartbeats and processing of responses.
 * It also check before each heartbeat send if there is any connected client that has timed out.
 */
class SendHeartbeatTask extends BlockCancelTask implements IResponseListener
{
	/** Name of the topic this task belongs to */
	private final String topicName;
	/** Instance with the capacity to send heartbeats, it is used to decouple the task from the topic publisher */
	private final IHeartbeatSender sender;
	/** Heartbeat parameters used for the heartbeat sending in the topic */
	private final HeartbeatParameters parameters;
	/** Listener for heartbeats events */
	private final IClientConnectionListener listener;
	/** Number of client disconnections checks by client id */
	private final ConcurrentMap<UUID, AtomicInteger> numDisconnecChecksByClientId = new ConcurrentHashMap<>();
	
	/**
	 * Create a new task to send heartbeats
	 * 
	 * @param parameters heartbeat configuration parameters
	 * @param heartbeatSender object that can send heartbeats
	 * @param listener listener for heartbeat events
	 */
	SendHeartbeatTask(final String topicName,
							 final HeartbeatParameters parameters,
                             final IHeartbeatSender heartbeatSender,
                             final IClientConnectionListener listener)
	{
        super();

		this.topicName = topicName;
		this.sender = heartbeatSender;
		this.parameters = parameters;
		this.listener = listener;
	}
	
	@Override
	public void action()
	{
		// Check for timeouts
		this.checkForTimeouts();

		// Send the heartbeat
		this.sender.sendHeartbeat(this, parameters.getHeartbeatTimeout());
	}

	/**
	 * Check for timeouts and update the number of checks per client
	 */
	private void checkForTimeouts()
	{
		// Check for disconnections in all the clients
		this.numDisconnecChecksByClientId.forEach((id, checks) ->
		{
			// Increase number of checks and try against the maximum
			if (checks.incrementAndGet() == this.parameters.getMaxClientConnChecks())
			{
				// Remove the client information and notify the listener
				this.numDisconnecChecksByClientId.remove(id);
				this.listener.onClientDisconnected(this.topicName, id);
			}
		});
	}

	@Override
	public void onResponseReceived(final ISentRequest originalSentRequest, final IRcvResponse response)
	{
		// Get the number of timeouts for the client
		final AtomicInteger numTimeouts = this.numDisconnecChecksByClientId.get(response.getInstanceId());

		// If there is no client data try to create a new one
		if (numTimeouts == null)
		{
		    // Just in case it has been removed
			this.numDisconnecChecksByClientId.put(response.getInstanceId(), new AtomicInteger(0));

			// Notify the listener of the connection
			this.listener.onClientConnected(this.topicName, response.getInstanceId());
		}
		else
		{
			// if the client exist reset the checks
			numTimeouts.set(0);
		}
	}

	@Override
	public void onRequestTimeout(final ISentRequest originalSentRequest)
	{
		// Ignore
	}
}
