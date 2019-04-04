package com.bbva.kyof.vega.protocol.heartbeat;

import com.bbva.kyof.vega.msg.IResponseListener;

/**
 * Interface that should be implemented by a class that is able to send heartbeat messages.
 * This is used internally by the framework heartbeat mechanism only.
 */
public interface IHeartbeatSender
{
	/**
	 * Send a heartbeat message
	 * 
	 * @param responseListener response listener to receive the heartbeat response
	 * @param timeout timeout time to use in the heartbeat
	 */
	void sendHeartbeat(final IResponseListener responseListener, final long timeout);
}
