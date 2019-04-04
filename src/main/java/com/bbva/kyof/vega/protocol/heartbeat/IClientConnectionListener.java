package com.bbva.kyof.vega.protocol.heartbeat;

import java.util.UUID;

/**
 * Listener for callbacks related to the heartbeat control mechanism. It will notify of
 * connections / disconnections based on the send and receive of heartbeats
 * 
 * @author XE27609
 *
 */
public interface IClientConnectionListener
{
	/**
	 * Method called when a new client connects and has been checked using the heartbeats mechanism
	 *
	 * @param topicName topic name the client has connected to
	 * @param clientInstanceId unique instance id of the connected client
	 */
	void onClientConnected(String topicName, UUID clientInstanceId);
	
	
	/**
	 * Method called when a client disconnects. This method is based on the heartbeat mechanism included in the framework. 
	 * 
	 * It also depend on the heartbeat checks configured before considering a client disconnected.
	 *
	 * @param topicName topic name the client has disconnected from
	 * @param clientInstanceId unique instance id of the connected client
	 */
	void onClientDisconnected(String topicName, UUID clientInstanceId);
}
