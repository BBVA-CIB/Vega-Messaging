package com.bbva.kyof.vega.protocol.heartbeat;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * This class contains all the configurable options to send heartbeats and control client connections using the heartbeat mechanism.
 *
 * It uses the Builder pattern.
 *
 * The unsettled parameters will be settled to the default values.
 * 
 * @author XE27609
 *
 */
@Builder
@ToString
public final class HeartbeatParameters
{
	/** Default heartbeat send rate in milliseconds */
	private static final int DEFAULT_RATE = 1000;
	/** Default heartbeat timeout value in milliseconds */
	private static final int DEFAULT_TIMEOUT = 1000;
	/** Default checks before considering a client disconnected */
	private static final int DEFAULT_CLIENT_CONN_CHECKS = 3;

	/** Heartbeat send rate in milliseconds */
	@Getter private final int heartbeatRate;
	/** Heartbeat timeout time in milliseconds */
	@Getter private final int heartbeatTimeout;
	/** Number of checks before considering a client disconnected */
	@Getter private final int maxClientConnChecks;
	
	/** Redefine the builder to force some default parameters */
	public static class HeartbeatParametersBuilder
	{
		/** Heartbeat send rate in milliseconds */
		private int heartbeatRate = DEFAULT_RATE;
		/** Heartbeat timeout time in milliseconds */
		private int heartbeatTimeout = DEFAULT_TIMEOUT;
		/** Number of checks before considering a client disconnected */
		private int maxClientConnChecks = DEFAULT_CLIENT_CONN_CHECKS;
	}
}
