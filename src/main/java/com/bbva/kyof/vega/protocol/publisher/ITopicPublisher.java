package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.msg.IResponseListener;
import com.bbva.kyof.vega.msg.ISentRequest;
import com.bbva.kyof.vega.msg.PublishResult;
import com.bbva.kyof.vega.protocol.heartbeat.IClientConnectionListener;
import com.bbva.kyof.vega.protocol.heartbeat.HeartbeatParameters;
import org.agrona.DirectBuffer;

import java.util.UUID;

/**
 * Interface for a topic publisher with the functionality available for a user
 */
public interface ITopicPublisher
{
    /**
     * This method sendM a normal message
     *
     * Try to reuse buffers if possible to reduce memory usage!
     *
     * It will send the provided offset and length to get the contents in the buffer to send
     *
     * @param message the binary message to send
     * @param offset Offset for the message start in the buffer
     * @param length Length of the message starting in the given offset
     * @return the enum with the result of the publication
     */
    PublishResult sendMsg(final DirectBuffer message, final int offset, final int length);

    /**
     * Send a request on the topic.
     *
     * It will send the provided offset and length to get the contents in the buffer to send
     *
     * Try to reuse buffers if possible to reduce memory allocation.
     *
     * @param message The request message to send
     * @param offset Offset for the message start in the buffer
     * @param length Length of the message starting in the given offset
     * @param timeout the timeout of the request in milliseconds
     * @param respListener Listener for responses on the request. If null no responses will be processed.
     *
     * @return an object that represent the sent request, containing the request ID and other useful information like the publish result
     */
    ISentRequest sendRequest(final DirectBuffer message, final int offset, final int length, final long timeout, final IResponseListener respListener);

    /** @return the topic associated to this topic publisher */
    String getTopicName();
    
    /** @return topic unique Id associated to this topic publisher */
    UUID getUniqueId();

    /**
     * Activate the heartbeats in the topic with the given options
     *
     * If the heartbeats are already active, it will deactivate and reactivate them with the new parameters
     *
     * @param listener listener for heartbeats related events
     * @param parameters configuration parameters for the hearbeats
     */
    void activateHeartbeats(IClientConnectionListener listener, HeartbeatParameters parameters);

    /** Deactivate heartbeats if active */
    void deactivateHeartbeats();

    /**
     * True if heartbeats are active
     *
     * @return true if the heartbeats are currently active
     */
    boolean isHeartbeatsActive();
}