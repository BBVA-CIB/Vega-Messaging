package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.msg.IRcvMessage;
import com.bbva.kyof.vega.msg.IRcvRequest;

/**
 * Implement in order to receive messages on a subscribed topic
 */
public interface ITopicSubListener
{
    /**
     * Method called when a message is received.
     *
     * IMPORTANT: If the received message contents are going to be accessed from a separate thread the message should be promoted!!
     *
     * @param receivedMessage the received message
     */
    void onMessageReceived(final IRcvMessage receivedMessage);

    /**
     * Method called when a request message is received.
     *
     * IMPORTANT: If the received message contents are going to be accessed from a separate thread the message should be promoted!!
     *
     * @param receivedRequest the received request
     */
    void onRequestReceived(final IRcvRequest receivedRequest);
}

