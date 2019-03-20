package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.msg.IRcvMessage;
import com.bbva.kyof.vega.msg.IRcvRequest;
import com.bbva.kyof.vega.msg.lost.IMsgLostReport;

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
    
    /**
     * The method is invoked when a gap in the sequence number of the last two messages received from the same topicPublisherId is detected {@link com.bbva.kyof.vega.protocol.subscriber.TopicSubscriber#checkMessageLoss(com.bbva.kyof.vega.msg.RcvMessage) }.
     * This sequence number is located at the message data header of Vega {@link com.bbva.kyof.vega.msg.MsgDataHeader }
     * 
     * Vega is implemented over Aeron, that provides a reliable connection for unreliable protocols.
     * Even so, there may be messages that Aeron can not recover. For this reason, this functionality has been implemented, allowing the user to know when a message is lost.
     *
     * The sequence numbers are shared by messages and requests and therefore this callback applies to any of both cases.
     *
     * Message loss is not check for responses.
     *
     * Default implementation does nothing with the loss report
     * 
     * @param lostReport a report of the lost messages
     */
    default void onMessageLost(final IMsgLostReport lostReport)
    {    
        // By default there is no action to take
    }
}

