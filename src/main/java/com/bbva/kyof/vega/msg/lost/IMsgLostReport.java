package com.bbva.kyof.vega.msg.lost;

import java.util.UUID;

/**
 * Interface that represents the information about the lost messages.
 */
public interface IMsgLostReport
{
    /** @return the topic name of the received message */
    String getTopicName();

    /** @return  the instance id of the message sender */
    UUID getInstanceId();
    
    /** @return the topic publisher id of the message sender*/
    UUID getTopicPublisherId();
    
    /** @return the number of messages that have been lost */
    long getNumberLostMessages();   
}
