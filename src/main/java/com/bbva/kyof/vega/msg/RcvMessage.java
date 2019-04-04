package com.bbva.kyof.vega.msg;


import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.UUID;

/**
 * Represent a received message.
 *
 * This class is not thread safe!
 */
@NoArgsConstructor
public class RcvMessage extends BaseRcvMessage implements IRcvMessage
{
    /** TopicPublisherId of the topic publisher that sent the message */
    @Getter @Setter private UUID topicPublisherId;

    @Override
    public IRcvMessage promote()
    {
        // Create the promoted message
        final RcvMessage promotedMsg = new RcvMessage();
        // Promote contents
        this.promote(promotedMsg);
        // Return promoted message
        return promotedMsg;
    }

    /**
     * Promote the message contents into the given promoted message
     *
     * @param promotedMsg the promoted message to clone the contents into
     */
    void promote(final RcvMessage promotedMsg)
    {
        super.promote(promotedMsg);
        promotedMsg.topicPublisherId = this.topicPublisherId;
    }
}
