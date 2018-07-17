package com.bbva.kyof.vega.autodiscovery;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.autodiscovery.subscriber.IAutodiscTopicSubListener;
import lombok.Getter;
import lombok.ToString;

/**
 * Contents of a onNewPubTopicForPattern / onPubTopicForPatternRemoved to topic action
 */
@ToString
final class AutodiscTopicSubcribeActionContent
{
    /** Name of the topic */
    @Getter private final String topicName;
    /** Transport type for the subscription / unsubscription */
    @Getter private final AutoDiscTransportType transportType;
    /** Listener for events on this subscription */
    @Getter private final IAutodiscTopicSubListener listener;

    /**
     * Create a new subscription action content
     * @param topicName Name of the topic
     * @param transportType Transport type for the subscription / unsubscription
     * @param listener listener for events related to the subscription
     */
    AutodiscTopicSubcribeActionContent(final String topicName, final AutoDiscTransportType transportType, final IAutodiscTopicSubListener listener)
    {
        this.topicName = topicName;
        this.transportType = transportType;
        this.listener = listener;
    }
}
