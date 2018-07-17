package com.bbva.kyof.vega.autodiscovery.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;

/**
 * Listener to implement in order to receive events triggered by a topic pattern subscription.<p>
 *
 * The events correspond to topics that match the given pattern in the subscription.<p>
 *
 * A single event is triggered per topic the first time that any application registers it, if a second application
 * register the same topic there will no event triggered. <p>
 *
 * The removed event is triggered when there are no more applications with that topic in the domain.
 */
public interface IAutodiscPubTopicPatternListener
{
    /**
     * Called when a new publisher topic for the given topic name appears in the domain
     * @param pubTopicInfo the publisher topic information
     * @param topicPattern the pattern of the original pattern subscription that triggered the event
     */
    void onNewPubTopicForPattern(AutoDiscTopicInfo pubTopicInfo, String topicPattern);

    /**
     * Called when there are no more publishers for the topic in the domain
     * @param pubTopicInfo the topic information
     * @param topicPattern the pattern of the original pattern subscription that triggered the event
     */
    void onPubTopicForPatternRemoved(AutoDiscTopicInfo pubTopicInfo, String topicPattern);
}
