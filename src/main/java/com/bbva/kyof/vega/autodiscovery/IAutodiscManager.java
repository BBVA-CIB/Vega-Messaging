package com.bbva.kyof.vega.autodiscovery;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.autodiscovery.subscriber.IAutodiscInstanceListener;
import com.bbva.kyof.vega.autodiscovery.subscriber.IAutodiscPubTopicPatternListener;
import com.bbva.kyof.vega.autodiscovery.subscriber.IAutodiscTopicSubListener;

/**
 * Interface implemented by the auto discovery manager that contains all method to interact with the
 * auto-discovery process.
 */
public interface IAutodiscManager
{
    /** Start the auto-discovery process */
    void start();

    /**
     * Stops the autodiscovery process. It wont return until all threads have been stopped
     */
    void close();

    /**
     * Register the given instance information in auto-discovery to be periodically advert to other clients
     *
     * @param instanceInfo the instance information to publish
     */
    void registerInstanceInfo(final AutoDiscInstanceInfo instanceInfo);

    /**
     * Unregister the given instance information from auto-discovery and close periodically advert to other clients
     */
    void unregisterInstanceInfo();

    /**
     * Register the given topic publisher or subscriber topic information in auto-discovery to be periodically advert to other clients.
     *
     * @param autoDiscTopicInfo the topic information information to publish
     */
    void registerTopicInfo(final AutoDiscTopicInfo autoDiscTopicInfo);

    /**
     * Unregister the given topic publisher or subscriber topic information from auto-discovery and close periodical adverts
     *
     * @param autoDiscTopicInfo the topic information information to remove
     */
    void unregisterTopicInfo(final AutoDiscTopicInfo autoDiscTopicInfo);

    /**
     * Register the given topic - socket pair information in auto-discovery to be periodically advert to other clients
     *
     * @param autoDiscTopicSocketInfo the topic socket pair information to publish
     */
    void registerTopicSocketInfo(final AutoDiscTopicSocketInfo autoDiscTopicSocketInfo);

    /**
     * Unregister the given topic - socket pair information in auto-discovery to be periodically advert to other clients
     *
     * @param autoDiscTopicSocketInfo the topic socket pair information to publish
     */
    void unregisterTopicSocketInfo(final AutoDiscTopicSocketInfo autoDiscTopicSocketInfo);

    /**
     * Subscribe to receive topic adverts with instances information
     *
     * @param listener listener that will receive the notifications of adverts related to the subscribed topic
     */
    void subscribeToInstances(final IAutodiscInstanceListener listener);

    /**
     * Unsubscribe to stop receiving topic adverts with instances information
     *
     * @param listener listener that will receive the notifications of adverts related to the subscribed topic
     */
    void unsubscribeFromInstances(final IAutodiscInstanceListener listener);

    /**
     * Subscribe to receive topic adverts for the given topic and transport type
     *
     * @param topicName name of the topic to subscribeToSubscribers to
     * @param transportType transport type and direction
     * @param listener listener that will receive the notifications of adverts related to the subscribed topic
     */
    void subscribeToTopic(final String topicName, final AutoDiscTransportType transportType, final IAutodiscTopicSubListener listener);

    /**
     * Unsubscribe from topic adverts for the given topic and transport type
     *
     * @param topicName name of the topic to unsubscribeFromSubscribers from
     * @param transportType transport type and direction
     * @param listener listener that was receiving the adverts of the subcribed topic
     */
    void unsubscribeFromTopic(final String topicName, final AutoDiscTransportType transportType, final IAutodiscTopicSubListener listener);

    /**
     * Add a listener to receive events related to added or removed publisher topic names on the domain.
     *
     * It will be triggered by topic name, not topic info, by grouping the information of all topic infos with the same name
     *
     * @param topicPattern the java string pattern to check the topic name against
     * @param listener the listener for events triggered due to the pattern
     */
    void subscribeToPubTopicPattern(final String topicPattern, final IAutodiscPubTopicPatternListener listener);

    /**
     * Remove the listener for events related to publisher topics with the given pattern
     *
     * @param topicPattern the java string pattern to check the topic name against
     */
    void unsubscribeFromPubTopicPattern(final String topicPattern);
}
