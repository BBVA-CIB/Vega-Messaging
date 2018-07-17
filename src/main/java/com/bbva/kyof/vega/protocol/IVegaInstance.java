package com.bbva.kyof.vega.protocol;

import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.protocol.publisher.ITopicPublisher;
import com.bbva.kyof.vega.protocol.subscriber.ITopicSubListener;

import java.io.Closeable;
import java.util.UUID;

/**
 * Interface for the manager. The manager is the main class of the library
 */
public interface IVegaInstance extends Closeable
{
    /**
     * Return the instance id of this instance
     * @return the instance id of this instance
     */
    UUID getInstanceId();

    /**
     * Create a new publisher to sendMsg messages into a topic
     *
     * It is not possible to create 2 publishers for the same topic unless the publisher is destroyed first.
     *
     * Each publisher instance is associated to an unique given topic.
     *
     * @param topic The topic to publish into.
     * @return the created publisher
     * @throws VegaException exception thrown if there is a problem in the subscription or the topic is not configured
     */
    ITopicPublisher createPublisher(final String topic) throws VegaException;

    /**
     * Destroys the publisher for the given topic.
     *
     * @param topic The topic of the publisher to destroy
     * @throws VegaException exception thrown if there is a problem destroying the publisher
     */
    void destroyPublisher(final String topic) throws VegaException;

    
    /**
     * Subscribes to the given topic in order to get messages from it.
     * 
     * You cannot subscribeToSubscribers to a topic twice.
     * 
     * @param topicName   Topic name to subscribeToSubscribers to.
     * @param listener    The Listener where the user wants to receive the messages.
     * @throws VegaException exception thrown if there is a problem or if it is already subscribed
     */
    void subscribeToTopic(final String topicName, final ITopicSubListener listener) throws VegaException;

    /**
     * Unsubscribe from a topicName.
     *
     * @param topicName Topic name that will be unsubscribed from. Allowed {@link String}
     * @throws VegaException exception thrown if there is a problem or if it is already unsubscribed
     */
    void unsubscribeFromTopic(final String topicName) throws VegaException;

    /**
     * Subscribes to topics that match the given pattern in order to get messages from it.
     *
     * It will also trigger notifications on topics created by the same instance. <p>
     *
     * Important! If a topic match a normal subscription and pattern subscription or even multiple pattern subscriptions and
     * they share the listener, the event will be received on the listener multiple times, once per subscription. <p>
     *
     * Important! Messages will only be received for configured topics that match the pattern.
     *
     * @param topicPattern the topic pattern in Java pattern format
     * @param listener The listener where the user wants to receive the messages.
     *
     * @throws VegaException exception thrown if there is a problem or if it is already subscribed tot he pattern
     */
    void subscribeToPattern(final String topicPattern, final ITopicSubListener listener) throws VegaException;

    /**
     * Unsubscribe from a topic pattern.
     *
     * @param topicPattern the topic pattern to unsubscribe from
     * @throws VegaException exception thrown if there is any problem in the un-subscription
     */
    void unsubscribeFromPattern(final String topicPattern) throws VegaException;
}