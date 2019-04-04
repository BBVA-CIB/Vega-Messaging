package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.msg.IRcvMessage;
import com.bbva.kyof.vega.msg.IRcvRequest;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * Represent a subscription to a topic name and keep track of all the AeronSubscribers sockets that are related to the topic name.
 *
 * The class assumes that listeners changes and aeron subscriber changes are always performed in thread-safe mode. It allows concurrent actions
 * of change listeners and receive messages.
 */
@Slf4j
class TopicSubscriber implements Closeable
{
    /** Topic Name the subscriber belongs to */
    @Getter private final String topicName;

    /** Unique id of the topic subscriber */
    @Getter private final UUID uniqueId = UUID.randomUUID();

    /** Listener for incoming messages due to normal subscription */
    private volatile ITopicSubListener normalListener;

    /** Listener for incoming messages due to pattern subscriptions */
    private final ConcurrentMap<String, ITopicSubListener> patternListenersByPattern = new ConcurrentHashMap<>();

    /** Return the topic configuration for this subscriber */
    @Getter private final TopicTemplateConfig topicConfig;

    /** Aeron subscribers related to this topic subscriber */
    private final Set<AeronSubscriber> aeronSubscribers = new HashSet<>();
    
    /**
     * Constructs a new topic subscriber
     *
     * @param topicName Topic name the subscriber is associated to
     * @param topicConfig Topic configuration
     */
    TopicSubscriber(final String topicName, final TopicTemplateConfig topicConfig)
    {
        this.topicName = topicName;
        this.topicConfig = topicConfig;
    }

    /**
     * Method called when a message is received.
     *
     * @param receivedMessage the received message
     */
    void onMessageReceived(final IRcvMessage receivedMessage)
    {
        final ITopicSubListener currentNormalListener = this.normalListener;

        if (currentNormalListener != null)
        {
            this.normalListener.onMessageReceived(receivedMessage);
        }

        if (!this.patternListenersByPattern.isEmpty())
        {
            this.patternListenersByPattern.forEach((key, value) -> value.onMessageReceived(receivedMessage));
        }
    }

    /**
     * Method called when a request message is received.
     *
     * @param receivedRequest the received request
     */
    void onRequestReceived(final IRcvRequest receivedRequest)
    {
        final ITopicSubListener currentNormalListener = this.normalListener;

        if (currentNormalListener != null)
        {
            this.normalListener.onRequestReceived(receivedRequest);
        }

        if (!this.patternListenersByPattern.isEmpty())
        {
            this.patternListenersByPattern.forEach((key, value) -> value.onRequestReceived(receivedRequest));
        }
    }

    /**
     * Remove the normal listener that was created due to a normal topic subscription for incoming messages and requests
     * @return true if removed, false if it was not settled
     */
    boolean removeNormalListener()
    {
        if (this.normalListener == null)
        {
            return false;
        }

        this.normalListener = null;
        return true;
    }

    /**
     * Set the normal listener created due to a normal topic subscription for incoming messages and requests
     *
     * @param listener the listener
     * @return false if is was already settled
     */
    boolean setNormalListener(final ITopicSubListener listener)
    {
        if (this.normalListener != null)
        {
            return false;
        }

        this.normalListener = listener;
        return true;
    }

    /**
     * Add a listener created due to a pattern subscription for incoming messages and requests
     *
     * @param pattern the pattern of the pattern subscription that has generated the listener
     * @param listener the listenr for messages
     * @return false if there was already a listener for the pattern
     */
    boolean addPatternListener(final String pattern, final ITopicSubListener listener)
    {
        return this.patternListenersByPattern.putIfAbsent(pattern, listener) == null;
    }

    /**
     * Remove a listener created due to a pattern subscription for incoming messages and requests
     *
     * @param pattern the pattern of the pattern subscription that has generated the listener
     * @return false if there was already no listener for the pattern
     */
    boolean removePatternListener(final String pattern)
    {
        return this.patternListenersByPattern.remove(pattern) != null;
    }

    /**
     * Return true if there are no more listeners
     */
    boolean hasNoListeners()
    {
        return this.normalListener == null && this.patternListenersByPattern.isEmpty();
    }

    /**
     * Add a related aeron subscriber that may receive topics for the topic name represented by the topic subscriber
     * @param subscriber the aeron subscriber
     */
    void addAeronSubscriber(final AeronSubscriber subscriber)
    {
        this.aeronSubscribers.add(subscriber);
    }

    /**
     * Remove a related aeron subscriber that may receive topics for the topic name represented by the topic subscriber
     * @param subscriber the aeron subscriber
     */
    void removeAeronSubscriber(final AeronSubscriber subscriber)
    {
        this.aeronSubscribers.remove(subscriber);
    }

    /** Close the topic subscriber cleaning all internal information */
    @Override
    public void close()
    {
        this.aeronSubscribers.clear();
        this.normalListener = null;
        this.patternListenersByPattern.clear();
    }

    /**
     * Run the given consumer function for all the internal related aeron subscribers
     * @param consumer the consumer that will be executed for each aeron subscriber
     */
    void runForEachRelatedAeronSubscriber(final Consumer<AeronSubscriber> consumer)
    {
        this.aeronSubscribers.forEach(consumer);
    }

    /**
     * True if the topic is configured to use security
     */
    public boolean hasSecurity()
    {
        return false;
    }
}
