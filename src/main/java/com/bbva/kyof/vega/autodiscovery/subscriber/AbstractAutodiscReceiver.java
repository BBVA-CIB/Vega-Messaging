package com.bbva.kyof.vega.autodiscovery.subscriber;

import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.autodiscovery.advert.ActiveAdvertsQueue;
import com.bbva.kyof.vega.autodiscovery.advert.ActiveTopicAdvertsQueue;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.msg.BaseHeader;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.collection.NativeArraySet;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import lombok.extern.slf4j.Slf4j;
import org.agrona.DirectBuffer;

import java.io.Closeable;
import java.util.UUID;

/**
 * Abstract class that is the base of specific implementations of the subscription functionality for auto-discovery.<p>
 *
 * The class handles all subscription related features. The information received for subscribed elements is stored when
 * there is a new element with a time out. When there is a timeout the active advert stored is removed and a notification
 * is sent.<p>
 *
 * The class allows to onNewPubTopicForPattern to topic adverts by name and pattern and also track record of topic subscriptions due to
 * topic registrations on the publisher.<p>
 *
 * Topic Infos are considered alive as soon as there is interest in them from a topic subscription, topic subscription due to topic
 * registration or due to a pattern subscription.<p>
 *
 * Topic Socket infos are only keep alive if there is a direct subscription.<p>
 *
 * Instance infos are always listened to.<p>
 *
 * The class is not thread-safe!
 */
@Slf4j
public abstract class AbstractAutodiscReceiver implements Closeable
{
    /** Subscription connection with Aeron to receive auto-discovery messages */
    private final Subscription subscription;

    /** Reusable unsafe buffer serializer to wrap incoming messages */
    private final UnsafeBufferSerializer bufferSerializer = new UnsafeBufferSerializer();

    /** Reusable base header to deserialize the header of incoming messages */
    private final BaseHeader reusableBaseHeader = new BaseHeader();

    /** Reusable auto-discovery instance info used to avoid object creation during deserialization */
    private AutoDiscInstanceInfo instanceInfo = new AutoDiscInstanceInfo();

    /** Reusable auto-discovery topic socket info used to avoid object creation during deserialization */
    private AutoDiscTopicSocketInfo topicSocketInfo = new AutoDiscTopicSocketInfo();

    /** Reusable auto-discovery topic info used to avoid object creation during deserialization */
    private AutoDiscTopicInfo topicInfo = new AutoDiscTopicInfo();

    /** Stores all the subscribed topics and listeners for each one of them */
    private final SubscribedTopics subscribedTopics = new SubscribedTopics();

    /** Store the queue with all the active instance information adverts */
    private final ActiveAdvertsQueue<AutoDiscInstanceInfo> instanceInfoActiveAdvertsQueue;

    /** Store the queue with all the active topic information adverts */
    private final ActiveTopicAdvertsQueue<AutoDiscTopicInfo> topicInfoActiveAdvertsQueue;

    /** Store the queue with all the active topic-socket pair information adverts */
    private final ActiveTopicAdvertsQueue<AutoDiscTopicSocketInfo> topicSocketActiveAdvertsQueue;

    /** Object that handles all the topic pattern subscriptions */
    private final PubPatternSubscriptionsManager pubPatternSubscriptionsManager = new PubPatternSubscriptionsManager();

    /** Global listener that has to receive the events always */
    private final IAutodiscGlobalEventListener globalListener;

    /** Listeners for instance subscriptions */
    private final NativeArraySet<IAutodiscInstanceListener> instancesSubListeners = new NativeArraySet<>(IAutodiscInstanceListener.class, 1);

    /**
     * Create a new auto-discovery subscriber
     *
     * @param instanceId unique id of the vega library instance this object belongs to
     * @param aeron the Aeron instance
     * @param config the auto-discovery configuration
     * @param globalListener Global listener that has to receive the events always
     */
    AbstractAutodiscReceiver(final UUID instanceId,
                             final Aeron aeron,
                             final AutoDiscoveryConfig config,
                             final IAutodiscGlobalEventListener globalListener)
    {
        // Create the queues with the timeout of the configuration
        this.instanceInfoActiveAdvertsQueue = new ActiveAdvertsQueue<>(config.getTimeout());
        this.topicInfoActiveAdvertsQueue = new ActiveTopicAdvertsQueue<>(config.getTimeout());
        this.topicSocketActiveAdvertsQueue = new ActiveTopicAdvertsQueue<>(config.getTimeout());
        this.globalListener = globalListener;

        // Create the Aeron subscription
        this.subscription = this.createSubscription(instanceId, aeron, config);
    }

    /**
     * Create a the subscription for incoming messages.
     *
     * @param instanceId unique id of the vega library instance this object belongs to
     * @param aeron aeron instance
     * @param config auto-discovery configuration
     *
     * @return the created subscription
     */
    public abstract Subscription createSubscription(UUID instanceId, Aeron aeron, AutoDiscoveryConfig config);

    @Override
    public void close()
    {
        log.info("Closing auto discovery receiver manager");

        // Close subscription
        this.subscription.close();

        // Clear internal queues
        this.topicInfoActiveAdvertsQueue.clear();
        this.topicSocketActiveAdvertsQueue.clear();
        this.instanceInfoActiveAdvertsQueue.clear();

        // Clean pattern subscribers
        this.pubPatternSubscriptionsManager.clear();

        // Clean subscribed topics
        this.subscribedTopics.clear();
    }

    /**
     * Subscribe to instances events
     * @param listener listener for events related to the instances
     * @return true if the listener has been added, false if it was already there
     */
    public boolean subscribeToInstances(final IAutodiscInstanceListener listener)
    {
        // Add to the list of listeners
        if (this.instancesSubListeners.addElement(listener))
        {
            // If there is already instances information stored, notify the listener immediately
            this.instanceInfoActiveAdvertsQueue.runForEachElement(listener::onNewAutoDiscInstanceInfo);

            return true;
        }

        return false;
    }

    /**
     * Unsubscribe from instances events
     * @param listener listener for events related to the instances
     * @return true if the listener has been removed, false if it was not there
     */
    public boolean unsubscribeFromInstances(final IAutodiscInstanceListener listener)
    {
        // Remove from the lsit of listeners
        return this.instancesSubListeners.removeElement(listener);
    }

    /**
     * Subscribe to topic info and topic socket info adverts that match the given topic name and transport type.
     *
     * If already subscribed the call will be ignored.
     *
     * @param topicName name of the topic to onNewPubTopicForPattern to
     * @param transportType auto-discovery transport type
     * @param listener listener for events of this subscription
     *
     * @return true if subscribed, false if already subscribed
     */
    public boolean subscribeToTopic(final String topicName, final AutoDiscTransportType transportType, final IAutodiscTopicSubListener listener)
    {
        log.debug("Subscribing to topic [{}] and transport [{}]", topicName, transportType);

        // Add to subscribed topic
        if (this.subscribedTopics.addListener(transportType, topicName, listener))
        {
            // If there is already any topic or topic socket information stored that match the name and transport, notify the listeners immediately
            this.topicInfoActiveAdvertsQueue.consumeIfMatch(topicName, transportType, listener::onNewAutoDiscTopicInfo);
            this.topicSocketActiveAdvertsQueue.consumeIfMatch(topicName, transportType, listener::onNewAutoDiscTopicSocketInfo);
            return true;
        }

        return false;
    }

    /**
     * Unsubscribe from topic info and topic socket info adverts that match the given topic name and transport type.
     *
     * If not subscribed the call will be ignored.
     * @param topicName name of the topic to onNewPubTopicForPattern to
     * @param transportType auto-discovery transport type
     * @param listener listener for events of this subscription
     *
     * @return true if unsubscribed, false if it was not subscribed
     */
    public boolean unsubscribeFromTopic(final String topicName, final AutoDiscTransportType transportType, final IAutodiscTopicSubListener listener)
    {
        log.debug("Unsubscribing from topic [{}] and transport [{}]", topicName, transportType);

        // Remove subscribed topic
        return this.subscribedTopics.removeListener(transportType, topicName, listener);
    }

    /**
     * Subscribe to topic information messages for topics that match the given pattern
     *
     * @param pattern the pattern to check against
     * @param patternListener the listener that will receive events related to new or removed topic infos that matches the pattern
     * @return true if subscribed, false if already subscribed to pattern
     */
    public boolean subscribeToPubPattern(final String pattern, final IAutodiscPubTopicPatternListener patternListener)
    {
        log.debug("Subscribe to topic pattern [{}]", pattern);

        // Create the pattern subscriber
        final PubPatternSubscription pubPatternSubscription = this.pubPatternSubscriptionsManager.subscribeToPattern(pattern, patternListener);

        // If null it means it is already subscribed
        if (pubPatternSubscription == null)
        {
            return false;
        }

        // Tell the created pattern subscription about all the existing adverts registered
        this.topicInfoActiveAdvertsQueue.consumeIfTopicMatchFilter(pubPatternSubscription::match, pubPatternSubscription::onNewAdvert);

        return true;
    }

    /**
     * Unsubscribe from topic information messages for topics that match the given pattern
     *
     * @param pattern the pattern to check against
     * @return true if unsubscribed, false if it was not subscribed
     */
    public boolean unsubscribeFromPubPattern(final String pattern)
    {
        log.debug("Unsubscribe from topic pattern [{}]", pattern);

        // Remove pattern subscription
        return this.pubPatternSubscriptionsManager.unsubscribeFromPattern(pattern);
    }

    /**
     * Poll for the next received message in the subscriber
     * @return the number of messages received
     */
    public int pollNextMessage()
    {
        return this.subscription.poll(this::processSubscriptionRcvMsg, 1);
    }

    /**
     * Check the next element in the internal list of active adverts for a timeout.<p>
     *
     * It will check for the 3 active advert types, TopicInfo, TopicSocketInfo and InstanceInfo.<p>
     *
     * If the advert has timed out, it will remove the element and notify the listener.<p>
     *
     * The method don't have to go through all the elements stored, since they are always sorted in the internal
     * queues by timeout time. Just checking the oldest element in the queue is enough.<p>
     *
     * @return the number of time outs processed
     */
    public int checkNextTimeout()
    {
        // Store the number of timed out elements
        int numTimeOuts = checkTopicInfoTimeouts();
        numTimeOuts += checkTopicSocketInfoTimeouts();
        numTimeOuts += checkInstanceInfoTimeouts();
        return numTimeOuts;
    }

    /**
     * Check for time out on instance info active adverts
     * @return the number of timeouts
     */
    private int checkInstanceInfoTimeouts()
    {
        final AutoDiscInstanceInfo timedOutInstanceInfo = this.instanceInfoActiveAdvertsQueue.returnNextTimedOutElement();
        if (timedOutInstanceInfo != null)
        {
            // Notify about the new removal to all listeners
            this.instancesSubListeners.consumeAll(element -> element.onTimedOutAutoDiscInstanceInfo(timedOutInstanceInfo));
            return 1;
        }
        return 0;
    }

    /**
     * Check for time out on topic socket info active adverts
     * @return the number of timeouts
     */
    private int checkTopicSocketInfoTimeouts()
    {
        // Check timeout in topic-socket info
        final AutoDiscTopicSocketInfo timedOutTopicSocketInfo = this.topicSocketActiveAdvertsQueue.returnNextTimedOutElement();
        if (timedOutTopicSocketInfo != null)
        {
            // Notify to the subscribed
            this.subscribedTopics.onTimedOutTopicSocketInfo(timedOutTopicSocketInfo);
            return 1;
        }
        return 0;
    }

    /**
     * Check for time out on topic info active adverts
     * @return the number of timeouts
     */
    private int checkTopicInfoTimeouts()
    {
        // Check timeout in topic infos
        final AutoDiscTopicInfo timedOutTopicInfo = this.topicInfoActiveAdvertsQueue.returnNextTimedOutElement();
        if (timedOutTopicInfo != null)
        {
            // Notify to the subscribed
            this.subscribedTopics.onTimedOutTopicInfo(timedOutTopicInfo);

            // Finally notify all the pattern subscribers, they will internally check if the topic matches
            this.pubPatternSubscriptionsManager.onTopicInfoTimedOut(timedOutTopicInfo);

            return 1;
        }

        return 0;
    }

    /**
     * Process a received message from the Aeron Subscription. It is called by the fragment handler on message poll.
     *
     * @param buffer the buffer that contains the received message
     * @param offset the offset inside the buffer
     * @param length the length of the message in the buffer
     * @param aeronHeader the header of the Aeron message
     */
    private void processSubscriptionRcvMsg(final DirectBuffer buffer, final int offset, final int length, final Header aeronHeader)
    {
        try
        {
            // Wrap the buffer into the serializer
            this.bufferSerializer.wrap(buffer, offset, length);

            // Read the base header
            this.reusableBaseHeader.fromBinary(this.bufferSerializer);

            // Check the version
            if (!this.reusableBaseHeader.isVersionCompatible())
            {
                log.warn("Autodiscovery message received from incompatible library version [{}]", Version.toStringRep(this.reusableBaseHeader.getVersion()));
                return;
            }

            // Check the message type and process
            switch (this.reusableBaseHeader.getMsgType())
            {
                case MsgType.AUTO_DISC_TOPIC:
                    this.onReceivedTopicInfoMsg();
                    break;
                case MsgType.AUTO_DISC_TOPIC_SOCKET:
                    this.onReceivedTopicSocketInfoMsg();
                    break;
                case MsgType.AUTO_DISC_INSTANCE:
                    this.onReceivedInstanceInfoMsg();
                    break;
                default:
                    log.warn("Wrong message type [{}] received on autodiscovery", this.reusableBaseHeader.getMsgType());
                    break;
            }
        }
        catch (final RuntimeException e)
        {
            log.error("Unexpected error processing received autodiscovery message", e);
        }
    }

    /**
     * Process a received message with information about a pair of topic-socket
     */
    private void onReceivedTopicSocketInfoMsg()
    {
        // Deserialize the message
        this.topicSocketInfo.fromBinary(this.bufferSerializer);

        if (log.isTraceEnabled())
        {
            log.trace("Processing received topic socket pair info message [{}]", this.topicSocketInfo);
        }

        // Add or update, if false is an update and there is nothing else to do
        if (!this.topicSocketActiveAdvertsQueue.addOrUpdateAdvert(this.topicSocketInfo))
        {
            return;
        }

        log.debug("New topic socket information [{}]", this.topicSocketInfo);

        // Notify about the new addition if there is any interested listener
        this.subscribedTopics.onNewTopicSocketInfo(this.topicSocketInfo);

        // Restart the reusable auto-discovery info object since the original has been stored
        this.topicSocketInfo = new AutoDiscTopicSocketInfo();
    }

    /**
     * Process a received message with information about a topic publisher or subscriber
     */
    private void onReceivedTopicInfoMsg()
    {
        // Deserialize the message
        this.topicInfo.fromBinary(this.bufferSerializer);

        if (log.isTraceEnabled())
        {
            log.trace("Processing received topic socket info message [{}]", this.topicInfo);
        }

        // Add or update, if is just an update there is nothing else to do
        if (!this.topicInfoActiveAdvertsQueue.addOrUpdateAdvert(this.topicInfo))
        {
            return;
        }

        log.debug("New topic information [{}]", this.topicInfo);

        // Notify about the new addition if there is any interested listener
        this.subscribedTopics.onNewTopicInfo(this.topicInfo);

        // Notify the pattern subscribers, they will internally decide if notify their listeners or not
        this.pubPatternSubscriptionsManager.onNewTopicInfo(this.topicInfo);

        // Notify the global listener
        this.globalListener.onNewTopicInfo(this.topicInfo);

        // Restart the reusable auto-discovery info object since the original has been stored
        this.topicInfo = new AutoDiscTopicInfo();
    }

    /**
     * Process a received message with information about a library instance
     */
    private void onReceivedInstanceInfoMsg()
    {
        // Deserialize the message
        this.instanceInfo.fromBinary(this.bufferSerializer);

        if (log.isTraceEnabled())
        {
            log.trace("Processing received instance info message [{}]", this.instanceInfo);
        }

        // If is an update, there is nothing else to do
        if (!this.instanceInfoActiveAdvertsQueue.addOrUpdateAdvert(this.instanceInfo))
        {
            return;
        }

        log.debug("New instance information [{}]", this.instanceInfo);

        // Notify about the new addition to all listeners
        this.instancesSubListeners.consumeAll(element -> element.onNewAutoDiscInstanceInfo(instanceInfo));

        // Notify the global listener
        this.globalListener.onNewInstanceInfo(this.instanceInfo);

        // Restart the reusable auto-discovery info object since the original has been stored
        this.instanceInfo = new AutoDiscInstanceInfo();
    }
}
