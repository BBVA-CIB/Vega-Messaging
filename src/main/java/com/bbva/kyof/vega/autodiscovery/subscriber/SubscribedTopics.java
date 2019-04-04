package com.bbva.kyof.vega.autodiscovery.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.util.collection.HashMapOfHashSet;

/**
 * Helper class to keep track of all subscribed topics by transport type and name
 */
class SubscribedTopics
{
    /** Unicast publisher topic adverts */
    private final HashMapOfHashSet<String, IAutodiscTopicSubListener> pubUniTopics = new HashMapOfHashSet<>();
    /** Multicast publisher topic adverts */
    private final HashMapOfHashSet<String, IAutodiscTopicSubListener> pubMulTopics = new HashMapOfHashSet<>();
    /** IPC publisher topic adverts */
    private final HashMapOfHashSet<String, IAutodiscTopicSubListener> pubIpcTopics = new HashMapOfHashSet<>();
    /** Unicast subscriber topic adverts */
    private final HashMapOfHashSet<String, IAutodiscTopicSubListener> subUniTopics = new HashMapOfHashSet<>();
    /** Multicast subscriber topic adverts */
    private final HashMapOfHashSet<String, IAutodiscTopicSubListener> subMulTopics = new HashMapOfHashSet<>();
    /** IPC subscriber topic adverts */
    private final HashMapOfHashSet<String, IAutodiscTopicSubListener> subIpcTopics = new HashMapOfHashSet<>();

    /**
     * Return all the subscribed topics for the given transport
     * @param transportType the topic transport
     * @return the subscribed topics
     */
    HashMapOfHashSet<String, IAutodiscTopicSubListener> getSubTopicsForTransport(final AutoDiscTransportType transportType)
    {
        switch (transportType)
        {
            case PUB_UNI:
                return this.pubUniTopics;
            case PUB_MUL:
                return this.pubMulTopics;
            case SUB_UNI:
                return this.subUniTopics;
            case SUB_MUL:
                return this.subMulTopics;
            case PUB_IPC:
                return this.pubIpcTopics;
            case SUB_IPC:
                return this.subIpcTopics;
            default:
                throw new IllegalArgumentException("Invalid transport type" + transportType);
        }
    }

    /**
     * Add a new listener for a topic name and transport type. It will ignore it if already exists.
     * @param transportType the transport type
     * @param topicName the name of the topic
     * @param listener the listener to get information about new topic info or topic socket info elements
     * @return true if the element has been added, false it it was already there
     */
    boolean addListener(final AutoDiscTransportType transportType, final String topicName, final IAutodiscTopicSubListener listener)
    {
        final HashMapOfHashSet<String, IAutodiscTopicSubListener> subscribedTopicsForType = this.getSubTopicsForTransport(transportType);
        return subscribedTopicsForType.put(topicName, listener);
    }

    /**
     * Remove a new listener for a topic name and transport type. It will ignore it if doesn't exists.
     * @param transportType the transport type
     * @param topicName the name of the topic
     * @param listener the listener to get information about new topic info or topic socket info elements
     @return true if the element has been removed, false it it was already there
     */
    boolean removeListener(final AutoDiscTransportType transportType, final String topicName, final IAutodiscTopicSubListener listener)
    {
        final HashMapOfHashSet<String, IAutodiscTopicSubListener> subscribedTopicsForType = this.getSubTopicsForTransport(transportType);
        return subscribedTopicsForType.remove(topicName, listener);
    }

    /**
     * Called to notify there is a new topic socket information, it will tell all the listeners that match
     * @param info the topic socket info
     */
    void onNewTopicSocketInfo(final AutoDiscTopicSocketInfo info)
    {
        this.getSubTopicsForTransport(info.getTransportType()).consumeIfKeyEquals(info.getTopicName(), listener -> listener.onNewAutoDiscTopicSocketInfo(info));
    }

    /**
     * Called to notify there is a new topic information, it will tell all the listeners that match
     * @param info the topic info
     */
    void onNewTopicInfo(final AutoDiscTopicInfo info)
    {
        this.getSubTopicsForTransport(info.getTransportType()).consumeIfKeyEquals(info.getTopicName(), listener -> listener.onNewAutoDiscTopicInfo(info));
    }

    /**
     * Called to notify there is a removed topic socket information, it will tell all the listeners that match
     * @param info the topic socket info
     */
    void onTimedOutTopicSocketInfo(final AutoDiscTopicSocketInfo info)
    {
        this.getSubTopicsForTransport(info.getTransportType()).consumeIfKeyEquals(info.getTopicName(), listener -> listener.onTimedOutAutoDiscTopicSocketInfo(info));
    }

    /**
     * Called to notify there is a removed topic information, it will tell all the listeners that match
     * @param info the topic info
     */
    void onTimedOutTopicInfo(final AutoDiscTopicInfo info)
    {
        this.getSubTopicsForTransport(info.getTransportType()).consumeIfKeyEquals(info.getTopicName(), listener -> listener.onTimedOutAutoDiscTopicInfo(info));
    }

    /**
     * Remove all the internal information
     */
    void clear()
    {
        this.pubUniTopics.clear();
        this.pubMulTopics.clear();
        this.subUniTopics.clear();
        this.subMulTopics.clear();
        this.pubIpcTopics.clear();
        this.subIpcTopics.clear();
    }
}
