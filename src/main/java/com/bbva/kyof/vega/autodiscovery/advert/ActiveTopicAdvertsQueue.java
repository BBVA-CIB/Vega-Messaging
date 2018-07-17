package com.bbva.kyof.vega.autodiscovery.advert;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.autodiscovery.model.IAutoDiscTopicInfo;
import com.bbva.kyof.vega.util.collection.HashMapOfHashSet;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * This class represents a queue of active topic adverts information that is going to be periodically checked for time outs.
 *
 * The queue stores the information on a HashMapStack by stored information unique id. This allows quick access
 * for information removal and existence check and at the same time by using a HashMapStack it keeps the order of
 * the added elements.
 *
 * The order of insertion is performed to ensure the next element that may time out is the last one added. Every
 * time an element times out, is removed and added at the end. Since the timeout interval is shared by all elements
 * this mechanism ensures that only checking the next element is enough to know if the next element has timed out.
 *
 * The information is also stored by topic name and transport type to allow quick search and iteration.
 *
 * This class is not thread safe!
 */
public class ActiveTopicAdvertsQueue<T extends IAutoDiscTopicInfo> extends ActiveAdvertsQueue<T>
{
    /** Unicast publisher topic adverts */
    private final HashMapOfHashSet<String, T> pubUniTopicAdverts = new HashMapOfHashSet<>();
    /** Multicast publisher topic adverts */
    private final HashMapOfHashSet<String, T> pubMulTopicAdverts = new HashMapOfHashSet<>();
    /** IPC publisher topic adverts */
    private final HashMapOfHashSet<String, T> pubIpcTopicAdverts = new HashMapOfHashSet<>();
    /** Unicast subscriber topic adverts */
    private final HashMapOfHashSet<String, T> subUniTopicAdverts = new HashMapOfHashSet<>();
    /** Multicast subscriber topic adverts */
    private final HashMapOfHashSet<String, T> subMulTopicAdverts = new HashMapOfHashSet<>();
    /** IPC subscriber topic adverts */
    private final HashMapOfHashSet<String, T> subIpcTopicAdverts = new HashMapOfHashSet<>();

    /**
     * Construct a new queue with the given timeout for added adverts
     * @param advertTimeout the timeout for the added adverts
     */
    public ActiveTopicAdvertsQueue(final long advertTimeout)
    {
        super(advertTimeout);
    }

    @Override
    public boolean addOrUpdateAdvert(final T advertInfo)
    {
        boolean added = super.addOrUpdateAdvert(advertInfo);

        if (added)
        {
            this.getAdvertsForTransport(advertInfo.getTransportType()).put(advertInfo.getTopicName(), advertInfo);
        }

        return added;
    }

    @Override
    public T returnNextTimedOutElement()
    {
        final T result = super.returnNextTimedOutElement();

        if (result != null)
        {
            this.getAdvertsForTransport(result.getTransportType()).remove(result.getTopicName(), result);
        }

        return result;
    }

    /**
     * Consume the internal stored elements if there is a match of topic name and transport type
     *
     * @param topicName topic name to match against
     * @param transportType transport type to match against
     * @param consumer the consumer for the elements that match
     */
    public void consumeIfMatch(final String topicName, final AutoDiscTransportType transportType, final Consumer<T> consumer)
    {
        this.getAdvertsForTransport(transportType).consumeIfKeyEquals(topicName, consumer);
    }

    /**
     * Consume the internal stored elements if the topic match the given predicate
     *
     * @param topicFilter predicate the topic should match
     * @param consumer the consumer for the elements that match
     */
    public void consumeIfTopicMatchFilter(final Predicate<String> topicFilter, final Consumer<T> consumer)
    {
        this.pubUniTopicAdverts.consumeIfKeyMatchFilter(topicFilter, consumer);
        this.pubMulTopicAdverts.consumeIfKeyMatchFilter(topicFilter, consumer);
        this.pubIpcTopicAdverts.consumeIfKeyMatchFilter(topicFilter, consumer);
        this.subUniTopicAdverts.consumeIfKeyMatchFilter(topicFilter, consumer);
        this.subMulTopicAdverts.consumeIfKeyMatchFilter(topicFilter, consumer);
        this.subIpcTopicAdverts.consumeIfKeyMatchFilter(topicFilter, consumer);
    }

    @Override
    public void clear()
    {
        super.clear();
        this.pubUniTopicAdverts.clear();
        this.pubMulTopicAdverts.clear();
        this.pubIpcTopicAdverts.clear();
        this.subUniTopicAdverts.clear();
        this.subMulTopicAdverts.clear();
        this.subIpcTopicAdverts.clear();
    }

    /**
     * Return the adverts that match the given transport
     * @param transportType the transport to check
     * @return the adverts that match the given transport
     */
    private HashMapOfHashSet<String, T> getAdvertsForTransport(final AutoDiscTransportType transportType)
    {
        switch (transportType)
        {
            case PUB_UNI:
                return this.pubUniTopicAdverts;
            case PUB_MUL:
                return this.pubMulTopicAdverts;
            case SUB_UNI:
                return this.subUniTopicAdverts;
            case SUB_MUL:
                return this.subMulTopicAdverts;
            case PUB_IPC:
                return this.pubIpcTopicAdverts;
            case SUB_IPC:
                return this.subIpcTopicAdverts;
            default:
                throw new IllegalArgumentException("Invalid transport type" + transportType);
        }
    }
}
