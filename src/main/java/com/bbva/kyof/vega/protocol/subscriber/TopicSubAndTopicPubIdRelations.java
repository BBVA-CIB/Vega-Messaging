package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.util.collection.HashMapOfHashSet;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class helps to manage the relationships between topic subscribers and topic publishers.<p>
 *
 * The idea is to store for each topic publisher id, the related topic subscriber if it exists and also for each topic
 * subscriber store the set of topic publishers ids that are related to it.<p>
 *
 * The topic publishers can be local or remote from other instances.<p>
 *
 * This class is thread safe
 */
class TopicSubAndTopicPubIdRelations
{
    /**
     * Store the topic subscribers by the topic publisher id that publish on the same topic name. <p>
     */
    private final Map<UUID, TopicSubscriber> topicSubsByTopicPubId = new ConcurrentHashMap<>();

    /**
     * Set of topic publisher id's by topic subscriber id. This map is used during unsubscriptions
     */
    private final HashMapOfHashSet<UUID, UUID> topicPubsByTopicSubId = new HashMapOfHashSet<>();

    /** Lock for instance synchronization */
    private final Object lock = new Object();

    /**
     * Add a relationshipt between a topic publisher id and a topic subscriber. Both should share the topic name.
     *
     * The topic publisher can be local or remote.
     *
     * @param topicPubId the topic publisher id
     * @param topicSubscriber the topic subscriber
     */
    void addTopicPubRelation(final UUID topicPubId, final TopicSubscriber topicSubscriber)
    {
        synchronized (this.lock)
        {
            this.topicSubsByTopicPubId.put(topicPubId, topicSubscriber);
            this.topicPubsByTopicSubId.put(topicSubscriber.getUniqueId(), topicPubId);
        }
    }

    /**
     * Remove the relationshipt between a topic publisher id and a topic subscriber. Both should share the topic name.
     *
     * The topic publisher can be local or remote.
     *
     * @param topicPubId the topic publisher id
     * @param topicSubscriber the topic subscriber
     */
    void removeTopicPubRelation(final UUID topicPubId, final TopicSubscriber topicSubscriber)
    {
        synchronized (this.lock)
        {
            this.topicSubsByTopicPubId.remove(topicPubId);
            this.topicPubsByTopicSubId.remove(topicSubscriber.getUniqueId(), topicPubId);
        }
    }

    /**
     * Remove a topic subscriber. It will also delete any publisher id related to the topic subscriber.
     *
     * @param topicSubscriber the topic subscriber to remove
     */
    void removeTopicSubscriber(final TopicSubscriber topicSubscriber)
    {
        synchronized (this.lock)
        {
            this.topicPubsByTopicSubId.removeAndConsumeIfKeyEquals(topicSubscriber.getUniqueId(), this.topicSubsByTopicPubId::remove);
        }
    }

    /**
     * Clear all internal information
     */
    public void clear()
    {
        synchronized (this.lock)
        {
            this.topicPubsByTopicSubId.clear();
            this.topicSubsByTopicPubId.clear();
        }
    }

    /**
     * Return the topic subscriber that is related to the given topic publisher id if any
     * @param topicPublisherId the topic publisher id to look for
     * @return the topic subscriber that matches, null in none
     */
    TopicSubscriber getTopicSubscriberForTopicPublisherId(final UUID topicPublisherId)
    {
        return this.topicSubsByTopicPubId.get(topicPublisherId);
    }
}
