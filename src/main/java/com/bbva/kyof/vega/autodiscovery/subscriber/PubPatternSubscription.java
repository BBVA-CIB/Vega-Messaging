package com.bbva.kyof.vega.autodiscovery.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.util.collection.HashMapOfHashSet;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a subscription to receive information about topics added and removed from the domain that match the given pattern.
 *
 * When a pattern subscription is started it will listen to any topic of any transport that match the pattern, both pub and sub.
 *
 * The subscription will send a a notification on the listener for each topic name added. This means that if there are multiple TopicInfos that
 * match the same topic name, it will only send one event. On the same hand the topic removed event will only be triggered when there are no
 * more TopicInfo adverts active for the topic.
 */
class PubPatternSubscription
{
    /** Pattern matcher that will be reseted and reused for each pattern check */
    private final Matcher matcher;
    /** The pattern in String format */
    private final String pattern;
    /** Listener for events derived from topics additions of removals that match the pattern */
    private final IAutodiscPubTopicPatternListener listener;
    /** Store all the AutoDiscTopicInfo objects representing a topic publishers that match the same topic name */
    private final HashMapOfHashSet<String, AutoDiscTopicInfo> topicPubInfosByTopicName = new HashMapOfHashSet<>();

    /**
     * Create a new pattern subscription for the given pattern and listener for events
     * @param pattern the pattern for this subscription
     */
    PubPatternSubscription(final String pattern, final IAutodiscPubTopicPatternListener listener)
    {
        this.pattern = pattern;
        this.listener = listener;

        // Create the matcher for the pattern
        this.matcher = Pattern.compile(pattern).matcher("");
    }

    /**
     * Called when there is a new created advert for a topic publisher or subscriber.
     *
     * It will store the info and notify if is a new topic name that was not registered
     *
     * @param topicInfo the information of the topic
     */
    void onNewAdvert(final AutoDiscTopicInfo topicInfo)
    {
        // Check if the topic matches and if it is subscriber, if not ignore
        if (!this.match(topicInfo.getTopicName()))
        {
            return;
        }

        // Check if we already have the topic name, on that case add but don't notify the listener
        if (this.topicPubInfosByTopicName.containsKey(topicInfo.getTopicName()))
        {
            this.topicPubInfosByTopicName.put(topicInfo.getTopicName(), topicInfo);
        }
        else
        {
            // We don't have the topic name yet, add and notify
            this.topicPubInfosByTopicName.put(topicInfo.getTopicName(), topicInfo);
            this.listener.onNewPubTopicForPattern(topicInfo, this.pattern);
        }
    }

    /**
     * Called when a topic publisher or subscriber information times out.
     *
     * It will remove the info and notify if there are no more topic infos for the topic name that has been removed.
     *
     * @param timedOutTopicInfo the information of the topic
     */
    void onAdvertTimedOut(final AutoDiscTopicInfo timedOutTopicInfo)
    {
        // Remove the information, if not contained don't do anything.
        if (!this.topicPubInfosByTopicName.remove(timedOutTopicInfo.getTopicName(), timedOutTopicInfo))
        {
            return;
        }

        // Check if there are more elements for that topic name, if not, notify
        if (!this.topicPubInfosByTopicName.containsKey(timedOutTopicInfo.getTopicName()))
        {
            this.listener.onPubTopicForPatternRemoved(timedOutTopicInfo, this.pattern);
        }
    }

    /**
     * Return true if the given topic name matches the pattern represented by this pattern subscription
     *
     * @param topicName the name of the topic to match against
     * @return true if they matches
     */
    public boolean match(final String topicName)
    {
        // Reset the matcher
        this.matcher.reset(topicName);

        // Check if the pattern matches
        return this.matcher.matches();
    }

    /** Clear the internal information */
    void clear()
    {
        this.topicPubInfosByTopicName.clear();
    }

    @Override
    public boolean equals(final Object target)
    {
        if (this == target)
        {
            return true;
        }
        if (target == null || getClass() != target.getClass())
        {
            return false;
        }

        PubPatternSubscription that = (PubPatternSubscription) target;

        return this.pattern.equals(that.pattern);
    }

    @Override
    public int hashCode()
    {
        return this.pattern.hashCode();
    }
}
