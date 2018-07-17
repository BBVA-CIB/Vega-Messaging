package com.bbva.kyof.vega.autodiscovery.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;

import java.util.HashMap;
import java.util.Map;

/**
 * Class that manages all the subscriptions to topic name patterns. It makes sure a topic pattern is not subscribed twice
 * and that a new topic info advert or a timed out info advert is check against all topic pattern subscriptions.
 */
class PubPatternSubscriptionsManager
{
    /** Stores all the pattern subscriptions by string pattern */
    private final Map<String, PubPatternSubscription> patternSubscribersByPattern = new HashMap<>();

    /**
     * Called when an stored topic info advert times out.
     *
     * It will call all the pattern subscriptions with the time out information and the subscription will decide if they should
     * process it and if a notification is required.
     *
     * @param timedOutTopicInfo the topic info that has timed out
     */
    void onTopicInfoTimedOut(final AutoDiscTopicInfo timedOutTopicInfo)
    {
        // Only interested on publisher events
        if (timedOutTopicInfo.getTransportType().isSubscriber())
        {
            return;
        }

        // Notify all pattern subscriptions
        this.patternSubscribersByPattern.forEach((key, value) -> value.onAdvertTimedOut(timedOutTopicInfo));
    }

    /**
     * Called when a new topic info advert is created.
     *
     * It will call all the pattern subscriptions with the time out information and the subscription will decide if they should
     * process it and if a notification is required.
     *
     * @param newTopicInfo the topic info that has timed out
     */
    void onNewTopicInfo(final AutoDiscTopicInfo newTopicInfo)
    {
        // Only interested on publisher events
        if (newTopicInfo.getTransportType().isSubscriber())
        {
            return;
        }

        // Notify all pattern subscriptions
        this.patternSubscribersByPattern.forEach((key, value) -> value.onNewAdvert(newTopicInfo));
    }

    /**
     * Subscribe to process topic info adverts whose topic name matches the given pattern.
     *
     * @param pattern the pattern to onNewPubTopicForPattern to
     * @param patternListener the listener for events on topic info adverts related to the pattern
     *
     * @return the created pattern subscription or null if already subscribed
     */
    PubPatternSubscription subscribeToPattern(final String pattern, final IAutodiscPubTopicPatternListener patternListener)
    {
        if (this.patternSubscribersByPattern.containsKey(pattern))
        {
            return null;
        }

        // Check if already subscribed, if not create a new pattern subscription
        final PubPatternSubscription subscription = new PubPatternSubscription(pattern, patternListener);
        this.patternSubscribersByPattern.put(pattern, subscription);

        return subscription;
    }

    /**
     * Unsubscribe from the given pattern and for each topic info contained the pattern subscription removed, execute the consumer.
     *
     * @param pattern pattern to unsubscribe from
     *
     * @return true if the listener has been removed
     */
    boolean unsubscribeFromPattern(final String pattern)
    {
        // Find the pattern subscription that match the pattern
        final PubPatternSubscription pubPatternSubscription = this.patternSubscribersByPattern.remove(pattern);

        return pubPatternSubscription != null;
    }

    /**
     * Clear everything
     */
    public void clear()
    {
        this.patternSubscribersByPattern.forEach((key, value) -> value.clear());
        this.patternSubscribersByPattern.clear();
    }
}
