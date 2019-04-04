package com.bbva.kyof.vega.autodiscovery.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Created by cnebrera on 04/08/16.
 */
public class PubPatternSubscriptionTest
{
    @Test
    public void testMatchEqualsHashCode()
    {
        final ListenerImpl listener1 = new ListenerImpl();
        final ListenerImpl listener2 = new ListenerImpl();
        final PubPatternSubscription subscription1 = new PubPatternSubscription("a.*", listener1);
        final PubPatternSubscription subscription2 = new PubPatternSubscription("b.*", listener2);
        final PubPatternSubscription subscription3 = new PubPatternSubscription("a.*", listener2);

        Assert.assertTrue(subscription1.match("aca"));
        Assert.assertFalse(subscription1.match("ba"));
        Assert.assertTrue(subscription2.match("ba"));
        Assert.assertFalse(subscription2.match("aca"));

        Assert.assertEquals(subscription1, subscription1);
        Assert.assertEquals(subscription1, subscription3);
        Assert.assertNotEquals(subscription1, subscription2);
        Assert.assertNotEquals(subscription1, null);
        Assert.assertNotEquals(subscription1, new Object());

        Assert.assertTrue(subscription1.hashCode() == subscription3.hashCode());
        Assert.assertFalse(subscription1.hashCode() == subscription2.hashCode());
    }

    @Test
    public void testAdverts()
    {
        final UUID instanceId = UUID.randomUUID();
        final ListenerImpl listener = new ListenerImpl();
        final AutoDiscTopicInfo topicInfo1 = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "ab");
        final AutoDiscTopicInfo topicInfo2 = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "ab");
        final AutoDiscTopicInfo topicInfo3 = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "ac");
        final AutoDiscTopicInfo topicInfo4 = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "bb");

        final PubPatternSubscription subscription = new PubPatternSubscription("a.*", listener);

        // Call new adverts, since is the first time the topic is added, there should be a new subscription
        subscription.onNewAdvert(topicInfo1);
        Assert.assertTrue(listener.addedNotifications == 1);

        // Call again, it should add it internally but since the topic exists, there should be no new notification
        subscription.onNewAdvert(topicInfo2);
        Assert.assertTrue(listener.addedNotifications == 1);

        // Call again with a different topic
        subscription.onNewAdvert(topicInfo3);
        Assert.assertTrue(listener.addedNotifications == 2);

        // Now remove
        subscription.onAdvertTimedOut(topicInfo3);
        Assert.assertTrue(listener.removedNotifications == 1);

        subscription.onAdvertTimedOut(topicInfo1);
        Assert.assertTrue(listener.removedNotifications == 1);

        subscription.onAdvertTimedOut(topicInfo2);
        Assert.assertTrue(listener.removedNotifications == 2);

        // Now add again, there should be a new advert
        Assert.assertTrue(listener.addedNotifications == 2);
        subscription.onNewAdvert(topicInfo1);
        Assert.assertTrue(listener.addedNotifications == 3);

        // Clear
        subscription.clear();
        subscription.onNewAdvert(topicInfo1);
        Assert.assertTrue(listener.addedNotifications == 4);

        // Finally try with non valid TOPICS
        subscription.clear();
        subscription.onNewAdvert(topicInfo4);
        Assert.assertTrue(listener.addedNotifications == 4);

        subscription.onAdvertTimedOut(topicInfo4);
        Assert.assertTrue(listener.removedNotifications == 2);
    }

    public class ListenerImpl implements IAutodiscPubTopicPatternListener
    {
        int addedNotifications = 0;
        int removedNotifications = 0;

        @Override
        public void onNewPubTopicForPattern(AutoDiscTopicInfo topicInfo, String topicPattern)
        {
            addedNotifications++;
        }

        @Override
        public void onPubTopicForPatternRemoved(AutoDiscTopicInfo topicInfo, String topicPattern)
        {
            removedNotifications++;
        }
    }
}