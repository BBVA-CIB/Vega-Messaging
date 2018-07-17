package com.bbva.kyof.vega.autodiscovery.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Created by cnebrera on 04/08/16.
 */
public class PubPatternSubscriptionsManagerTest
{
    @Test
    public void testSubscribeUnsubscribe()
    {
        final ListenerImpl listener = new ListenerImpl();
        final ListenerImpl listener2 = new ListenerImpl();

        final PubPatternSubscriptionsManager manager = new PubPatternSubscriptionsManager();

        // Subscribe now
        Assert.assertNotNull(manager.subscribeToPattern("a.*", listener));
        Assert.assertNull(manager.subscribeToPattern("a.*", listener));
        Assert.assertNull(manager.subscribeToPattern("a.*", listener2));
        Assert.assertNotNull(manager.subscribeToPattern("b.*", listener2));

        // Unsubscribe
        Assert.assertFalse(manager.unsubscribeFromPattern(".*"));
        Assert.assertTrue(manager.unsubscribeFromPattern("a.*"));
        Assert.assertFalse(manager.unsubscribeFromPattern("a.*"));
        Assert.assertTrue(manager.unsubscribeFromPattern("b.*"));

        // Subscribe and clear
        Assert.assertNotNull(manager.subscribeToPattern("a.*", listener));
        manager.clear();
        Assert.assertNotNull(manager.subscribeToPattern("a.*", listener));
    }

    @Test
    public void testAdverts()
    {
        final UUID instanceId = UUID.randomUUID();
        final ListenerImpl listener = new ListenerImpl();
        final ListenerImpl listener2 = new ListenerImpl();
        final AutoDiscTopicInfo subTopic = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.SUB_IPC, UUID.randomUUID(), "a");
        final AutoDiscTopicInfo topicInfo1 = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "ab");
        final AutoDiscTopicInfo topicInfo11 = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "ab");
        final AutoDiscTopicInfo topicInfo2 = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "abb");
        final AutoDiscTopicInfo topicInfo3 = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "acb");
        final AutoDiscTopicInfo nonMatchTopic = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "bbb");

        final PubPatternSubscriptionsManager manager = new PubPatternSubscriptionsManager();

        // Subscribe now
        manager.subscribeToPattern("ab.*", listener);
        manager.subscribeToPattern("ac.*", listener2);


        // Call on sub topic, should ignore it
        manager.onNewTopicInfo(subTopic);
        Assert.assertTrue(listener.addedNotifications == 0);
        Assert.assertTrue(listener2.addedNotifications == 0);

        manager.onTopicInfoTimedOut(subTopic);
        Assert.assertTrue(listener.removedNotifications == 0);
        Assert.assertTrue(listener2.removedNotifications == 0);

        // Call on non match topic, should ignore it
        manager.onNewTopicInfo(nonMatchTopic);
        Assert.assertTrue(listener.addedNotifications == 0);
        Assert.assertTrue(listener2.addedNotifications == 0);

        // Call new adverts, since is the first time the topic is added, there should be a new subscription
        manager.onNewTopicInfo(topicInfo1);
        Assert.assertTrue(listener.addedNotifications == 1);
        Assert.assertTrue(listener2.addedNotifications == 0);

        // Call new adverts, since is the first time the topic is added, there should be a new subscription
        manager.onNewTopicInfo(topicInfo11);
        Assert.assertTrue(listener.addedNotifications == 1);
        Assert.assertTrue(listener2.addedNotifications == 0);

        // Call new adverts, since is the first time the topic is added, there should be a new subscription
        manager.onNewTopicInfo(topicInfo2);
        Assert.assertTrue(listener.addedNotifications == 2);
        Assert.assertTrue(listener2.addedNotifications == 0);

        // Call new adverts, since is the first time the topic is added, there should be a new subscription
        manager.onNewTopicInfo(topicInfo3);
        Assert.assertTrue(listener.addedNotifications == 2);
        Assert.assertTrue(listener2.addedNotifications == 1);

        // Now try to remove
        manager.onTopicInfoTimedOut(topicInfo1);
        Assert.assertTrue(listener.removedNotifications == 0);
        Assert.assertTrue(listener2.removedNotifications == 0);
        manager.onTopicInfoTimedOut(topicInfo1);
        Assert.assertTrue(listener.removedNotifications == 0);
        Assert.assertTrue(listener2.removedNotifications == 0);
        manager.onTopicInfoTimedOut(topicInfo2);
        Assert.assertTrue(listener.removedNotifications == 1);
        Assert.assertTrue(listener2.removedNotifications == 0);
        manager.onTopicInfoTimedOut(topicInfo3);
        Assert.assertTrue(listener.removedNotifications == 1);
        Assert.assertTrue(listener2.removedNotifications == 1);
        manager.onTopicInfoTimedOut(topicInfo11);
        Assert.assertTrue(listener.removedNotifications == 2);
        Assert.assertTrue(listener2.removedNotifications == 1);
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