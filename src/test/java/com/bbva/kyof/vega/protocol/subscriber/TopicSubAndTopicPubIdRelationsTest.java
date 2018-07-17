package com.bbva.kyof.vega.protocol.subscriber;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Created by cnebrera on 11/08/16.
 */
public class TopicSubAndTopicPubIdRelationsTest
{
    @Test
    public void test()
    {
        final TopicSubAndTopicPubIdRelations relations = new TopicSubAndTopicPubIdRelations();

        final UUID topicSubId1 = UUID.randomUUID();
        final UUID topicSubId2 = UUID.randomUUID();

        final TopicSubscriber topicSubscriber1 = EasyMock.createNiceMock(TopicSubscriber.class);
        final TopicSubscriber topicSubscriber2 = EasyMock.createNiceMock(TopicSubscriber.class);

        EasyMock.expect(topicSubscriber1.getUniqueId()).andReturn(topicSubId1).anyTimes();
        EasyMock.expect(topicSubscriber2.getUniqueId()).andReturn(topicSubId2).anyTimes();

        EasyMock.replay(topicSubscriber1, topicSubscriber2);

        final UUID topicPubId1 = UUID.randomUUID();
        final UUID topicPubId2 = UUID.randomUUID();
        final UUID topicPubId3 = UUID.randomUUID();

        // Add some relations
        relations.addTopicPubRelation(topicPubId1, topicSubscriber1);
        relations.addTopicPubRelation(topicPubId2, topicSubscriber2);
        relations.addTopicPubRelation(topicPubId3, topicSubscriber2);

        Assert.assertEquals(relations.getTopicSubscriberForTopicPublisherId(topicPubId1), topicSubscriber1);
        Assert.assertEquals(relations.getTopicSubscriberForTopicPublisherId(topicPubId2), topicSubscriber2);
        Assert.assertEquals(relations.getTopicSubscriberForTopicPublisherId(topicPubId3), topicSubscriber2);

        // Remove single relation
        relations.removeTopicPubRelation(topicPubId1, topicSubscriber1);
        Assert.assertNull(relations.getTopicSubscriberForTopicPublisherId(topicPubId1));

        // Remove topic subscriber relation, should remove all related topic publishers
        relations.removeTopicSubscriber(topicSubscriber2);
        Assert.assertNull(relations.getTopicSubscriberForTopicPublisherId(topicPubId2));
        Assert.assertNull(relations.getTopicSubscriberForTopicPublisherId(topicPubId3));

        // Add relation and clear
        relations.addTopicPubRelation(topicPubId1, topicSubscriber1);
        relations.clear();
        Assert.assertNull(relations.getTopicSubscriberForTopicPublisherId(topicPubId1));
    }
}