package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.msg.IRcvMessage;
import com.bbva.kyof.vega.msg.IRcvRequest;
import com.bbva.kyof.vega.msg.RcvMessage;
import com.bbva.kyof.vega.msg.RcvRequest;
import com.bbva.kyof.vega.msg.lost.IMsgLostReport;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by cnebrera on 11/08/16.
 */
public class TopicSubscriberTest
{
    final TopicTemplateConfig config = new TopicTemplateConfig();
    private final TopicSubscriber topicSubscriber = new TopicSubscriber("topic1", config);

    @Test
    public void testBasicGetSet()
    {
        // Test basic getters
        Assert.assertEquals(topicSubscriber.getTopicName(), "topic1");
        Assert.assertEquals(topicSubscriber.getTopicConfig(), config);
    }

    @Test
    public void testAddRemoveSubscribers()
    {
        // Test basic getters
        Assert.assertEquals(topicSubscriber.getTopicName(), "topic1");
        Assert.assertEquals(topicSubscriber.getTopicConfig(), config);

        // Test add/remove aeron subscriber
        final AeronSubscriber aeronSubscriber = EasyMock.createNiceMock(AeronSubscriber.class);
        final AeronSubscriber aeronSubscriber2 = EasyMock.createNiceMock(AeronSubscriber.class);
        EasyMock.replay(aeronSubscriber, aeronSubscriber2);

        Assert.assertEquals(0, this.getAeronSubsCount());

        topicSubscriber.addAeronSubscriber(aeronSubscriber);
        Assert.assertEquals(1, this.getAeronSubsCount());

        topicSubscriber.addAeronSubscriber(aeronSubscriber);
        Assert.assertEquals(1, this.getAeronSubsCount());

        topicSubscriber.addAeronSubscriber(aeronSubscriber2);
        Assert.assertEquals(2, this.getAeronSubsCount());

        topicSubscriber.removeAeronSubscriber(aeronSubscriber);
        Assert.assertEquals(1, this.getAeronSubsCount());

        topicSubscriber.removeAeronSubscriber(aeronSubscriber2);
        Assert.assertEquals(0, this.getAeronSubsCount());
    }

    @Test
    public void testAddRemoveListeners()
    {
        assertTrue(topicSubscriber.hasNoListeners());

        final Listener listener1 = new Listener();
        assertTrue(topicSubscriber.setNormalListener(listener1));
        Assert.assertFalse(topicSubscriber.setNormalListener(listener1));

        Assert.assertFalse(topicSubscriber.hasNoListeners());
        assertTrue(topicSubscriber.removeNormalListener());
        Assert.assertFalse(topicSubscriber.removeNormalListener());

        assertTrue(topicSubscriber.hasNoListeners());

        assertTrue(topicSubscriber.addPatternListener("a.*", listener1));
        Assert.assertFalse(topicSubscriber.addPatternListener("a.*", listener1));
        Assert.assertFalse(topicSubscriber.hasNoListeners());

        assertTrue(topicSubscriber.removePatternListener("a.*"));
        assertTrue(topicSubscriber.hasNoListeners());
        Assert.assertFalse(topicSubscriber.removePatternListener("a.*"));
    }

    @Test
    public void testClose()
    {
        // Test add/remove aeron subscriber
        final AeronSubscriber aeronSubscriber = EasyMock.createNiceMock(AeronSubscriber.class);
        EasyMock.replay(aeronSubscriber);

        topicSubscriber.addAeronSubscriber(aeronSubscriber);
        topicSubscriber.setNormalListener(new Listener());
        topicSubscriber.addPatternListener("a.*", new Listener());

        topicSubscriber.close();

        assertTrue(topicSubscriber.hasNoListeners());
        assertEquals(0, this.getAeronSubsCount());
    }

    @Test
    public void testReceiveMessages()
    {
        final Listener normalListener = new Listener();
        final Listener patternListener1 = new Listener();
        final Listener patternListener2 = new Listener();

        topicSubscriber.setNormalListener(normalListener);

        final RcvMessage testMsg = new RcvMessage();
        testMsg.setSequenceNumber(1234);
        testMsg.setTopicPublisherId(UUID.randomUUID());

        final RcvRequest testRequest = new RcvRequest();
        testRequest.setSequenceNumber(3333);
        testRequest.setTopicPublisherId(UUID.randomUUID());

        topicSubscriber.onMessageReceived(testMsg);
        testMsg.setSequenceNumber(testMsg.getSequenceNumber() + 1);
        topicSubscriber.onMessageReceived(testMsg);
        testMsg.setSequenceNumber(testMsg.getSequenceNumber() + 1);
        topicSubscriber.onRequestReceived(testRequest);
        testRequest.setSequenceNumber(testRequest.getSequenceNumber() + 1);

        Assert.assertEquals(2, normalListener.msgsReceived);
        Assert.assertEquals(1, normalListener.requestsReceived);

        topicSubscriber.addPatternListener("a.*", patternListener1);
        topicSubscriber.addPatternListener("b.*", patternListener2);

        topicSubscriber.onMessageReceived(testMsg);
        topicSubscriber.onRequestReceived(testRequest);

        assertEquals(3, normalListener.msgsReceived);
        assertEquals(2, normalListener.requestsReceived);
        assertEquals(1, patternListener1.msgsReceived);
        assertEquals(1, patternListener1.requestsReceived);
        assertEquals(1, patternListener2.msgsReceived);
        assertEquals(1, patternListener2.requestsReceived);
        
        //Check sequence number
        Assert.assertEquals(normalListener.msgSequenceNumber, testMsg.getSequenceNumber());
        Assert.assertEquals(normalListener.requestSequenceNumber, testRequest.getSequenceNumber());
        Assert.assertEquals(patternListener1.msgSequenceNumber, testMsg.getSequenceNumber());
        Assert.assertEquals(patternListener1.requestSequenceNumber, testRequest.getSequenceNumber());
        Assert.assertEquals(patternListener2.msgSequenceNumber, testMsg.getSequenceNumber());
        Assert.assertEquals(patternListener2.requestSequenceNumber,testRequest.getSequenceNumber());

    }

    @Test
    public void testSequenceNumberMap()
    {
        final long msgSequenceNumber = new Random().nextLong();
        final UUID topicPublisherId = UUID.randomUUID();

        final RcvMessage testMsg = new RcvMessage();
        testMsg.setTopicPublisherId(topicPublisherId);
        testMsg.setSequenceNumber(msgSequenceNumber);

        topicSubscriber.onMessageReceived(testMsg);

        Assert.assertEquals(msgSequenceNumber + 1, topicSubscriber.getExpectedSeqNumByTopicPubId().get(topicPublisherId).get());


        // Remove topic publisher id from topic subscriber
        topicSubscriber.onTopicPublisherRemoved(topicPublisherId);

        Assert.assertNull(topicSubscriber.getExpectedSeqNumByTopicPubId().get(topicPublisherId));
    }

    private long getAeronSubsCount()
    {
        final AtomicLong count = new AtomicLong();
        topicSubscriber.runForEachRelatedAeronSubscriber((sub) -> count.getAndIncrement());
        return count.get();
    }

    private class Listener implements ITopicSubListener
    {
        int msgsReceived = 0;
        int requestsReceived = 0;

        long msgSequenceNumber = 0;
        long requestSequenceNumber = 0;

        long msgsLost = 0;

        @Override
        public void onMessageReceived(IRcvMessage receivedMessage)
        {
            this.msgsReceived++;
            this.msgSequenceNumber = ((RcvMessage)receivedMessage).getSequenceNumber();
        }

        @Override
        public void onRequestReceived(IRcvRequest receivedRequest)
        {
            this.requestsReceived++;
            this.requestSequenceNumber = ((RcvRequest)receivedRequest).getSequenceNumber();
        }

        @Override
        public void onMessageLost(final IMsgLostReport lostReport)
        {
            this.msgsLost = this.msgsLost + lostReport.getNumberLostMessages();
        }
    }

    @Test
    public void testReceiveMessagesWithDuplicates()
    {
        final Listener normalListener = new Listener();
        final Listener patternListener1 = new Listener();
        final Listener patternListener2 = new Listener();

        topicSubscriber.setNormalListener(normalListener);

        final RcvMessage testMsg = new RcvMessage();
        testMsg.setSequenceNumber(1234);
        testMsg.setTopicPublisherId(UUID.randomUUID());

        final RcvRequest testRequest = new RcvRequest();
        testRequest.setSequenceNumber(3333);
        testRequest.setTopicPublisherId(UUID.randomUUID());

        topicSubscriber.onMessageReceived(testMsg);
        topicSubscriber.onMessageReceived(testMsg);
        topicSubscriber.onRequestReceived(testRequest);
        topicSubscriber.onRequestReceived(testRequest);

        Assert.assertEquals(1, normalListener.msgsReceived);
        Assert.assertEquals(1, normalListener.requestsReceived);

        testMsg.setSequenceNumber(testMsg.getSequenceNumber() + 1);
        testRequest.setSequenceNumber(testRequest.getSequenceNumber() + 1);

        topicSubscriber.addPatternListener("a.*", patternListener1);
        topicSubscriber.addPatternListener("b.*", patternListener2);

        topicSubscriber.onMessageReceived(testMsg);
        topicSubscriber.onMessageReceived(testMsg);
        topicSubscriber.onRequestReceived(testRequest);
        topicSubscriber.onRequestReceived(testRequest);

        assertEquals(2, normalListener.msgsReceived);
        assertEquals(2, normalListener.requestsReceived);
        assertEquals(1, patternListener1.msgsReceived);
        assertEquals(1, patternListener1.requestsReceived);
        assertEquals(1, patternListener2.msgsReceived);
        assertEquals(1, patternListener2.requestsReceived);

        //Check sequence number
        Assert.assertEquals(normalListener.msgSequenceNumber, testMsg.getSequenceNumber());
        Assert.assertEquals(normalListener.requestSequenceNumber, testRequest.getSequenceNumber());
        Assert.assertEquals(patternListener1.msgSequenceNumber, testMsg.getSequenceNumber());
        Assert.assertEquals(patternListener1.requestSequenceNumber, testRequest.getSequenceNumber());
        Assert.assertEquals(patternListener2.msgSequenceNumber, testMsg.getSequenceNumber());
        Assert.assertEquals(patternListener2.requestSequenceNumber,testRequest.getSequenceNumber());
    }

    @Test
    public void testReceiveMessagesWithGap()
    {
        final Listener normalListener = new Listener();
        final Listener patternListener1 = new Listener();
        final Listener patternListener2 = new Listener();

        topicSubscriber.setNormalListener(normalListener);

        final RcvMessage testMsg = new RcvMessage();
        testMsg.setSequenceNumber(1234);
        testMsg.setTopicPublisherId(UUID.randomUUID());

        final RcvRequest testRequest = new RcvRequest();
        testRequest.setSequenceNumber(3333);
        testRequest.setTopicPublisherId(UUID.randomUUID());

        topicSubscriber.onMessageReceived(testMsg);
        //simulate gap of 10 msgs
        testMsg.setSequenceNumber(testMsg.getSequenceNumber() + 11);
        topicSubscriber.onMessageReceived(testMsg);
        //simulate gap of 10 msgs
        testMsg.setSequenceNumber(testMsg.getSequenceNumber() + 11);
        topicSubscriber.onRequestReceived(testRequest);
        //simulate gap of 10 rqs
        testRequest.setSequenceNumber(testRequest.getSequenceNumber() + 11);
        //simulate gap of 10 rqs
        topicSubscriber.onRequestReceived(testRequest);
        testRequest.setSequenceNumber(testRequest.getSequenceNumber() + 11);

        Assert.assertEquals(2, normalListener.msgsReceived);
        Assert.assertEquals(2, normalListener.requestsReceived);

        topicSubscriber.addPatternListener("a.*", patternListener1);
        topicSubscriber.addPatternListener("b.*", patternListener2);

        //4 Gaps (2 of msgs & 2 of rqs)
        topicSubscriber.onMessageReceived(testMsg);
        testMsg.setSequenceNumber(testMsg.getSequenceNumber() + 11);
        topicSubscriber.onMessageReceived(testMsg);
        topicSubscriber.onRequestReceived(testRequest);
        testRequest.setSequenceNumber(testRequest.getSequenceNumber() + 11);
        topicSubscriber.onRequestReceived(testRequest);

        assertEquals(4, normalListener.msgsReceived);
        assertEquals(4, normalListener.requestsReceived);
        assertEquals(2, patternListener1.msgsReceived);
        assertEquals(2, patternListener1.requestsReceived);
        assertEquals(2, patternListener2.msgsReceived);
        assertEquals(2, patternListener2.requestsReceived);

        //Check sequence number
        Assert.assertEquals(normalListener.msgSequenceNumber, testMsg.getSequenceNumber());
        Assert.assertEquals(normalListener.requestSequenceNumber, testRequest.getSequenceNumber());
        Assert.assertEquals(patternListener1.msgSequenceNumber, testMsg.getSequenceNumber());
        Assert.assertEquals(patternListener1.requestSequenceNumber, testRequest.getSequenceNumber());
        Assert.assertEquals(patternListener2.msgSequenceNumber, testMsg.getSequenceNumber());
        Assert.assertEquals(patternListener2.requestSequenceNumber,testRequest.getSequenceNumber());

        //Check loss repots (6 gaps of 10 messages / requests)
        Assert.assertEquals(normalListener.msgsLost, 60);

        //Check loss repots (4 gaps of 10 messages / requests)
        Assert.assertEquals(patternListener1.msgsLost, 40);
        Assert.assertEquals(patternListener2.msgsLost, 40);
    }
}