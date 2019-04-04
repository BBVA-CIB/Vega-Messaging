package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.msg.IRcvMessage;
import com.bbva.kyof.vega.msg.IRcvRequest;
import com.bbva.kyof.vega.msg.RcvMessage;
import com.bbva.kyof.vega.msg.RcvRequest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by cnebrera on 11/08/16.
 */
public class TopicSubscriberTest
{
    final TopicTemplateConfig config = new TopicTemplateConfig();
    final TopicSubscriber topicSubscriber = new TopicSubscriber("topic1", config);

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

        Assert.assertTrue(this.getAeronSubsCount() == 0);

        topicSubscriber.addAeronSubscriber(aeronSubscriber);
        Assert.assertTrue(this.getAeronSubsCount() == 1);

        topicSubscriber.addAeronSubscriber(aeronSubscriber);
        Assert.assertTrue(this.getAeronSubsCount() == 1);

        topicSubscriber.addAeronSubscriber(aeronSubscriber2);
        Assert.assertTrue(this.getAeronSubsCount() == 2);

        topicSubscriber.removeAeronSubscriber(aeronSubscriber);
        Assert.assertTrue(this.getAeronSubsCount() == 1);

        topicSubscriber.removeAeronSubscriber(aeronSubscriber2);
        Assert.assertTrue(this.getAeronSubsCount() == 0);
    }

    @Test
    public void testAddRemoveListeners()
    {
        Assert.assertTrue(topicSubscriber.hasNoListeners());

        final Listener listener1 = new Listener();
        Assert.assertTrue(topicSubscriber.setNormalListener(listener1));
        Assert.assertFalse(topicSubscriber.setNormalListener(listener1));

        Assert.assertFalse(topicSubscriber.hasNoListeners());
        Assert.assertTrue(topicSubscriber.removeNormalListener());
        Assert.assertFalse(topicSubscriber.removeNormalListener());

        Assert.assertTrue(topicSubscriber.hasNoListeners());

        Assert.assertTrue(topicSubscriber.addPatternListener("a.*", listener1));
        Assert.assertFalse(topicSubscriber.addPatternListener("a.*", listener1));
        Assert.assertFalse(topicSubscriber.hasNoListeners());

        Assert.assertTrue(topicSubscriber.removePatternListener("a.*"));
        Assert.assertTrue(topicSubscriber.hasNoListeners());
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

        Assert.assertTrue(topicSubscriber.hasNoListeners());
        Assert.assertTrue(this.getAeronSubsCount() == 0);
    }

    @Test
    public void testReceiveMessages()
    {
        final Listener normalListener = new Listener();
        final Listener patternListener1 = new Listener();
        final Listener patternListener2 = new Listener();

        topicSubscriber.setNormalListener(normalListener);

        topicSubscriber.onMessageReceived(new RcvMessage());
        topicSubscriber.onMessageReceived(new RcvMessage());
        topicSubscriber.onRequestReceived(new RcvRequest());

        Assert.assertTrue(normalListener.msgsReceived == 2);
        Assert.assertTrue(normalListener.requestsReceived == 1);

        topicSubscriber.addPatternListener("a.*", patternListener1);
        topicSubscriber.addPatternListener("b.*", patternListener2);

        topicSubscriber.onMessageReceived(new RcvMessage());
        topicSubscriber.onRequestReceived(new RcvRequest());

        Assert.assertTrue(normalListener.msgsReceived == 3);
        Assert.assertTrue(normalListener.requestsReceived == 2);
        Assert.assertTrue(patternListener1.msgsReceived == 1);
        Assert.assertTrue(patternListener1.requestsReceived == 1);
        Assert.assertTrue(patternListener2.msgsReceived == 1);
        Assert.assertTrue(patternListener2.requestsReceived == 1);
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

        @Override
        public void onMessageReceived(IRcvMessage receivedMessage)
        {
            this.msgsReceived++;
        }

        @Override
        public void onRequestReceived(IRcvRequest receivedRequest)
        {
            this.requestsReceived++;
        }
    }
}