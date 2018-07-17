package com.bbva.kyof.vega.protocol;

import com.bbva.kyof.vega.config.general.ConfigReaderTest;
import com.bbva.kyof.vega.msg.*;
import com.bbva.kyof.vega.msg.lost.IMsgLostReport;
import com.bbva.kyof.vega.protocol.common.VegaInstanceParams;
import com.bbva.kyof.vega.protocol.publisher.ITopicPublisher;
import com.bbva.kyof.vega.protocol.subscriber.ITopicSubListener;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;

import java.lang.reflect.Field;

import lombok.Getter;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Test for loss messages
 * Created by cnebrera on 31/06/2018.
 */
public class VegaLostMessageTest
{
    private static final String CONFIG_FILE = Objects.requireNonNull(ConfigReaderTest.class.getClassLoader().getResource("config/lossTest.xml")).getPath();

    private UnsafeBuffer sendBuffer = new UnsafeBuffer(ByteBuffer.allocate(128));

    private static final int NUM_MESSAGES = 1000;

    @BeforeClass
    public static void beforeClass() throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException
    {
        // Add system properties to simulate lost
        System.setProperty("aeron.SendChannelEndpoint.supplier", "io.aeron.driver.ext.DebugSendChannelEndpointSupplier");
        System.setProperty("aeron.ReceiveChannelEndpoint.supplier", "io.aeron.driver.ext.DebugReceiveChannelEndpointSupplier");
        System.setProperty("aeron.debug.receive.data.loss.rate", "0.80");
        System.setProperty("aeron.debug.receive.data.loss.seed", "-1");
        
        // Remove reliability on the channels
        Field reliable = AeronChannelHelper.class.getDeclaredField("reliable");
        reliable.setAccessible(true);
        reliable.setBoolean(null, false);
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        // Remove system properties
        final Properties properties = System.getProperties();
        properties.remove("aeron.SendChannelEndpoint.supplier");
        properties.remove("aeron.ReceiveChannelEndpoint.supplier");
        properties.remove("aeron.debug.receive.data.loss.rate");
        properties.remove("aeron.debug.receive.data.loss.seed");

        // Set the channels to reliable again
        Field reliable = AeronChannelHelper.class.getDeclaredField("reliable");
        reliable.setAccessible(true);
        reliable.setBoolean(null, true);
    }

    @Test
    public void testMessageLost() throws Exception
    {
        final VegaInstanceParams params1 = VegaInstanceParams.builder().
                instanceName("Instance1").
                configurationFile(CONFIG_FILE).build();

        // Create 2 application instances, use auto-closeable just in case
        try(final IVegaInstance vegaInstance = VegaInstance.createNewInstance(params1))
        {
            // Create a receiver listener for normal subscriptions and a separate one for wildcard
            final ReceiverListener receiverListener = new ReceiverListener();
            final ReceiverListener wildCardReceiverListener = new ReceiverListener();

            // Perform normal subscriptions and a wildcard as well
            vegaInstance.subscribeToTopic("utopic1", receiverListener);
            vegaInstance.subscribeToTopic("mtopic1", receiverListener);
            vegaInstance.subscribeToPattern(".*", wildCardReceiverListener);


            // Create 2 publishers
            final ITopicPublisher utopicPub = vegaInstance.createPublisher("utopic1");
            final ITopicPublisher mtopicPub = vegaInstance.createPublisher("mtopic1");

            // Add the topic publishers to the listeners
            receiverListener.addTopicPublisher(utopicPub.getUniqueId());
            receiverListener.addTopicPublisher(mtopicPub.getUniqueId());

            wildCardReceiverListener.addTopicPublisher(utopicPub.getUniqueId());
            wildCardReceiverListener.addTopicPublisher(mtopicPub.getUniqueId());

            // Wait to give the auto-discovery time to work
            Thread.sleep(5000);

            // Send messages
            for(int i = 0; i < NUM_MESSAGES; i++)
            {
                this.sendMessage(utopicPub);
                this.sendRequest(utopicPub, receiverListener);
                this.sendMessage(mtopicPub);
                this.sendRequest(mtopicPub, receiverListener);
                Thread.sleep(5);
            }

            // Make sure there has been message loss
            //Assert.assertTrue(receiverListener.lostMessagesInAllTopics());
            //Assert.assertTrue(wildCardReceiverListener.lostMessagesInAllTopics());
        }
    }

    private void sendMessage(final ITopicPublisher publisher)
    {
        // Write the message in the buffer
        sendBuffer.putInt(0, 33);

        // Send and wait
        publisher.sendMsg(sendBuffer, 0, 4);
    }

    private void sendRequest(final ITopicPublisher publisher, final ReceiverListener listener)
    {
        // Write the message in the buffer
        sendBuffer.putInt(0, 33);

        // Send and wait
        publisher.sendRequest(sendBuffer, 0, 4, 10, listener);
    }

    class ReceiverListener implements ITopicSubListener, IResponseListener
    {
        private Map<UUID, TopicReceiver> receiversByTopicId = new HashMap<>();

        void addTopicPublisher(final UUID topicPubId)
        {
            receiversByTopicId.put(topicPubId, new TopicReceiver());
        }

        @Override
        public void onMessageReceived(final IRcvMessage receivedMessage)
        {
            final RcvMessage msg = (RcvMessage)receivedMessage;
            this.receiversByTopicId.get(msg.getTopicPublisherId()).processReceivedMessage(msg.getSequenceNumber());
        }

        @Override
        public void onRequestReceived(final IRcvRequest receivedRequest)
        {
            final RcvRequest msg = (RcvRequest)receivedRequest;
            this.receiversByTopicId.get(msg.getTopicPublisherId()).processReceivedMessage(msg.getSequenceNumber());
        }

        @Override
        public void onMessageLost(final IMsgLostReport lostReport)
        {
            // Update loss info number
            this.receiversByTopicId.get(lostReport.getTopicPublisherId()).processLossMessage(lostReport.getNumberLostMessages());
        }

        @Override
        public void onRequestTimeout(ISentRequest originalSentRequest)
        {
        }

        @Override
        public void onResponseReceived(ISentRequest originalSentRequest, IRcvResponse response)
        {
        }

        /** True if all the topic contains loss messages */
        boolean lostMessagesInAllTopics()
        {
            for(final TopicReceiver topicReceiver : this.receiversByTopicId.values())
            {
                if (topicReceiver.messagesLost == 0)
                {
                    return false;
                }
            }

            return true;
        }
    }

    class TopicReceiver
    {
        @Getter private int messagesLost = 0;
        @Getter private int messagesRcv = 0;
        @Getter private long nextExpectedSeq = 1;
        private boolean firstMessage = true;

        void processReceivedMessage(long rcvSequence)
        {
            this.messagesRcv++;

            if (this.firstMessage)
            {
                this.nextExpectedSeq = rcvSequence;
                this.firstMessage = false;
            }

            Assert.assertEquals(rcvSequence, this.nextExpectedSeq);

            this.nextExpectedSeq = rcvSequence + 1;
        }

        void processLossMessage(final long numberOfLossMessages)
        {
            // Update loss info number
            this.messagesLost += numberOfLossMessages;

            this.nextExpectedSeq += numberOfLossMessages;
        }
    }
}