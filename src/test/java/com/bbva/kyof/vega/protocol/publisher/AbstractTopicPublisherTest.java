package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.msg.PublishResult;
import com.bbva.kyof.vega.msg.SentRequest;
import com.bbva.kyof.vega.protocol.common.AsyncRequestManager;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.heartbeat.HeartbeatParameters;
import com.bbva.kyof.vega.protocol.heartbeat.IClientConnectionListener;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by cnebrera on 11/08/16.
 */
public class AbstractTopicPublisherTest
{
    TopicTemplateConfig topicConfig;
    VegaContext vegaContext;
    AsyncRequestManager asyncRequestManager;

    @Before
    public void beforeTest()
    {
        topicConfig = TopicTemplateConfig.builder().name("name").transportType(TransportMediaType.UNICAST).build();
        vegaContext = new VegaContext(null, null);

        asyncRequestManager = new AsyncRequestManager(UUID.randomUUID());
        vegaContext.setAsyncRequestManager(asyncRequestManager);
    }

    @After
    public void afterTest() throws Exception
    {
        asyncRequestManager.close();
    }

    @Test
    public void testSendAndClose() throws Exception
    {
        final TopicPublisherImpl topicPubImpl = new TopicPublisherImpl("topic", topicConfig, vegaContext);

        // Test get topic name and topic config
        Assert.assertEquals(topicPubImpl.getTopicName(), "topic");
        Assert.assertTrue(topicPubImpl.getTopicConfig() == topicConfig);

        // Now test send message
        UnsafeBuffer sendBuffer = new UnsafeBuffer(ByteBuffer.allocate(128));
        Assert.assertEquals(topicPubImpl.sendMsg(sendBuffer, 0, 128), PublishResult.OK);
        Assert.assertTrue(topicPubImpl.getSendMessageBufferRef() == sendBuffer);

        // Test send request
        sendBuffer = new UnsafeBuffer(ByteBuffer.allocate(128));
        SentRequest sentRequest = topicPubImpl.sendRequest(sendBuffer, 0, 128, 100L, null);
        Assert.assertEquals(sentRequest.getSentResult(), PublishResult.OK);
        Assert.assertFalse(sentRequest.isClosed());

        // Wait a bit, the request should expire and close
        Thread.sleep(200);
        Assert.assertTrue(sentRequest.isClosed());

        // Test close and send again
        topicPubImpl.close();
        Assert.assertTrue(topicPubImpl.getCleanedPublishers());

        sendBuffer = new UnsafeBuffer(ByteBuffer.allocate(128));
        Assert.assertEquals(topicPubImpl.sendMsg(sendBuffer, 0, 128), PublishResult.UNEXPECTED_ERROR);
        Assert.assertFalse(topicPubImpl.getSendMessageBufferRef() == sendBuffer);

        // Test send request
        sendBuffer = new UnsafeBuffer(ByteBuffer.allocate(128));
        sentRequest = topicPubImpl.sendRequest(sendBuffer, 0, 128, 100L, null);
        Assert.assertEquals(sentRequest.getSentResult(), PublishResult.UNEXPECTED_ERROR);
        Assert.assertFalse(sentRequest.isClosed());

        // Wait a bit, the request should not close because has not been inserted in the async request manager
        Thread.sleep(200);
        Assert.assertFalse(sentRequest.isClosed());
    }

    @Test
    public void testActivateHeartbeats() throws Exception
    {
        final TopicPublisherImpl topicPublisherBase = new TopicPublisherImpl("topic", topicConfig, vegaContext);
        Assert.assertFalse(topicPublisherBase.isHeartbeatsActive());

        // Activate the heartbeats
        topicPublisherBase.activateHeartbeats(topicPublisherBase, HeartbeatParameters.builder().heartbeatRate(100).build());
        Assert.assertTrue(topicPublisherBase.isHeartbeatsActive());

        // Wait a bit
        Thread.sleep(1000);

        // Stop
        topicPublisherBase.deactivateHeartbeats();
        Assert.assertFalse(topicPublisherBase.isHeartbeatsActive());

        // Some request should have been sent
        Assert.assertTrue(topicPublisherBase.getLastReqTypeSent() == MsgType.HEARTBEAT_REQ);

        // Activate twice now and close
        topicPublisherBase.activateHeartbeats(topicPublisherBase, HeartbeatParameters.builder().heartbeatRate(100).build());
        topicPublisherBase.activateHeartbeats(topicPublisherBase, HeartbeatParameters.builder().heartbeatRate(100).build());

        // Wait a bit
        Thread.sleep(1000);

        topicPublisherBase.close();

        // After close they should have been deactivated
        Assert.assertFalse(topicPublisherBase.isHeartbeatsActive());

        // Try to activate / deactivate after close
        topicPublisherBase.activateHeartbeats(topicPublisherBase, HeartbeatParameters.builder().heartbeatRate(100).build());
        Assert.assertFalse(topicPublisherBase.isHeartbeatsActive());
        topicPublisherBase.deactivateHeartbeats();
    }

    private class TopicPublisherImpl extends AbstractTopicPublisher implements IClientConnectionListener
    {
        AtomicReference<DirectBuffer> sendMessageBufferRef = new AtomicReference<>();
        AtomicReference<DirectBuffer> sentRequestBufferRef = new AtomicReference<>();
        AtomicBoolean cleanedPublishers = new AtomicBoolean(false);
        AtomicReference<Byte> lastReqTypeSent = new AtomicReference<>((byte)222);

        /**
         * Constructor of the class
         *
         * @param topicName   Topic name that is going to sendMsg
         * @param topicConfig Topic configuration
         * @param vegaContext library instance configuration
         */
        TopicPublisherImpl(String topicName, TopicTemplateConfig topicConfig, VegaContext vegaContext)
        {
            super(topicName, topicConfig, vegaContext);
        }

        @Override
        boolean hasSecurity()
        {
            return false;
        }

        @Override
        public TopicSecurityTemplateConfig getTopicSecurityConfig()
        {
            return null;
        }

        @Override
        protected PublishResult sendToAeron(DirectBuffer message, int offset, int length)
        {
            sendMessageBufferRef.set(message);
            return PublishResult.OK;
        }

        @Override
        protected PublishResult sendRequestToAeron(byte msgType, UUID requestId, DirectBuffer message, int offset, int length)
        {
            lastReqTypeSent.set(msgType);
            sentRequestBufferRef.set(message);
            return PublishResult.OK;
        }

        @Override
        protected void cleanAeronPublishers()
        {
            cleanedPublishers.set(true);
        }

        public DirectBuffer getSendMessageBufferRef()
        {
            return sendMessageBufferRef.get();
        }

        public DirectBuffer getSentRequestBufferRef()
        {
            return sentRequestBufferRef.get();
        }

        public boolean getCleanedPublishers()
        {
            return cleanedPublishers.get();
        }

        public byte getLastReqTypeSent()
        {
            return lastReqTypeSent.get();
        }

        @Override
        public void onClientConnected(String topicName, UUID clientInstanceId)
        {

        }

        @Override
        public void onClientDisconnected(String topicName, UUID clientInstanceId)
        {

        }
    }
}