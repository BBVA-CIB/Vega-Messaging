package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.config.general.GlobalConfiguration;
import com.bbva.kyof.vega.config.general.IdleStrategyType;
import com.bbva.kyof.vega.config.general.RcvPollerConfig;
import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.msg.*;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.publisher.AeronPublisher;
import com.bbva.kyof.vega.protocol.publisher.AeronPublisherParams;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import lombok.Getter;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by cnebrera on 11/08/16.
 */
public class SubcribersPollerTest
{
    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static SubnetAddress SUBNET_ADDRESS;
    private static VegaContext VEGA_CONTEXT;
    private static AeronSubscriber IPC_SUBSCRIBER;
    private static AeronSubscriber MCAST_SUBSCRIBER;
    private static AeronSubscriber UCAST_SUBSCRIBER;
    private static AeronPublisher IPC_PUBLISHER;
    private static AeronPublisher MCAST_PUBLISHER;
    private static AeronPublisher UCAST_PUBLISHER;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx1 = new Aeron.Context();
        ctx1.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON = Aeron.connect(ctx1);

        SUBNET_ADDRESS = InetUtil.getDefaultSubnet();
        VEGA_CONTEXT = new VegaContext(AERON, new GlobalConfiguration());

        Thread.sleep(1000);

        final int mcastIp = InetUtil.convertIpAddressToInt("224.1.1.1");
        final int ucastIp = InetUtil.convertIpAddressToInt(SUBNET_ADDRESS.getIpAddres().getHostAddress());

        final AeronSubscriberParams ipcSubscriberParams = new AeronSubscriberParams(TransportMediaType.IPC, mcastIp, 0, 2, null);
        final AeronSubscriberParams mcastSubscriberParams = new AeronSubscriberParams(TransportMediaType.MULTICAST, mcastIp, 28889, 2, SUBNET_ADDRESS);
        final AeronSubscriberParams unicastSubscriberParams = new AeronSubscriberParams(TransportMediaType.UNICAST, ucastIp, 29333, 2, SUBNET_ADDRESS);

        IPC_SUBSCRIBER = new AeronSubscriber(VEGA_CONTEXT, ipcSubscriberParams);
        MCAST_SUBSCRIBER = new AeronSubscriber(VEGA_CONTEXT, mcastSubscriberParams);
        UCAST_SUBSCRIBER = new AeronSubscriber(VEGA_CONTEXT, unicastSubscriberParams);

        final AeronPublisherParams ipcPubParams = new AeronPublisherParams(TransportMediaType.IPC, mcastIp, 0, 2, null);
        final AeronPublisherParams mcastPubParams = new AeronPublisherParams(TransportMediaType.MULTICAST, mcastIp, 28889, 2, SUBNET_ADDRESS);
        final AeronPublisherParams unicastPubParams = new AeronPublisherParams(TransportMediaType.UNICAST, ucastIp, 29333, 2, SUBNET_ADDRESS);

        IPC_PUBLISHER = new AeronPublisher(VEGA_CONTEXT, ipcPubParams);
        MCAST_PUBLISHER = new AeronPublisher(VEGA_CONTEXT, mcastPubParams);
        UCAST_PUBLISHER = new AeronPublisher(VEGA_CONTEXT, unicastPubParams);

        Thread.sleep(1000);
    }

    @AfterClass
    public static void afterClass()
    {
        IPC_SUBSCRIBER.close();
        MCAST_SUBSCRIBER.close();
        UCAST_SUBSCRIBER.close();
        IPC_PUBLISHER.close();
        MCAST_PUBLISHER.close();
        UCAST_PUBLISHER.close();
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Test
    public void createPollerWithNoSubscribers() throws Exception
    {
        // Create the config
        RcvPollerConfig config = RcvPollerConfig.builder().name("PollerName").idleStrategyType(IdleStrategyType.BACK_OFF).build();
        config.completeAndValidateConfig();

        // Create and start the poller
        final Listener listener = new Listener();
        final SubcribersPoller poller = new SubcribersPoller(listener, config);
        poller.start("Poller!!!");

        Thread.sleep(1000);

        poller.close();
    }

    @Test
    public void createPollerAndPoll() throws Exception
    {
        // Create the config
        RcvPollerConfig config = RcvPollerConfig.builder().name("PollerName").idleStrategyType(IdleStrategyType.BACK_OFF).build();
        config.completeAndValidateConfig();

        // Create and start the poller
        final Listener listener = new Listener();
        final SubcribersPoller poller = new SubcribersPoller(listener, config);
        poller.start();

        Assert.assertEquals(config, poller.getConfig());

        // Add the subscriptions
        poller.addSubscription(IPC_SUBSCRIBER);
        poller.addSubscription(UCAST_SUBSCRIBER);
        poller.addSubscription(MCAST_SUBSCRIBER);

        Thread.sleep(100);

        final UnsafeBuffer sendBuffer = new UnsafeBuffer(ByteBuffer.allocate(128));

        // Send some messages, requests and responses to ipc publisher
        final UUID topicId1 = UUID.randomUUID();
        final UUID requestId1 = UUID.randomUUID();
        final UUID heartbeatRequestId1 = UUID.randomUUID();
        final UUID heartbeatRequestId2 = UUID.randomUUID();
        final UUID heartbeatRequestId3 = UUID.randomUUID();

        final UUID responseId1 = UUID.randomUUID();

        // Send an unexpected type message, should not arrive
        IPC_PUBLISHER.sendMessage((byte)66, topicId1, sendBuffer, 0, 0, 4);

        // Send IPD messages
        sendBuffer.putInt(0, 11);
        IPC_PUBLISHER.sendMessage(MsgType.DATA, topicId1, sendBuffer, 1, 0, 4);
        sendBuffer.putInt(0, 12);
        IPC_PUBLISHER.sendRequest(MsgType.DATA_REQ, topicId1, requestId1, sendBuffer, 2, 0, 4);
        sendBuffer.putInt(0, 13);
        IPC_PUBLISHER.sendResponse(responseId1, sendBuffer, 0, 4);
        sendBuffer.putInt(0, 14);
        IPC_PUBLISHER.sendMessage(MsgType.ENCRYPTED_DATA, topicId1, sendBuffer, 3, 0, 4);
        sendBuffer.putInt(0, 15);
        IPC_PUBLISHER.sendRequest(MsgType.HEARTBEAT_REQ, topicId1, heartbeatRequestId1, sendBuffer, 4, 0, 4);


        // Send some messages, requests and responses to ucast publisher
        final UUID topicId2 = UUID.randomUUID();
        final UUID requestId2 = UUID.randomUUID();
        final UUID responseId2 = UUID.randomUUID();
        
        sendBuffer.putInt(0, 21);
        UCAST_PUBLISHER.sendMessage(MsgType.DATA, topicId2, sendBuffer, 5, 0, 4);
        sendBuffer.putInt(0, 22);
        UCAST_PUBLISHER.sendRequest(MsgType.DATA_REQ, topicId2, requestId2, sendBuffer, 6, 0, 4);
        sendBuffer.putInt(0, 23);
        UCAST_PUBLISHER.sendResponse(responseId2, sendBuffer, 0, 4);
        sendBuffer.putInt(0, 24);
        UCAST_PUBLISHER.sendMessage(MsgType.ENCRYPTED_DATA, topicId1, sendBuffer, 7, 0, 4);
        sendBuffer.putInt(0, 25);
        UCAST_PUBLISHER.sendRequest(MsgType.HEARTBEAT_REQ, topicId1, heartbeatRequestId2, sendBuffer, 8, 0, 4);

        // Send some messages, requests and responses to mcast publisher
        final UUID topicId3 = UUID.randomUUID();
        final UUID requestId3 = UUID.randomUUID();
        final UUID responseId3 = UUID.randomUUID();
        
        sendBuffer.putInt(0, 31);
        MCAST_PUBLISHER.sendMessage(MsgType.DATA, topicId3, sendBuffer, 9, 0, 4);
        sendBuffer.putInt(0, 32);
        MCAST_PUBLISHER.sendRequest(MsgType.DATA_REQ, topicId3, requestId3, sendBuffer, 10, 0, 4);
        sendBuffer.putInt(0, 33);
        MCAST_PUBLISHER.sendResponse(responseId3, sendBuffer, 0, 4);
        sendBuffer.putInt(0, 34);
        MCAST_PUBLISHER.sendMessage(MsgType.ENCRYPTED_DATA, topicId1, sendBuffer, 11, 0, 4);
        sendBuffer.putInt(0, 35);
        MCAST_PUBLISHER.sendRequest(MsgType.HEARTBEAT_REQ, topicId1, heartbeatRequestId3, sendBuffer, 12, 0, 4);

        // Wait for messages to arrive
        Thread.sleep(1000);

        // Check the messages
        Assert.assertTrue(listener.rcvMessagesContents.contains(11));
        Assert.assertTrue(listener.rcvMessagesContents.contains(21));
        Assert.assertTrue(listener.rcvMessagesContents.contains(31));

        Assert.assertTrue(listener.rcvMessagesSecuences.contains(1L));
        Assert.assertTrue(listener.rcvMessagesSecuences.contains(5L));
        Assert.assertTrue(listener.rcvMessagesSecuences.contains(9L));

        // Check the encrypted messages
        Assert.assertEquals(3, listener.rcvEncryptedMessagesCount.get());

        // Check the requests
        Assert.assertTrue(listener.rcvRequestsByContentValue.contains(12));
        Assert.assertTrue(listener.rcvRequestsByContentValue.contains(22));
        Assert.assertTrue(listener.rcvRequestsByContentValue.contains(32));

        Assert.assertTrue(listener.rcvRequestsBySeqNumber.contains(2L));
        Assert.assertTrue(listener.rcvRequestsBySeqNumber.contains(6L));
        Assert.assertTrue(listener.rcvRequestsBySeqNumber.contains(10L));

        // Check the heartbeat requests
        Assert.assertTrue(listener.rcvHeartbeatRequestIds.contains(heartbeatRequestId1));
        Assert.assertTrue(listener.rcvHeartbeatRequestIds.contains(heartbeatRequestId2));
        Assert.assertTrue(listener.rcvHeartbeatRequestIds.contains(heartbeatRequestId3));

        // Check the responses
        Assert.assertTrue(listener.rcvResponses.contains(13));
        Assert.assertTrue(listener.rcvResponses.contains(23));
        Assert.assertTrue(listener.rcvResponses.contains(33));

        // Remove the subscriptions
        poller.removeSubscription(UCAST_SUBSCRIBER);
        poller.removeSubscription(IPC_SUBSCRIBER);
        poller.removeSubscription(MCAST_SUBSCRIBER);

        Thread.sleep(10);

        // Try again, there should be no messages
        listener.rcvMessagesContents.clear();

        sendBuffer.putInt(0, 11);
        IPC_PUBLISHER.sendMessage(MsgType.DATA, topicId1, sendBuffer, 13, 0, 4);
        UCAST_PUBLISHER.sendMessage(MsgType.DATA, topicId1, sendBuffer, 14, 0, 4);
        MCAST_PUBLISHER.sendMessage(MsgType.DATA, topicId1, sendBuffer, 15, 0, 4);

        Thread.sleep(100);

        Assert.assertFalse(listener.rcvMessagesContents.contains(11));
        Assert.assertFalse(listener.rcvMessagesSecuences.contains(13L));
        Assert.assertFalse(listener.rcvMessagesSecuences.contains(14L));
        Assert.assertFalse(listener.rcvMessagesSecuences.contains(15L));

        poller.close();
    }

    @Test
    public void pollMessagesOfMultipleSizesAndAssemble() throws Exception
    {
        // 1 Mega byte message size
        final int MAX_MSG_SIZE = 2 * 1024 * 1024;

        // RANDOM generator
        final Random rnd = new Random();

        // Create some random messages of different sizes
        final List<byte[]> randomMessages = new LinkedList<>();
        for (int size = 1; size < MAX_MSG_SIZE; size = size * 2)
        {
            final byte[] msg = new byte[size];
            rnd.nextBytes(msg);
            randomMessages.add(msg);
        }

        // Create the config
        RcvPollerConfig config = RcvPollerConfig.builder().name("PollerName").idleStrategyType(IdleStrategyType.BACK_OFF).build();
        config.completeAndValidateConfig();

        // Create and start the poller
        final SimpleListener listener = new SimpleListener();
        final SubcribersPoller poller = new SubcribersPoller(listener, config);
        poller.start();

        // Add the subscription
        poller.addSubscription(UCAST_SUBSCRIBER);
        Thread.sleep(100);

        // Send all the messages
        randomMessages.forEach((msg) ->
        {
            final UnsafeBuffer sendBuffer = new UnsafeBuffer(msg);
            UCAST_PUBLISHER.sendMessage(MsgType.DATA, UUID.randomUUID(), sendBuffer, new Random().nextLong(), 0, msg.length);
        });

        // Wait for message to arrive
        Thread.sleep(3000);

        // Check the messages
        Assert.assertFalse(listener.rcvMessages.isEmpty());
        Assert.assertEquals(listener.rcvMessages.size(), randomMessages.size());

        // Check the contents
        for (int i = 0; i < randomMessages.size(); i++)
        {
            final IRcvMessage receivedMsg = listener.getRcvMessages().get(i);

            // Check length
            Assert.assertEquals(receivedMsg.getContentLength(), randomMessages.get(i).length);

            // Check content
            final byte[] content = new byte[receivedMsg.getContentLength()];
            receivedMsg.getContents().getBytes(receivedMsg.getContentOffset(), content);

            Assert.assertArrayEquals(content, randomMessages.get(i));
        }

        // Remove the subscription and close
        poller.removeSubscription(UCAST_SUBSCRIBER);
        poller.close();
    }

    private class Listener implements ISubscribersPollerListener
    {
        @Getter final Set<Integer> rcvMessagesContents = new HashSet<>();
        @Getter final Set<Long> rcvMessagesSecuences = new HashSet<>();
        @Getter final AtomicInteger rcvEncryptedMessagesCount = new AtomicInteger(0);
        @Getter final Set<Integer> rcvRequestsByContentValue = new HashSet<>();
        @Getter final Set<Long> rcvRequestsBySeqNumber = new HashSet<>();
        @Getter final Set<UUID>  rcvHeartbeatRequestIds = new HashSet<>();
        @Getter final Set<UUID>  rcvRequestIds = new HashSet<>();
        @Getter final Set<Integer>  rcvResponses = new HashSet<>();
        @Getter final Set<UUID>  rcvRespIds = new HashSet<>();

        @Override
        public void onDataMsgReceived(RcvMessage msg)
        {
            rcvMessagesContents.add(msg.getContents().getInt(msg.getContentOffset()));

            rcvMessagesSecuences.add(msg.getSequenceNumber());
        }

        @Override
        public void onEncryptedDataMsgReceived(RcvMessage msg)
        {
            rcvEncryptedMessagesCount.getAndIncrement();
        }

        @Override
        public void onDataRequestMsgReceived(RcvRequest request)
        {
            rcvRequestsByContentValue.add(request.getContents().getInt(request.getContentOffset()));
            rcvRequestsBySeqNumber.add(request.getSequenceNumber());
            rcvRequestIds.add(request.getRequestId());
        }

        @Override
        public void onDataResponseMsgReceived(RcvResponse response)
        {
            rcvResponses.add(response.getContents().getInt(response.getContentOffset()));
            rcvRequestIds.add(response.getOriginalRequestId());
        }

        @Override
        public void onHeartbeatRequestMsgReceived(UUID senderInstanceId, UUID requestId)
        {
            rcvHeartbeatRequestIds.add(requestId);
        }
    }

    private class SimpleListener implements ISubscribersPollerListener
    {
        @Getter final List<IRcvMessage> rcvMessages = new LinkedList<>();

        @Override
        public void onDataMsgReceived(RcvMessage msg)
        {
            rcvMessages.add(msg.promote());
        }

        @Override
        public void onEncryptedDataMsgReceived(RcvMessage msg)
        {

        }

        @Override
        public void onDataRequestMsgReceived(RcvRequest request)
        {
        }

        @Override
        public void onDataResponseMsgReceived(RcvResponse response)
        {
        }

        @Override
        public void onHeartbeatRequestMsgReceived(UUID senderInstanceId, UUID requestId)
        {

        }
    }
}