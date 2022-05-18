package com.bbva.kyof.vega.autodiscovery.subscriber;

import com.bbva.kyof.vega.TestConstants;
import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.autodiscovery.model.*;
import com.bbva.kyof.vega.config.general.AutoDiscoType;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.msg.BaseHeader;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import com.bbva.kyof.vega.serialization.IUnsafeSerializable;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.junit.*;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by cnebrera on 04/08/16.
 */
public class AbstractAutodiscReceiverTest implements IAutodiscGlobalEventListener
{
    private final ByteBuffer sendBuffer = ByteBuffer.allocate(1024);
    private final UnsafeBufferSerializer sendBufferSerializer = new UnsafeBufferSerializer();

    private static final UUID SENDER_INSTANCE_ID = UUID.randomUUID();
    private static final int STREAM_ID = 2;
    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static Publication PUBLICATION;

    private static AutoDiscReceiverImpl RECEIVER;
    private static final GlobalEventListener GLOBAL_EVENT_LISTENER = new GlobalEventListener();

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx = new Aeron.Context();
        ctx.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON = Aeron.connect(ctx);

        // Create the publication
        final String ipcChannel = AeronChannelHelper.createIpcChannelString();
        PUBLICATION = AERON.addPublication(ipcChannel, STREAM_ID);

        // Create the configuration, with 300 millis refresh interval
        final AutoDiscoveryConfig config = AutoDiscoveryConfig.builder().
                autoDiscoType(AutoDiscoType.MULTICAST).
                refreshInterval(100L).
                timeout(500L).build();
        config.completeAndValidateConfig();

        // Create the test receivers
        RECEIVER = new AutoDiscReceiverImpl(UUID.randomUUID(), AERON, config, GLOBAL_EVENT_LISTENER);

        // Wait and let the connections to be created
        Thread.sleep(2000);
    }

    @AfterClass
    public static void afterClass()
    {
        PUBLICATION.close();
        RECEIVER.close();
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Before
    public void setUp() throws Exception
    {

    }

    @Test
    public void testWrongReceptions() throws Exception
    {
        final AutoDiscInstanceInfo instanceInfo = new AutoDiscInstanceInfo("instance1", UUID.randomUUID(), 12, 23, 55,TestConstants.EMPTY_HOSTNAME, 12, 23, 56,TestConstants.EMPTY_HOSTNAME);

        // Wrong message type
        this.sendMessage((byte)128, instanceInfo);

        // Wait a bit and force message reception
        Thread.sleep(100);
        this.callReceiverLifeCycle();

        // Try now with wrong version
        // Wrong message type
        this.sendMessage((byte)128, instanceInfo, true);
        Thread.sleep(100);
        this.callReceiverLifeCycle();
    }

    @Test
    public void testInstanceSubscriptionUnsubscription() throws Exception
    {
        final AutoDiscInstanceInfo instanceInfo = new AutoDiscInstanceInfo("instance1", UUID.randomUUID(), 12, 23, 55, TestConstants.EMPTY_HOSTNAME, 12, 23, 56,TestConstants.EMPTY_HOSTNAME);
        final AutoDiscInstanceListener listener = new AutoDiscInstanceListener();

        Assert.assertTrue(RECEIVER.subscribeToInstances(listener));
        Assert.assertFalse(RECEIVER.subscribeToInstances(listener));

        this.sendMessage(MsgType.AUTO_DISC_INSTANCE, instanceInfo);

        // Wait a bit and force message reception
        Thread.sleep(100);
        this.callReceiverLifeCycle();

        // There should be now a new instance info event
        Assert.assertEquals(listener.receivedInstanceMsg, instanceInfo);
        Assert.assertEquals(GLOBAL_EVENT_LISTENER.receivedInstanceInfo, instanceInfo);
        listener.reset();
        GLOBAL_EVENT_LISTENER.reset();

        // Now call again, since there has been no time out, there shouldn't be a new event
        this.sendMessage(MsgType.AUTO_DISC_INSTANCE, instanceInfo);
        this.callReceiverLifeCycle();

        Assert.assertNull(listener.receivedInstanceMsg);
        Assert.assertNull(GLOBAL_EVENT_LISTENER.receivedInstanceInfo);

        // Wait 300 millis, the time out is in 500, there should be no time out
        Thread.sleep(300);
        this.callReceiverLifeCycle();
        Assert.assertNull(listener.timedOutInstanceMsg);

        // Now wait a bit more, there should be a time out
        Thread.sleep(300);
        this.callReceiverLifeCycle();
        Assert.assertEquals(listener.timedOutInstanceMsg, instanceInfo);

        // If we send again, there should be a new element again
        this.sendMessage(MsgType.AUTO_DISC_INSTANCE, instanceInfo);
        Thread.sleep(100);
        this.callReceiverLifeCycle();
        Assert.assertEquals(listener.receivedInstanceMsg, instanceInfo);

        // Now unsubscribe
        Assert.assertTrue(RECEIVER.unsubscribeFromInstances(listener));
        Assert.assertFalse(RECEIVER.unsubscribeFromInstances(listener));
        listener.reset();

        // Wait for time out, nothing should happen, we have unsubscribed
        Thread.sleep(600);
        this.callReceiverLifeCycle();
        Assert.assertNull(listener.receivedInstanceMsg);
        Assert.assertNull(listener.timedOutInstanceMsg);

        // Send a new instance message
        // If we send again, there should be a new element again
        this.sendMessage(MsgType.AUTO_DISC_INSTANCE, instanceInfo);
        Thread.sleep(100);
        this.callReceiverLifeCycle();
        Assert.assertNull(listener.receivedInstanceMsg);
        Assert.assertNull(listener.timedOutInstanceMsg);

        // Subscribe again, the existing instance should be received immediately
        Assert.assertTrue(RECEIVER.subscribeToInstances(listener));
        Assert.assertEquals(listener.receivedInstanceMsg, instanceInfo);
    }

    @Test
    public void testTopicSubscriptionUnsubscription() throws Exception
    {
        final AutoDiscTopicInfo topicInfo = new AutoDiscTopicInfo(SENDER_INSTANCE_ID, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic");
        final AutoDiscTopicSocketInfo topicSocketInfo = new AutoDiscTopicSocketInfo(SENDER_INSTANCE_ID, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic", UUID.randomUUID(), 1, 2, 4, TestConstants.EMPTY_HOSTNAME);
        final AutoDiscTopicListener listener = new AutoDiscTopicListener();

        Assert.assertTrue(RECEIVER.subscribeToTopic("topic", AutoDiscTransportType.PUB_MUL, listener));
        Assert.assertFalse(RECEIVER.subscribeToTopic("topic", AutoDiscTransportType.PUB_MUL, listener));

        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfo);
        Thread.sleep(100);
        this.callReceiverLifeCycle();

        // There should be no adverts, we are not subscribed to that transport type
        Assert.assertNull(listener.receivedTopicMsg);
        Assert.assertNull(listener.receivedTopicSocketMsg);

        // Now subscribe to the right transport type
        Assert.assertTrue(RECEIVER.subscribeToTopic("topic", AutoDiscTransportType.PUB_IPC, listener));
        // Wait a bit and force message reception
        Thread.sleep(100);
        this.callReceiverLifeCycle();

        // The information should have been forwarded immediately!
        Assert.assertEquals(listener.receivedTopicMsg, topicInfo);
        Assert.assertNull(listener.receivedTopicSocketMsg);
        Assert.assertEquals(GLOBAL_EVENT_LISTENER.receivedTopicInfo, topicInfo);
        GLOBAL_EVENT_LISTENER.reset();

        // Now send a topic socket message
        this.sendMessage(MsgType.AUTO_DISC_TOPIC_SOCKET, topicSocketInfo);
        Thread.sleep(100);
        this.callReceiverLifeCycle();

        // Now both should have arrived
        Assert.assertEquals(listener.receivedTopicMsg, topicInfo);
        Assert.assertEquals(listener.receivedTopicSocketMsg, topicSocketInfo);
        listener.reset();

        // Now call again, since there has been no time out, there shouldn't be a new event
        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfo);
        this.sendMessage(MsgType.AUTO_DISC_TOPIC_SOCKET, topicSocketInfo);
        // Call twice to force both messages reception
        Thread.sleep(100);
        this.callReceiverLifeCycle();
        this.callReceiverLifeCycle();

        Assert.assertNull(listener.receivedTopicMsg);
        Assert.assertNull(listener.receivedTopicSocketMsg);

        // Wait 600 millis, the time out is in 500, there should be time out
        Thread.sleep(600);
        this.callReceiverLifeCycle();
        Assert.assertEquals(listener.timedTopicMsg, topicInfo);
        Assert.assertEquals(listener.timedTopicSocketMsg, topicSocketInfo);

        // If we send again, there should be a new element again
        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfo);
        this.sendMessage(MsgType.AUTO_DISC_TOPIC_SOCKET, topicSocketInfo);
        Thread.sleep(100);
        this.callReceiverLifeCycle();
        this.callReceiverLifeCycle();
        Assert.assertEquals(listener.receivedTopicMsg, topicInfo);
        Assert.assertEquals(listener.receivedTopicSocketMsg, topicSocketInfo);

        // Now unsubscribe
        Assert.assertTrue(RECEIVER.unsubscribeFromTopic("topic", AutoDiscTransportType.PUB_IPC, listener));
        Assert.assertFalse(RECEIVER.unsubscribeFromTopic("topic", AutoDiscTransportType.PUB_IPC, listener));
        Assert.assertTrue(RECEIVER.unsubscribeFromTopic("topic", AutoDiscTransportType.PUB_MUL, listener));
        Assert.assertFalse(RECEIVER.unsubscribeFromTopic("topic", AutoDiscTransportType.PUB_MUL, listener));
        listener.reset();

        // Wait for time out, nothing should happen, we have un subscribed
        Thread.sleep(600);
        this.callReceiverLifeCycle();
        Assert.assertNull(listener.receivedTopicMsg);
        Assert.assertNull(listener.receivedTopicSocketMsg);

        // Send a new instance message
        // If we send again, there should be a new element again
        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfo);
        this.sendMessage(MsgType.AUTO_DISC_TOPIC_SOCKET, topicSocketInfo);
        Thread.sleep(100);
        this.callReceiverLifeCycle();
        this.callReceiverLifeCycle();
        Assert.assertNull(listener.receivedTopicMsg);
        Assert.assertNull(listener.receivedTopicSocketMsg);

        // Subscribe again, the existing info should be received immediately
        Assert.assertTrue(RECEIVER.subscribeToTopic("topic", AutoDiscTransportType.PUB_IPC, listener));
        Assert.assertEquals(listener.receivedTopicMsg, topicInfo);
        Assert.assertEquals(listener.receivedTopicSocketMsg, topicSocketInfo);
    }

    @Test
    public void patternSubscriptionUnsubscription() throws Exception
    {
        final AutoDiscTopicInfo topicInfo = new AutoDiscTopicInfo(SENDER_INSTANCE_ID, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic");
        final AutoDiscTopicInfo topicInfo2 = new AutoDiscTopicInfo(SENDER_INSTANCE_ID, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic2");
        final AutoDiscTopicInfo topicInfo3 = new AutoDiscTopicInfo(SENDER_INSTANCE_ID, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "atopic3");
        final AutoDiscPatternListener listener = new AutoDiscPatternListener();

        Assert.assertTrue(RECEIVER.subscribeToPubPattern("t.*", listener));
        Assert.assertFalse(RECEIVER.subscribeToPubPattern("t.*", new AutoDiscPatternListener()));

        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfo3);
        Thread.sleep(100);
        this.callReceiverLifeCycle();

        // There should be no adverts, we are not subscribed to the pattern that match the topic 3
        Assert.assertNull(listener.topicAdded);

        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfo);
        Thread.sleep(100);
        this.callReceiverLifeCycle();

        // The information should have be there
        Assert.assertEquals(listener.topicAdded, topicInfo.getTopicName());
        Assert.assertNull(listener.topicRemoved);

        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfo2);
        Thread.sleep(100);
        this.callReceiverLifeCycle();

        // The information should have be there
        Assert.assertEquals(listener.topicAdded, topicInfo2.getTopicName());
        Assert.assertNull(listener.topicRemoved);

        // Wait 600 millis, the time out is in 500, there should be time out
        // Since both have timed out, the last one updated should be the last to time out.
        // First lyfecyle will process the topic3 time out, that wont change anything
        Thread.sleep(600);
        this.callReceiverLifeCycle();
        Assert.assertNull(listener.topicRemoved);
        // The second lyfecycle process the right time out
        this.callReceiverLifeCycle();
        Assert.assertEquals(listener.topicRemoved, topicInfo.getTopicName());

        // If we send again, there should be a new element again
        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfo);
        Thread.sleep(100);
        this.callReceiverLifeCycle();
        Assert.assertEquals(listener.topicAdded, topicInfo.getTopicName());

        // Now unsubscribe
        Assert.assertTrue(RECEIVER.unsubscribeFromPubPattern("t.*"));
        Assert.assertFalse(RECEIVER.unsubscribeFromPubPattern("t.*"));
        listener.reset();

        // Wait for time out, nothing should happen, we have un subscribed
        Thread.sleep(600);
        this.callReceiverLifeCycle();
        Assert.assertNull(listener.topicRemoved);

        // Send a new message
        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfo);
        Thread.sleep(100);
        this.callReceiverLifeCycle();
        Assert.assertNull(listener.topicAdded);

        // Subscribe again, the existing info should be received immediately
        Assert.assertTrue(RECEIVER.subscribeToPubPattern("t.*", listener));
        Assert.assertEquals(listener.topicAdded, topicInfo.getTopicName());
    }

    private void callReceiverLifeCycle()
    {
        RECEIVER.pollNextMessage();
        RECEIVER.checkNextTimeout();
    }

    private void sendMessage(final byte msgType, final IUnsafeSerializable serializable)
    {
        this.sendMessage(msgType, serializable, false);
    }

    private void sendMessage(final byte msgType, final IUnsafeSerializable serializable, boolean wrongVersion)
    {
        // Prepare the send buffer
        this.sendBuffer.clear();
        this.sendBufferSerializer.wrap(this.sendBuffer);

        // Set msg type and write the base header
        BaseHeader baseHeader;

        if (wrongVersion)
        {
            baseHeader = new BaseHeader(msgType, Version.toIntegerRepresentation((byte)55, (byte)3, (byte)1));
        }
        else
        {
            baseHeader = new BaseHeader(msgType, Version.LOCAL_VERSION);
        }

        // Write the base header
        baseHeader.toBinary(this.sendBufferSerializer);

        // Serialize the message
        serializable.toBinary(this.sendBufferSerializer);

        // Send the message
        PUBLICATION.offer(this.sendBufferSerializer.getInternalBuffer(), 0, this.sendBufferSerializer.getOffset());
    }

    @Override
    public void onNewInstanceInfo(AutoDiscInstanceInfo info)
    {
        // We will test this in the
    }

    @Override
    public void onNewTopicInfo(AutoDiscTopicInfo info)
    {

    }

    static class GlobalEventListener implements IAutodiscGlobalEventListener
    {
        AutoDiscInstanceInfo receivedInstanceInfo;
        AutoDiscTopicInfo receivedTopicInfo;

        @Override
        public void onNewInstanceInfo(AutoDiscInstanceInfo info)
        {
            this.receivedInstanceInfo = info;
        }

        @Override
        public void onNewTopicInfo(AutoDiscTopicInfo info)
        {
            this.receivedTopicInfo = info;
        }

        public void reset()
        {
            receivedInstanceInfo = null;
            receivedTopicInfo = null;
        }
    }

    static class AutoDiscPatternListener implements IAutodiscPubTopicPatternListener
    {
        String topicAdded;
        String topicRemoved;

        public void reset()
        {
            topicAdded = null;
            topicRemoved = null;
        }

        @Override
        public void onNewPubTopicForPattern(AutoDiscTopicInfo topicInfo, String topicPattern)
        {
            topicAdded = topicInfo.getTopicName();
        }

        @Override
        public void onPubTopicForPatternRemoved(AutoDiscTopicInfo topicInfo, String topicPattern)
        {
            topicRemoved = topicInfo.getTopicName();
        }
    }

    static class AutoDiscInstanceListener implements IAutodiscInstanceListener
    {
        AutoDiscInstanceInfo receivedInstanceMsg;
        AutoDiscInstanceInfo timedOutInstanceMsg;

        @Override
        public void onNewAutoDiscInstanceInfo(AutoDiscInstanceInfo info)
        {
            receivedInstanceMsg = info;
        }

        @Override
        public void onTimedOutAutoDiscInstanceInfo(AutoDiscInstanceInfo info)
        {
            timedOutInstanceMsg = info;
        }

        public void reset()
        {
            receivedInstanceMsg = null;
            timedOutInstanceMsg = null;
        }
    }

    static class AutoDiscTopicListener implements IAutodiscTopicSubListener
    {
        AutoDiscTopicInfo receivedTopicMsg;
        AutoDiscTopicInfo timedTopicMsg;
        AutoDiscTopicSocketInfo receivedTopicSocketMsg;
        AutoDiscTopicSocketInfo timedTopicSocketMsg;

        public void reset()
        {
            receivedTopicMsg = null;
            timedTopicMsg = null;
            receivedTopicSocketMsg = null;
            timedTopicSocketMsg = null;
        }

        @Override
        public void onNewAutoDiscTopicInfo(AutoDiscTopicInfo info)
        {
            this.receivedTopicMsg = info;
        }

        @Override
        public void onTimedOutAutoDiscTopicInfo(AutoDiscTopicInfo info)
        {
            this.timedTopicMsg = info;
        }

        @Override
        public void onNewAutoDiscTopicSocketInfo(AutoDiscTopicSocketInfo info)
        {
            this.receivedTopicSocketMsg = info;
        }

        @Override
        public void onTimedOutAutoDiscTopicSocketInfo(AutoDiscTopicSocketInfo info)
        {
            this.timedTopicSocketMsg = info;
        }
    }

    static class AutoDiscReceiverImpl extends AbstractAutodiscReceiver
    {
        AutoDiscReceiverImpl(UUID instanceId, Aeron aeron, AutoDiscoveryConfig config, IAutodiscGlobalEventListener globalListener)
        {
            super(instanceId, aeron, config, globalListener);
        }

        @Override
        public Subscription createSubscription(UUID instanceId, Aeron aeron, AutoDiscoveryConfig config)
        {
            final String ipcChannel = AeronChannelHelper.createIpcChannelString();
            return aeron.addSubscription(ipcChannel, STREAM_ID);
        }

        protected boolean processAutoDiscDaemonServerInfoMsg(AutoDiscDaemonServerInfo autoDiscDaemonServerInfo)
        {
            return false;
        }

        protected int checkAutoDiscDaemonServerInfoTimeouts()
        {
            return 0;
        }
    }
}