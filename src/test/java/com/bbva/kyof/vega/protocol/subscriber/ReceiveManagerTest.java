package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.autodiscovery.daemon.CommandLineParserTest;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.config.general.ConfigReader;
import com.bbva.kyof.vega.config.general.ConfigReaderTest;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.msg.*;
import com.bbva.kyof.vega.msg.lost.IMsgLostReport;
import com.bbva.kyof.vega.protocol.AutoDiscManagerMock;
import com.bbva.kyof.vega.protocol.common.AsyncRequestManager;
import com.bbva.kyof.vega.protocol.common.KeySecurityType;
import com.bbva.kyof.vega.protocol.common.SecurityParams;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.control.ISecuredMsgsDecoder;
import com.bbva.kyof.vega.protocol.control.ISecurityRequesterNotifier;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.easymock.EasyMock;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Created by cnebrera on 11/08/16.
 */
public class ReceiveManagerTest
{
    private static final String KEYS_DIR_PATH = CommandLineParserTest.class.getClassLoader().getResource("keys").getPath();
    private static final String validConfigFile = ConfigReaderTest.class.getClassLoader().getResource("config/subscribersManagerTestConfig.xml").getPath();

    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static SubnetAddress SUBNET_ADDRESS;
    private static int IP_ADDRESS;
    private static VegaContext VEGA_CONTEXT;
    private static AutoDiscManagerMock AUTO_DISC_MANAGER_MOCK;

    private static ReceiveManager RECEIVER_MANAGER;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx1 = new Aeron.Context();
        ctx1.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON = Aeron.connect(ctx1);

        SUBNET_ADDRESS = InetUtil.getDefaultSubnet();
        IP_ADDRESS = InetUtil.convertIpAddressToInt(SUBNET_ADDRESS.getIpAddres().getHostAddress());
        VEGA_CONTEXT = new VegaContext(AERON, ConfigReader.readConfiguration(validConfigFile));

        final AsyncRequestManager requestManager = EasyMock.createNiceMock(AsyncRequestManager.class);
        EasyMock.replay(requestManager);

        VEGA_CONTEXT.setAsyncRequestManager(requestManager);

        // Initialize the security
        final SecurityParams securityParams = SecurityParams.builder().
                keySecurityType(KeySecurityType.PLAIN_KEY_FILE).
                privateKeyDirPath(KEYS_DIR_PATH).
                publicKeysDirPath(KEYS_DIR_PATH).
                securityId(11111).build();

        // Set in the vega contexts
        VEGA_CONTEXT.initializeSecurity(securityParams);

        // Mock auto-discovery manager calls
        AUTO_DISC_MANAGER_MOCK = new AutoDiscManagerMock();
        VEGA_CONTEXT.setAutodiscoveryManager(AUTO_DISC_MANAGER_MOCK.getMock());

        final ISecuredMsgsDecoder messagesDecoder = EasyMock.createNiceMock(ISecuredMsgsDecoder.class);
        final ISecurityRequesterNotifier requesterNotifier = EasyMock.createNiceMock(ISecurityRequesterNotifier.class);
        EasyMock.replay(messagesDecoder, requesterNotifier);

        RECEIVER_MANAGER = new ReceiveManager(VEGA_CONTEXT, messagesDecoder, requesterNotifier);

        // Give it time to start
        Thread.sleep(1000);
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        RECEIVER_MANAGER.close();
        RECEIVER_MANAGER.close();
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Test(expected = VegaException.class)
    public void testSubscribeNonConfigTopic() throws Exception
    {
        RECEIVER_MANAGER.subscribeToTopic("ttttt", new ReceiverListener());
    }

    @Test(expected = VegaException.class)
    public void testUnsubscribeNonConfigTopic() throws Exception
    {
        RECEIVER_MANAGER.unsubscribeFromTopic("ttttt");
    }

    @Test(expected = VegaException.class)
    public void testSubscribeTwice() throws Exception
    {
        RECEIVER_MANAGER.subscribeToTopic("ucastTopicSubTwice", new ReceiverListener());
        RECEIVER_MANAGER.subscribeToTopic("ucastTopicSubTwice", new ReceiverListener());
    }

    @Test(expected = VegaException.class)
    public void testUnsubscribeTwice() throws Exception
    {
        RECEIVER_MANAGER.subscribeToTopic("ucastTopicUnSubTwice", new ReceiverListener());
        RECEIVER_MANAGER.unsubscribeFromTopic("ucastTopicUnSubTwice");
        RECEIVER_MANAGER.unsubscribeFromTopic("ucastTopicUnSubTwice");
    }

    @Test
    public void testSubscribeUnsubscribe() throws Exception
    {
        RECEIVER_MANAGER.subscribeToTopic("ucastTopic", new ReceiverListener());
        RECEIVER_MANAGER.subscribeToTopic("mcastTopic", new ReceiverListener());
        RECEIVER_MANAGER.subscribeToTopic("ipcTopic", new ReceiverListener());

        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("ucastTopic"));
        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("mcastTopic"));
        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("ipcTopic"));

        RECEIVER_MANAGER.unsubscribeFromTopic("ucastTopic");
        RECEIVER_MANAGER.unsubscribeFromTopic("mcastTopic");
        RECEIVER_MANAGER.unsubscribeFromTopic("ipcTopic");

        assertNull(RECEIVER_MANAGER.getTopicSubscriber("ucastTopic"));
        assertNull(RECEIVER_MANAGER.getTopicSubscriber("mcastTopic"));
        assertNull(RECEIVER_MANAGER.getTopicSubscriber("ipcTopic"));
    }

    @Test(expected = VegaException.class)
    public void testPatternSubscribeUnsubscribe() throws Exception
    {
        RECEIVER_MANAGER.subscribeToPattern("*.twicePatternSub", new ReceiverListener());
        RECEIVER_MANAGER.subscribeToPattern("*.twicePatternSub", new ReceiverListener());
    }

    @Test(expected = VegaException.class)
    public void testPatternUnsubscribeTwice() throws Exception
    {
        RECEIVER_MANAGER.subscribeToPattern("*.twicePatternUn", new ReceiverListener());
        RECEIVER_MANAGER.unsubscribefromPattern("*.twicePatternUn");
        RECEIVER_MANAGER.unsubscribefromPattern("*.twicePatternUn");
    }

    @Test
    public void testSecureSubscribe() throws Exception
    {
        RECEIVER_MANAGER.subscribeToTopic("sTopic", new ReceiverListener());
        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("sTopic"));

        RECEIVER_MANAGER.unsubscribeFromTopic("sTopic");
        assertNull(RECEIVER_MANAGER.getTopicSubscriber("sTopic"));
    }

    @Test
    public void testPatternSubscribe() throws Exception
    {
        // Calibration check
        Assert.assertFalse(RECEIVER_MANAGER.isSubscribedToPattern("*.utopicPattern"));

        // Subscribe to pattern
        RECEIVER_MANAGER.subscribeToPattern("*.utopicPattern", new ReceiverListener());
        RECEIVER_MANAGER.subscribeToPattern("*.itopicPattern", new ReceiverListener());

        Assert.assertTrue(RECEIVER_MANAGER.isSubscribedToPattern("*.utopicPattern"));
        Assert.assertTrue(RECEIVER_MANAGER.isSubscribedToPattern("*.itopicPattern"));

        // Force a topic subscription due to new advert
        RECEIVER_MANAGER.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"utopicPattern"), "*.utopicPattern");
        RECEIVER_MANAGER.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(),"itopicPattern"), "*.itopicPattern");

        // It should be subscribed to both TOPICS
        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("utopicPattern"));
        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("itopicPattern"));

        // Force an un-subscription due to advert removed
        RECEIVER_MANAGER.onPubTopicForPatternRemoved(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"utopicPattern"), "*.utopicPattern");
        assertNull(RECEIVER_MANAGER.getTopicSubscriber("utopicPattern"));

        // Advert on non configured topic
        RECEIVER_MANAGER.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(),"LOL"), "*.utopicPattern");
        assertNull(RECEIVER_MANAGER.getTopicSubscriber("LOL"));

        // Try unsubscribe from pattern, should unsubscribe both topic and pattern
        RECEIVER_MANAGER.unsubscribefromPattern("*.itopicPattern");
        Assert.assertFalse(RECEIVER_MANAGER.isSubscribedToPattern("*.itopicPattern"));
        assertNull(RECEIVER_MANAGER.getTopicSubscriber("itopicPattern"));

        // Launch advert again, this time since there is no subscription nothing will happen
        RECEIVER_MANAGER.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(),"itopicPattern"), "*.itopicPattern");
        Assert.assertFalse(RECEIVER_MANAGER.isSubscribedToPattern("*.itopicPattern"));
        assertNull(RECEIVER_MANAGER.getTopicSubscriber("itopicPattern"));

        // Subscribe again
        RECEIVER_MANAGER.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"utopicPattern"), "*.utopicPattern");
        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("utopicPattern"));

        // Now launch a wrong advert on non existing pattern
        RECEIVER_MANAGER.onPubTopicForPatternRemoved(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"utopicPattern"), "LOL*.utopicPattern");
        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("utopicPattern"));

        // Now launch a wrong advert on non configured topic
        RECEIVER_MANAGER.onPubTopicForPatternRemoved(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"LOLutopicPattern"), "*.utopicPattern");
        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("utopicPattern"));

        // Last test is to process a removed topic in mcast or ipc
        RECEIVER_MANAGER.subscribeToPattern("*.itopicPattern", new ReceiverListener());
        RECEIVER_MANAGER.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(),"itopicPattern"), "*.itopicPattern");

        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("itopicPattern"));

        RECEIVER_MANAGER.onPubTopicForPatternRemoved(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(),"itopicPattern"), "*.itopicPattern");
        assertNull(RECEIVER_MANAGER.getTopicSubscriber("itopicPattern"));

        // Finally just unsubscribe from both
        RECEIVER_MANAGER.unsubscribefromPattern("*.utopicPattern");
        RECEIVER_MANAGER.unsubscribefromPattern("*.itopicPattern");
    }

    @Test
    public void testSecurePatternSubscribe() throws Exception
    {
        // Calibration check
        Assert.assertFalse(RECEIVER_MANAGER.isSubscribedToPattern("*.stopicPattern"));

        // Subscribe to patterns
        RECEIVER_MANAGER.subscribeToPattern("*.utopicPattern", new ReceiverListener());
        RECEIVER_MANAGER.subscribeToPattern("*.stopicPattern", new ReceiverListener());
        RECEIVER_MANAGER.subscribeToPattern("*.2stopicPattern", new ReceiverListener());
        Assert.assertTrue(RECEIVER_MANAGER.isSubscribedToPattern("*.stopicPattern"));
        Assert.assertTrue(RECEIVER_MANAGER.isSubscribedToPattern("*.2stopicPattern"));

        // Force a topic subscription due to new advert that is secure but we are not allowed to subscribe to it
        RECEIVER_MANAGER.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"2stopicPattern", 77), "*.2stopicPattern");
        // It should not subscribe
        assertNull(RECEIVER_MANAGER.getTopicSubscriber("2stopicPattern"));

        // Force a topic subscription due to new advert that is not secure
        RECEIVER_MANAGER.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"stopicPattern"), "*.stopicPattern");
        // It should not subscribe
        assertNull(RECEIVER_MANAGER.getTopicSubscriber("stopicPattern"));

        // Force a topic subscription due to new advert that is secure but is not allowed to publish
        RECEIVER_MANAGER.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"stopicPattern", 77), "*.stopicPattern");
        // It should not subscribe
        assertNull(RECEIVER_MANAGER.getTopicSubscriber("stopicPattern"));

        // Force a topic subscription due to new advert that is secure is allowed to publish but we have no RSA key for the publisher id
        RECEIVER_MANAGER.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"stopicPattern", 33333), "*.stopicPattern");
        // It should not subscribe
        assertNull(RECEIVER_MANAGER.getTopicSubscriber("stopicPattern"));

        // Force a topic subscription with a secure advert on a non secure topic
        RECEIVER_MANAGER.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"utopicPattern", 6666), "*.utopicPattern");
        // It should not subscribe
        assertNull(RECEIVER_MANAGER.getTopicSubscriber("utopicPattern"));

        // Finally one that should work!
        RECEIVER_MANAGER.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"stopicPattern", 22222), "*.stopicPattern");
        // It should not subscribe
        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("stopicPattern"));

        // Finally just unsubscribe from all
        RECEIVER_MANAGER.unsubscribefromPattern("*.stopicPattern");
        RECEIVER_MANAGER.unsubscribefromPattern("*.2stopicPattern");
    }

    @Test
    public void testPatternTopicSubscribeMix() throws Exception
    {
        // Calibration check
        assertNull(RECEIVER_MANAGER.getTopicSubscriber("*.uMixTopicPattern"));

        // Subscribe to pattern and force a topic subscription
        RECEIVER_MANAGER.subscribeToPattern("*.uMixTopicPattern", new ReceiverListener());
        RECEIVER_MANAGER.subscribeToPattern("*.uMixTopicPattern.*", new ReceiverListener());
        RECEIVER_MANAGER.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"uMixPattern"), "*.uMixTopicPattern");
        RECEIVER_MANAGER.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"uMixPattern"), "*.uMixTopicPattern.*");

        // It should be subscribed to both TOPICS
        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("uMixPattern"));

        // Now subscribe normally
        RECEIVER_MANAGER.subscribeToTopic("uMixPattern", new ReceiverListener());
        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("uMixPattern"));

        // Now force the pattern unsubscriptions, it should still be subscribed!
        RECEIVER_MANAGER.onPubTopicForPatternRemoved(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"uMixPattern"), "*.uMixTopicPattern");
        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("uMixPattern"));
        RECEIVER_MANAGER.onPubTopicForPatternRemoved(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"uMixPattern"), "*.uMixTopicPattern.*");
        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("uMixPattern"));

        // Now unsubscribe the normal topic, it should be gonce
        RECEIVER_MANAGER.unsubscribeFromTopic("uMixPattern");
        assertNull(RECEIVER_MANAGER.getTopicSubscriber("uMixPattern"));

        //-------------------------- The other way around --------------------------
        // Now subscribe normally
        RECEIVER_MANAGER.subscribeToTopic("uMixPattern", new ReceiverListener());
        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("uMixPattern"));

        RECEIVER_MANAGER.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"uMixPattern"), "*.uMixTopicPattern");
        RECEIVER_MANAGER.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"uMixPattern"), "*.uMixTopicPattern.*");

        // Now unsubscribe the normal topic, it should be there still
        RECEIVER_MANAGER.unsubscribeFromTopic("uMixPattern");
        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("uMixPattern"));

        // Now force the pattern unsubscriptions
        RECEIVER_MANAGER.onPubTopicForPatternRemoved(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"uMixPattern"), "*.uMixTopicPattern");
        assertNotNull(RECEIVER_MANAGER.getTopicSubscriber("uMixPattern"));
        RECEIVER_MANAGER.onPubTopicForPatternRemoved(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"uMixPattern"), "*.uMixTopicPattern.*");
        assertNull(RECEIVER_MANAGER.getTopicSubscriber("uMixPattern"));
    }

    @Test(expected = VegaException.class)
    public void testSubscribeOnClosed() throws Exception
    {
        final ISecuredMsgsDecoder messagesDecoder = EasyMock.createNiceMock(ISecuredMsgsDecoder.class);
        final ISecurityRequesterNotifier requesterNotifier = EasyMock.createNiceMock(ISecurityRequesterNotifier.class);
        EasyMock.replay(messagesDecoder, requesterNotifier);
        final ReceiveManager receiveManager = new ReceiveManager(VEGA_CONTEXT, messagesDecoder, requesterNotifier);
        receiveManager.close();
        receiveManager.subscribeToTopic("topic", new ReceiverListener());
    }

    @Test(expected = VegaException.class)
    public void testUnsubscribeOnClosed() throws Exception
    {
        final ISecuredMsgsDecoder messagesDecoder = EasyMock.createNiceMock(ISecuredMsgsDecoder.class);
        final ISecurityRequesterNotifier requesterNotifier = EasyMock.createNiceMock(ISecurityRequesterNotifier.class);
        EasyMock.replay(messagesDecoder, requesterNotifier);
        final ReceiveManager receiveManager = new ReceiveManager(VEGA_CONTEXT, messagesDecoder, requesterNotifier);
        receiveManager.close();
        receiveManager.unsubscribeFromTopic("topic");
    }

    @Test(expected = VegaException.class)
    public void testSubscribeToPatternOnClosed() throws Exception
    {
        final ISecuredMsgsDecoder messagesDecoder = EasyMock.createNiceMock(ISecuredMsgsDecoder.class);
        final ISecurityRequesterNotifier requesterNotifier = EasyMock.createNiceMock(ISecurityRequesterNotifier.class);
        EasyMock.replay(messagesDecoder, requesterNotifier);
        final ReceiveManager receiveManager = new ReceiveManager(VEGA_CONTEXT, messagesDecoder, requesterNotifier);
        receiveManager.close();
        receiveManager.subscribeToPattern("topic", new ReceiverListener());
    }

    @Test(expected = VegaException.class)
    public void testUnsubscribeToPatternOnClosed() throws Exception
    {
        final ISecuredMsgsDecoder messagesDecoder = EasyMock.createNiceMock(ISecuredMsgsDecoder.class);
        final ISecurityRequesterNotifier requesterNotifier = EasyMock.createNiceMock(ISecurityRequesterNotifier.class);
        EasyMock.replay(messagesDecoder, requesterNotifier);
        final ReceiveManager receiveManager = new ReceiveManager(VEGA_CONTEXT, messagesDecoder, requesterNotifier);
        receiveManager.close();
        receiveManager.unsubscribefromPattern("topic");
    }

    @Test
    public void testEventsOnClosedReceiver() throws Exception
    {
        final ISecuredMsgsDecoder messagesDecoder = EasyMock.createNiceMock(ISecuredMsgsDecoder.class);
        final ISecurityRequesterNotifier requesterNotifier = EasyMock.createNiceMock(ISecurityRequesterNotifier.class);
        EasyMock.replay(messagesDecoder, requesterNotifier);
        final ReceiveManager receiveManager = new ReceiveManager(VEGA_CONTEXT, messagesDecoder, requesterNotifier);
        receiveManager.close();
        receiveManager.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"topic"), "topic");
        receiveManager.onPubTopicForPatternRemoved(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(),"topic"), "topic");
        receiveManager.onNewAutoDiscInstanceInfo(new AutoDiscInstanceInfo("name", UUID.randomUUID(), 23, 345, 12, 33, 44, 55));
        receiveManager.onTimedOutAutoDiscInstanceInfo(new AutoDiscInstanceInfo("name", UUID.randomUUID(), 23, 345, 12, 33, 44, 55));
    }

    @Test
    public void testNewAutodiscInstanceInfo() throws Exception
    {
        final AutoDiscInstanceInfo instanceInfo1 = new AutoDiscInstanceInfo("instName1", UUID.randomUUID(), IP_ADDRESS, 33333, 2, IP_ADDRESS, 33333, 3);

        // Add response publishers and check
        RECEIVER_MANAGER.onNewAutoDiscInstanceInfo(instanceInfo1);

        // Remove response publishers and check
        RECEIVER_MANAGER.onTimedOutAutoDiscInstanceInfo(instanceInfo1);
    }
    
    @Test
    public void getResponseSubParams()
    {
        assertNotNull(RECEIVER_MANAGER.getResponseSubscriberParams());
    }

    @Test
    public void onDataMessageReceived() throws VegaException
    {
        // Subscribe to a topic
        final ReceiverListener listener = new ReceiverListener();
        RECEIVER_MANAGER.subscribeToTopic("itopicMsg", listener);

        // Get the subscribed topic
        final TopicSubscriber topicSubscriber = RECEIVER_MANAGER.getTopicSubscriber("itopicMsg");
        
        // Now simulate that we have information on a topic publisher
        final UUID topicPubId = UUID.randomUUID();
        RECEIVER_MANAGER.getTopicSubAndTopicPubIdRelations().addTopicPubRelation(topicPubId, topicSubscriber);

        // Now simulate we have gotten a new message
        final RcvMessage rcvMessage = new RcvMessage();
        rcvMessage.setTopicPublisherId(topicPubId);
        rcvMessage.setInstanceId(VEGA_CONTEXT.getInstanceUniqueId());

        RECEIVER_MANAGER.onDataMsgReceived(rcvMessage);

        // It should be received by our listener and the received message should have a topic name
        Assert.assertEquals(listener.receivedMsg.getTopicName(), topicSubscriber.getTopicName());
        listener.reset();

        // Call the encrypted version, since the topic is not encrypted it should not work
        RECEIVER_MANAGER.onEncryptedDataMsgReceived(rcvMessage);
        assertNull(listener.receivedMsg);

        // If we try with another topic id it should not work
        rcvMessage.setTopicPublisherId(UUID.randomUUID());

        RECEIVER_MANAGER.onDataMsgReceived(rcvMessage);
        assertNull(listener.receivedMsg);
    }

    @Test
    public void onNonSecureMessageReceivedInSecuredSub() throws VegaException
    {
        // Subscribe to a topic
        final ReceiverListener listener = new ReceiverListener();
        RECEIVER_MANAGER.subscribeToTopic("stopicMsg", listener);

        // Get the subscribed topic
        final TopicSubscriber topicSubscriber = RECEIVER_MANAGER.getTopicSubscriber("stopicMsg");

        // Now simulate that we have information on a topic publisher
        final UUID topicPubId = UUID.randomUUID();
        RECEIVER_MANAGER.getTopicSubAndTopicPubIdRelations().addTopicPubRelation(topicPubId, topicSubscriber);

        // Now simulate we have gotten a new message
        final RcvMessage rcvMessage = new RcvMessage();
        rcvMessage.setTopicPublisherId(topicPubId);
        rcvMessage.setInstanceId(VEGA_CONTEXT.getInstanceUniqueId());

        // It should not arrive
        RECEIVER_MANAGER.onDataMsgReceived(rcvMessage);
        assertNull(listener.receivedMsg);

        RECEIVER_MANAGER.unsubscribeFromTopic("stopicMsg");
    }

    @Test
    public void onEncryptedDataMessageReceived() throws VegaException
    {
        // Subscribe to a topic
        final ReceiverListener listener = new ReceiverListener();
        RECEIVER_MANAGER.subscribeToTopic("stopicMsg", listener);

        // Get the subscribed topic
        final TopicSubscriber topicSubscriber = RECEIVER_MANAGER.getTopicSubscriber("stopicMsg");

        // Now simulate that we have information on a topic publisher
        final UUID topicPubId = UUID.randomUUID();
        RECEIVER_MANAGER.getTopicSubAndTopicPubIdRelations().addTopicPubRelation(topicPubId, topicSubscriber);

        // Now simulate we have gotten a new message
        final RcvMessage rcvMessage = new RcvMessage();
        rcvMessage.setTopicPublisherId(topicPubId);
        rcvMessage.setInstanceId(VEGA_CONTEXT.getInstanceUniqueId());

        RECEIVER_MANAGER.onEncryptedDataMsgReceived(rcvMessage);

        // It should not work because we don't have the AES decoder for the publisher
        assertNull(listener.receivedMsg);

        RECEIVER_MANAGER.unsubscribeFromTopic("stopicMsg");
    }

    @Test
    public void onHeartbeatRequestReceived() throws VegaException
    {
        // Subscribe to a topic
        final ReceiverListener listener = new ReceiverListener();
        RECEIVER_MANAGER.subscribeToTopic("itopicMsg3", listener);

        // Get the subscribed topic
        final TopicSubscriber topicSubscriber = RECEIVER_MANAGER.getTopicSubscriber("itopicMsg3");

        // Now simulate that we have information on a topic publisher
        final UUID topicPubId = UUID.randomUUID();
        RECEIVER_MANAGER.getTopicSubAndTopicPubIdRelations().addTopicPubRelation(topicPubId, topicSubscriber);

        // It wont sent any response, since there is no response socket for this
        MsgReqHeader heartbeatReqMsgHeader = new MsgReqHeader();
        heartbeatReqMsgHeader.setInstanceId(UUID.randomUUID());
        heartbeatReqMsgHeader.setRequestId(UUID.randomUUID());
        heartbeatReqMsgHeader.setTopicPublisherId(topicPubId);

        //The first heartbeat initialize the TopicSubscriber.expectedSeqNumByTopicPubId with a sequence number 0
        RECEIVER_MANAGER.onHeartbeatRequestMsgReceived(heartbeatReqMsgHeader);

        // assert that there are not gaps
        assertNull(listener.lostReport);

        //The second heartbeat increases the TopicSubscriber.expectedSeqNumByTopicPubId to a sequence number 1
        heartbeatReqMsgHeader.setSequenceNumber(1);
        RECEIVER_MANAGER.onHeartbeatRequestMsgReceived(heartbeatReqMsgHeader);

        // assert that there are not gaps
        assertNull(listener.lostReport);
    }

    @Test
    public void onHeartbeatRequestReceivedWithLoss() throws VegaException
    {
        // Subscribe to a topic
        final ReceiverListener listener = new ReceiverListener();
        RECEIVER_MANAGER.subscribeToTopic("itopicMsg4", listener);

        // Get the subscribed topic
        final TopicSubscriber topicSubscriber = RECEIVER_MANAGER.getTopicSubscriber("itopicMsg4");

        // Now simulate that we have information on a topic publisher
        final UUID topicPubId = UUID.randomUUID();
        RECEIVER_MANAGER.getTopicSubAndTopicPubIdRelations().addTopicPubRelation(topicPubId, topicSubscriber);

        // It wont sent any response, since there is no response socket for this
        MsgReqHeader heartbeatReqMsgHeader = new MsgReqHeader();
        heartbeatReqMsgHeader.setInstanceId(UUID.randomUUID());
        heartbeatReqMsgHeader.setRequestId(UUID.randomUUID());
        heartbeatReqMsgHeader.setTopicPublisherId(topicPubId);

        //Initialize the TopicSubscriber.expectedSeqNumByTopicPubId with a new message sequence number
        RECEIVER_MANAGER.onHeartbeatRequestMsgReceived(heartbeatReqMsgHeader);

        // assert that there are not gaps
        assertNull(listener.lostReport);

        //The second heartbeat simulates a GAP with a sequence number of 2 (one message lost)
        heartbeatReqMsgHeader.setSequenceNumber(2);
        RECEIVER_MANAGER.onHeartbeatRequestMsgReceived(heartbeatReqMsgHeader);

        // Must exist an error with one message lost
        assertNotNull(listener.lostReport);
        assertEquals(1, listener.lostReport.getNumberLostMessages());

        //The second heartbeat simulates a GAP of 10 messages lost
        heartbeatReqMsgHeader.setSequenceNumber(13);
        RECEIVER_MANAGER.onHeartbeatRequestMsgReceived(heartbeatReqMsgHeader);

        // Must exist an error with one message lost
        assertNotNull(listener.lostReport);
        assertEquals(10, listener.lostReport.getNumberLostMessages());
    }

    @Test
    public void onHeartbeatRequestReceivedWithPatterListenerLoss() throws VegaException
    {
        // topic name & patterns
        String topicName = "itopicPatterMsg";
        String topicPattern1 = "itopicPattern*";
        String topicPattern2 = "itopicPatternMsg*";

        // Subscribe to topic and pattern the listeners
        final ReceiverListener listener0 = new ReceiverListener();
        RECEIVER_MANAGER.subscribeToTopic(topicName, listener0);
        final ReceiverListener listener1 = new ReceiverListener();
        RECEIVER_MANAGER.subscribeToPattern(topicPattern1, listener1);
        final ReceiverListener listener2 = new ReceiverListener();
        RECEIVER_MANAGER.subscribeToPattern(topicPattern2, listener2);

        // Get the subscribed topic and add the patterns
        final TopicSubscriber topicSubscriber0 = RECEIVER_MANAGER.getTopicSubscriber(topicName);
        topicSubscriber0.addPatternListener(topicPattern1, listener1);
        topicSubscriber0.addPatternListener(topicPattern2, listener2);

        // Now simulate that we have information on a topic publisher
        final UUID topicPubId = UUID.randomUUID();
        RECEIVER_MANAGER.getTopicSubAndTopicPubIdRelations().addTopicPubRelation(topicPubId, topicSubscriber0);

        // It wont sent any response, since there is no response socket for this
        MsgReqHeader heartbeatReqMsgHeader = new MsgReqHeader();
        heartbeatReqMsgHeader.setInstanceId(UUID.randomUUID());
        heartbeatReqMsgHeader.setRequestId(UUID.randomUUID());
        heartbeatReqMsgHeader.setTopicPublisherId(topicPubId);

        //Initialize the TopicSubscriber.expectedSeqNumByTopicPubId with a new message sequence number
        RECEIVER_MANAGER.onHeartbeatRequestMsgReceived(heartbeatReqMsgHeader);

        // assert that there are not gaps in the listeners
        assertNull(listener0.lostReport);
        assertNull(listener1.lostReport);
        assertNull(listener2.lostReport);

        //The second heartbeat simulates a GAP with a sequence number of 2 (one message lost)
        heartbeatReqMsgHeader.setSequenceNumber(2);
        RECEIVER_MANAGER.onHeartbeatRequestMsgReceived(heartbeatReqMsgHeader);

        // Must exist an error with one message lost in all the listeners
        assertNotNull(listener0.lostReport);
        assertNotNull(listener1.lostReport);
        assertNotNull(listener2.lostReport);
        assertEquals(1, listener0.lostReport.getNumberLostMessages());
        assertEquals(1, listener1.lostReport.getNumberLostMessages());
        assertEquals(1, listener2.lostReport.getNumberLostMessages());

        //The second heartbeat simulates a GAP of 10 messages lost
        heartbeatReqMsgHeader.setSequenceNumber(13);
        RECEIVER_MANAGER.onHeartbeatRequestMsgReceived(heartbeatReqMsgHeader);

        // Must exist an error with then message lost
        assertNotNull(listener0.lostReport);
        assertNotNull(listener1.lostReport);
        assertNotNull(listener2.lostReport);
        assertEquals(10, listener0.lostReport.getNumberLostMessages());
        assertEquals(10, listener1.lostReport.getNumberLostMessages());
        assertEquals(10, listener2.lostReport.getNumberLostMessages());
    }

    @Test
    public void onDataRequestReceived() throws VegaException
    {
        // Subscribe to a topic
        final ReceiverListener listener = new ReceiverListener();
        RECEIVER_MANAGER.subscribeToTopic("itopicMsg2", listener);

        // Get the subscribed topic
        final TopicSubscriber topicSubscriber = RECEIVER_MANAGER.getTopicSubscriber("itopicMsg2");

        // Create a topic publisher id
        final UUID topicPubId = UUID.randomUUID();

        // Now simulate we have gotten a new message
        final UUID remoteRequesterInstanceId = UUID.randomUUID();
        final RcvRequest rcvRequest = new RcvRequest();
        rcvRequest.setTopicPublisherId(topicPubId);
        rcvRequest.setRequestId(UUID.randomUUID());
        rcvRequest.setInstanceId(remoteRequesterInstanceId);

        // Send the request it should not arrive, there is no response publisher registered
        RECEIVER_MANAGER.onDataRequestMsgReceived(rcvRequest);
        assertNull(listener.receivedReq);

        // Now add the response publisher info!
        RECEIVER_MANAGER.onNewAutoDiscInstanceInfo(new AutoDiscInstanceInfo("intanceName", remoteRequesterInstanceId, IP_ADDRESS, 25555, 8, IP_ADDRESS, 25555, 9));

        // Send the request again, it should still not arrive because it cannot find the topic name
        RECEIVER_MANAGER.onDataRequestMsgReceived(rcvRequest);
        assertNull(listener.receivedReq);

        // Now simulate that we have information on a topic publisher
        RECEIVER_MANAGER.getTopicSubAndTopicPubIdRelations().addTopicPubRelation(topicPubId, topicSubscriber);

        // Send the request again, now it should finally work!
        RECEIVER_MANAGER.onDataRequestMsgReceived(rcvRequest);
        assertNotNull(listener.receivedReq);
        Assert.assertEquals(listener.receivedReq.getTopicName(), topicSubscriber.getTopicName());

        // Send a response
        Assert.assertEquals(listener.receivedReq.sendResponse(new UnsafeBuffer(ByteBuffer.allocate(128)), 0, 128), PublishResult.OK);
    }

    @Test
    public void onDataResponseReceived() throws VegaException
    {
        final RcvResponse rcvResponse = new RcvResponse();
        rcvResponse.setOriginalRequestId(UUID.randomUUID());
        RECEIVER_MANAGER.onDataResponseMsgReceived(rcvResponse);
    }

    static class ReceiverListener implements ITopicSubListener
    {
        volatile IRcvMessage receivedMsg = null;
        volatile IRcvRequest receivedReq = null;
        volatile IMsgLostReport lostReport = null;

        private void reset()
        {
            this.receivedMsg = null;
        }

        @Override
        public void onMessageReceived(final IRcvMessage receivedMessage)
        {
            this.receivedMsg = receivedMessage;
        }

        @Override
        public void onRequestReceived(IRcvRequest receivedRequest)
        {
            this.receivedReq = receivedRequest;
        }

        @Override
        public void onMessageLost(IMsgLostReport lostReport)
        {
            this.lostReport = lostReport;
        }
    }
}