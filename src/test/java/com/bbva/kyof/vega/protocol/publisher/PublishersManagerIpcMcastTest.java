package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.config.general.GlobalConfiguration;
import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.msg.IRcvMessage;
import com.bbva.kyof.vega.protocol.AutoDiscManagerMock;
import com.bbva.kyof.vega.protocol.common.KeySecurityType;
import com.bbva.kyof.vega.protocol.common.SecurityParams;
import com.bbva.kyof.vega.protocol.common.SecurityParamsTest;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.control.IOwnSecPubTopicsChangesListener;
import com.bbva.kyof.vega.util.crypto.AESCrypto;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Created by cnebrera on 11/08/16.
 */
public class PublishersManagerIpcMcastTest
{
    private static final String KEYS_DIR = SecurityParamsTest.class.getClassLoader().getResource("keys").getPath();

    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static SubnetAddress SUBNET_ADDRESS;
    private static VegaContext VEGA_CONTEXT;
    private static AutoDiscManagerMock AUTO_DISC_MANAGER_MOCK;
    private static OwnSecPubTopicsChangesListener secureChangesListener;

    private PublishersManagerIpcMcast publisherManager;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx1 = new Aeron.Context();
        ctx1.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON = Aeron.connect(ctx1);

        SUBNET_ADDRESS = InetUtil.getDefaultSubnet();

        // Create the set with the secure topic ids
        final Set<Integer> secureTopicIds = new HashSet<>();
        secureTopicIds.add(11111);
        secureTopicIds.add(22222);

        final GlobalConfiguration globalConfiguration = EasyMock.createNiceMock(GlobalConfiguration.class);
        EasyMock.expect(globalConfiguration.getAllSecureTopicsSecurityIds()).andReturn(secureTopicIds).anyTimes();
        EasyMock.replay(globalConfiguration);

        VEGA_CONTEXT = new VegaContext(AERON, globalConfiguration);

        final SecurityParams plainParams = SecurityParams.builder().
                keySecurityType(KeySecurityType.PLAIN_KEY_FILE).
                securityId(11111).
                privateKeyDirPath(KEYS_DIR).
                publicKeysDirPath(KEYS_DIR).build();

        VEGA_CONTEXT.initializeSecurity(plainParams);

        // Mock auto-discovery manager calls
        AUTO_DISC_MANAGER_MOCK = new AutoDiscManagerMock();
        VEGA_CONTEXT.setAutodiscoveryManager(AUTO_DISC_MANAGER_MOCK.getMock());

        // Give it time to start
        Thread.sleep(1000);
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Before
    public void before()
    {
        secureChangesListener = new OwnSecPubTopicsChangesListener();
        publisherManager = new PublishersManagerIpcMcast(VEGA_CONTEXT, secureChangesListener);
    }

    @After
    public void after()
    {
        publisherManager.close();
    }

    @Test
    public void testAutodiscEvents() throws VegaException
    {
        this.publisherManager.onNewAutoDiscTopicInfo(null);
        this.publisherManager.onNewAutoDiscTopicSocketInfo(null);
        this.publisherManager.onTimedOutAutoDiscTopicInfo(null);
        this.publisherManager.onTimedOutAutoDiscTopicSocketInfo(null);
    }

    @Test
    public void testCreateIpcRemove() throws VegaException
    {
        final UUID instanceId = UUID.randomUUID();

        // Create the topic configuration
        final TopicTemplateConfig templateIpc = TopicTemplateConfig.builder().transportType(TransportMediaType.IPC).numStreamsPerPort(1).build();

        // Create several topic publishers
        final ITopicPublisher topicPublisher = publisherManager.createTopicPublisher("topic1", templateIpc, null);

        final AutoDiscTopicInfo topicInfo1 = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, topicPublisher.getUniqueId(), "topic1");
        Assert.assertTrue(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().contains(topicInfo1));

        Assert.assertEquals(1, AUTO_DISC_MANAGER_MOCK.getRegTopicSocketInfos().size());
    }

    @Test
    public void testCreateRemove() throws VegaException
    {
        final UUID instanceId = UUID.randomUUID();

        // Create the topic configuration
        final TopicTemplateConfig templateMcast = new TopicTemplateConfig(
                "template1",
                TransportMediaType.MULTICAST,
                null,
                28000,
                28000,
                2,
                "224.1.1.1",
                "224.1.1.2",
                SUBNET_ADDRESS.toString(),
                SUBNET_ADDRESS,
                null);

        // Create several topic publishers
        final ITopicPublisher topicPublisher = publisherManager.createTopicPublisher("topic1", templateMcast, null);
        final ITopicPublisher topicPublisher2 = publisherManager.createTopicPublisher("topic2", templateMcast, null);
        final ITopicPublisher topicPublisher3 = publisherManager.createTopicPublisher("topic3", templateMcast, null);
        final ITopicPublisher topicPublisher4 = publisherManager.createTopicPublisher("topic4", templateMcast, null);

        // These are the topic info for each created publisher
        final AutoDiscTopicInfo topicInfo1 = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_MUL, topicPublisher.getUniqueId(), "topic1");
        final AutoDiscTopicInfo topicInfo2 = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_MUL, topicPublisher2.getUniqueId(), "topic2");
        final AutoDiscTopicInfo topicInfo3 = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_MUL, topicPublisher3.getUniqueId(), "topic3");
        final AutoDiscTopicInfo topicInfo4 = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_MUL, topicPublisher4.getUniqueId(), "topic4");

        // 4 new auto-discovery topic info should have been registered in auto-discovery
        Assert.assertTrue(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().contains(topicInfo1));
        Assert.assertTrue(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().contains(topicInfo2));
        Assert.assertTrue(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().contains(topicInfo3));
        Assert.assertTrue(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().contains(topicInfo4));

        // There should be 4 topic sockets, one per topic publisher but only 2 actual publishers are created
        Assert.assertEquals(4, AUTO_DISC_MANAGER_MOCK.getRegTopicSocketInfos().size());

        // Review them
        AUTO_DISC_MANAGER_MOCK.getRegTopicSocketInfos().forEach((topicSocket) ->
        {
            Assert.assertEquals(28000, topicSocket.getPort());
            Assert.assertSame(topicSocket.getTransportType(), AutoDiscTransportType.PUB_MUL);
            Assert.assertTrue(topicSocket.getStreamId() == 2 || topicSocket.getStreamId() == 3);
            Assert.assertTrue(topicSocket.getTopicName().startsWith("topic"));
        });

        // Now destroy them one by one...
        publisherManager.destroyTopicPublisher("topic1");
        Assert.assertEquals(3, AUTO_DISC_MANAGER_MOCK.getRegTopicSocketInfos().size());
        Assert.assertFalse(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().contains(topicInfo1));

        publisherManager.destroyTopicPublisher("topic2");
        Assert.assertEquals(2, AUTO_DISC_MANAGER_MOCK.getRegTopicSocketInfos().size());
        Assert.assertFalse(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().contains(topicInfo2));

        publisherManager.destroyTopicPublisher("topic3");
        Assert.assertEquals(1, AUTO_DISC_MANAGER_MOCK.getRegTopicSocketInfos().size());
        Assert.assertFalse(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().contains(topicInfo3));

        publisherManager.destroyTopicPublisher("topic4");
        Assert.assertEquals(0, AUTO_DISC_MANAGER_MOCK.getRegTopicSocketInfos().size());
        Assert.assertFalse(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().contains(topicInfo4));
    }

    @Test
    public void testReceive() throws Exception
    {
        // Create the topic configuration
        final TopicTemplateConfig templateMcast = new TopicTemplateConfig(
                "template1",
                TransportMediaType.MULTICAST,
                null,
                28033,
                28033,
                2,
                "224.4.1.1",
                "224.4.1.2",
                SUBNET_ADDRESS.toString(),
                SUBNET_ADDRESS,
                null);

        // Create several topic publishers
        final ITopicPublisher topicPublisher = publisherManager.createTopicPublisher("topic1", templateMcast, null);
        final ITopicPublisher topicPublisher2 = publisherManager.createTopicPublisher("topic2", templateMcast, null);
        final ITopicPublisher topicPublisher3 = publisherManager.createTopicPublisher("topic3", templateMcast, null);
        final ITopicPublisher topicPublisher4 = publisherManager.createTopicPublisher("topic4", templateMcast, null);

        // The id should not be in the secure change listener
        Assert.assertFalse(secureChangesListener.containsPubId(topicPublisher.getUniqueId()));
        Assert.assertFalse(secureChangesListener.containsPubId(topicPublisher2.getUniqueId()));
        Assert.assertFalse(secureChangesListener.containsPubId(topicPublisher3.getUniqueId()));
        Assert.assertFalse(secureChangesListener.containsPubId(topicPublisher4.getUniqueId()));

        // There should be maximum 2 sockets if we consider the parameters
        SimpleReceiver simpleReceiver1 = new SimpleReceiver(AERON, TransportMediaType.MULTICAST, "224.4.1.1", 28033, 2, SUBNET_ADDRESS);
        SimpleReceiver simpleReceiver2 = new SimpleReceiver(AERON, TransportMediaType.MULTICAST, "224.4.1.1", 28033, 3, SUBNET_ADDRESS);

        // Give time to the connections to stabilise
        Thread.sleep(1000);

        // Now send messages in all topic publishers, they should all arrive at least in one of the receivers!
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(128));
        this.sendMessagesFromTopicPublisher(topicPublisher, simpleReceiver1, simpleReceiver2, buffer);
        this.checkMsgReception(topicPublisher, simpleReceiver1, simpleReceiver2);
        this.sendMessagesFromTopicPublisher(topicPublisher2, simpleReceiver1, simpleReceiver2, buffer);
        this.checkMsgReception(topicPublisher2, simpleReceiver1, simpleReceiver2);
        this.sendMessagesFromTopicPublisher(topicPublisher3, simpleReceiver1, simpleReceiver2, buffer);
        this.checkMsgReception(topicPublisher3, simpleReceiver1, simpleReceiver2);
        this.sendMessagesFromTopicPublisher(topicPublisher4, simpleReceiver1, simpleReceiver2, buffer);
        this.checkMsgReception(topicPublisher4, simpleReceiver1, simpleReceiver2);

        // Now disconnect all topic publishers but one, there should be only on socket left open
        publisherManager.destroyTopicPublisher("topic1");
        publisherManager.destroyTopicPublisher("topic2");
        publisherManager.destroyTopicPublisher("topic3");

        this.sendMessagesFromTopicPublisher(topicPublisher4, simpleReceiver1, simpleReceiver2, buffer);
        this.checkSingleSocketMsgReception(simpleReceiver1, simpleReceiver2);
    }

    @Test
    public void testCreateSecureAndReceive() throws Exception
    {
        // Create the topic configuration
        final TopicTemplateConfig templateMcast = new TopicTemplateConfig(
                "template1",
                TransportMediaType.MULTICAST,
                null,
                28033,
                28033,
                2,
                "224.4.1.1",
                "224.4.1.2",
                SUBNET_ADDRESS.toString(),
                SUBNET_ADDRESS,
                null);

        // Create a topic publisher
        final Set<Integer> secureSubs = new HashSet<>(Collections.singletonList(22222));
        final Set<Integer> securePubs = new HashSet<>(Collections.singletonList(11111));
        final TopicSecurityTemplateConfig securityTemplateConfig = new TopicSecurityTemplateConfig("topic1", 100L, securePubs, secureSubs);
        final SecureTopicPublisherIpcMcast topicPublisher = (SecureTopicPublisherIpcMcast) publisherManager.createTopicPublisher("topic1", templateMcast, securityTemplateConfig);

        // Create a receiver
        SimpleReceiver simpleReceiver1 = new SimpleReceiver(AERON, TransportMediaType.MULTICAST, "224.4.1.1", 28033, 2, SUBNET_ADDRESS);

        // Give time to the connections to stabilise
        Thread.sleep(1000);

        // Now send messages in all topic publishers, they should all arrive at least in one of the receivers!
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(128));
        this.sendMessagesFromTopicPublisher(topicPublisher, simpleReceiver1, buffer);
        this.checkSecureMsgReception(simpleReceiver1, new AESCrypto(topicPublisher.getSessionKey()));

        // The id should be in the secure change listener
        Assert.assertTrue(secureChangesListener.containsPubId(topicPublisher.getUniqueId()));

        // Now disconnect all topic publishers but one, there should be only on socket left open
        publisherManager.destroyTopicPublisher("topic1");

        // The id should not be in the secure change listener
        Assert.assertFalse(secureChangesListener.containsPubId(topicPublisher.getUniqueId()));
    }

    private void sendMessagesFromTopicPublisher(ITopicPublisher topicPublisher, SimpleReceiver simpleReceiver1, SimpleReceiver simpleReceiver2, UnsafeBuffer buffer) throws InterruptedException
    {
        buffer.putInt(0, 128);

        topicPublisher.sendMsg(buffer, 0, 4);

        Thread.sleep(100);

        simpleReceiver1.pollReceivedMessage();
        simpleReceiver2.pollReceivedMessage();
    }

    private void sendMessagesFromTopicPublisher(ITopicPublisher topicPublisher, SimpleReceiver simpleReceiver, UnsafeBuffer buffer) throws InterruptedException
    {
        buffer.putInt(0, 128);

        topicPublisher.sendMsg(buffer, 0, 4);

        Thread.sleep(100);

        simpleReceiver.pollReceivedMessage();
    }

    private void checkMsgReception(ITopicPublisher topicPublisher, SimpleReceiver simpleReceiver1, SimpleReceiver simpleReceiver2)
    {
        if (simpleReceiver1.getReusableReceivedMsg().getTopicPublisherId() != null)
        {
            Assert.assertEquals(topicPublisher.getUniqueId(), simpleReceiver1.getReusableReceivedMsg().getTopicPublisherId());
        }
        else
        {
            Assert.assertEquals(topicPublisher.getUniqueId(), simpleReceiver2.getReusableReceivedMsg().getTopicPublisherId());
        }

        simpleReceiver1.reset();
        simpleReceiver2.reset();
    }

    private void checkSecureMsgReception(SimpleReceiver simpleReceiver, AESCrypto decoder) throws VegaException
    {
        final IRcvMessage rcvMessage = simpleReceiver.getReusableReceivedMsg();

        // Decode, the result should be the same
        final ByteBuffer encodedMsg = ByteBuffer.allocate(1024);
        final ByteBuffer decodedMsg = ByteBuffer.allocate(1024);

        // First we need the copy the message contents into a byte buffer
        rcvMessage.getContents().getBytes(rcvMessage.getContentOffset(), encodedMsg, rcvMessage.getContentLength());
        encodedMsg.flip();

        decoder.decode(encodedMsg, decodedMsg);
        decodedMsg.flip();

        Assert.assertEquals(new UnsafeBuffer(decodedMsg).getInt(0), 128);

        simpleReceiver.reset();
    }

    private void checkSingleSocketMsgReception(SimpleReceiver simpleReceiver1, SimpleReceiver simpleReceiver2)
    {
        Assert.assertTrue((simpleReceiver1.getReusableReceivedMsg().getTopicPublisherId() != null && simpleReceiver2.getReusableReceivedMsg().getTopicPublisherId() == null) ||
                (simpleReceiver2.getReusableReceivedMsg().getTopicPublisherId() != null && simpleReceiver1.getReusableReceivedMsg().getTopicPublisherId() == null));

        simpleReceiver1.reset();
        simpleReceiver2.reset();
    }

    private static class OwnSecPubTopicsChangesListener implements IOwnSecPubTopicsChangesListener
    {
        private final Set<UUID> secureTopicPubAdded = new HashSet<>();

        @Override
        public void onOwnSecureTopicPublisherAdded(UUID topicPubId, byte[] sessionKey, TopicSecurityTemplateConfig securityConfig)
        {
            secureTopicPubAdded.add(topicPubId);
        }

        @Override
        public void onOwnSecuredTopicPublisherRemoved(UUID topicPubId)
        {
            secureTopicPubAdded.remove(topicPubId);
        }

        public boolean containsPubId(final UUID id)
        {
            return this.secureTopicPubAdded.contains(id);
        }
    }
}