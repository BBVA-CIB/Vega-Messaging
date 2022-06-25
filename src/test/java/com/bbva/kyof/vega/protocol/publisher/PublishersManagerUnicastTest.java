package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.config.general.GlobalConfiguration;
import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.config.general.TransportMediaType;
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
public class PublishersManagerUnicastTest
{
    private static final String KEYS_DIR = SecurityParamsTest.class.getClassLoader().getResource("keys").getPath();

    private static final long SEND_POLL_WAIT = 500;
    public static final int NEW_EVENT_WAIT_TIME = 500;
    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static SubnetAddress SUBNET_ADDRESS;
    private static VegaContext VEGA_CONTEXT;
    private static AutoDiscManagerMock AUTO_DISC_MANAGER_MOCK;

    private PublishersManagerUnicast publisherManager;
    private OwnSecPubTopicsChangesListener secureChangesListener;

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
        publisherManager = new PublishersManagerUnicast(VEGA_CONTEXT, secureChangesListener);
    }

    @After
    public void after()
    {
        publisherManager.close();
    }

    @Test
    public void testCreateRemoveSend() throws Exception
    {
        final UUID instanceId = UUID.randomUUID();

        final String ipAddress = SUBNET_ADDRESS.getIpAddres().getHostAddress();
        final String hostname = SUBNET_ADDRESS.getIpAddres().getHostName();
        final int intAddress = InetUtil.convertIpAddressToInt(ipAddress);

        // Create the topic configuration
        final TopicTemplateConfig templateMcast = TopicTemplateConfig.builder().
                name("template1").
                transportType(TransportMediaType.UNICAST).
                numStreamsPerPort(2).
                minPort(28300).
                maxPort(28302).
                hostname("").
                isResolveHostname(false).
                subnetAddress(SUBNET_ADDRESS).build();

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

        // There should be no topic sockets, there are no events of anyone receiving on the other end
        Assert.assertEquals(0, AUTO_DISC_MANAGER_MOCK.getRegTopicSocketInfos().size());

        // Create several receivers and their corresponding "TopicSocketInfos"
        SimpleReceiver simpleReceiver1 = new SimpleReceiver(AERON, TransportMediaType.UNICAST, ipAddress, 28300, 2, SUBNET_ADDRESS);
        SimpleReceiver simpleReceiver2 = new SimpleReceiver(AERON, TransportMediaType.UNICAST, ipAddress, 28301, 2, SUBNET_ADDRESS);
        SimpleReceiver simpleReceiver3 = new SimpleReceiver(AERON, TransportMediaType.UNICAST, ipAddress, 28301, 3, SUBNET_ADDRESS);

        // The corresponding topic socket infos, the info 4 is repeated but since is other topic it should reuse the existing Socket
        final AutoDiscTopicSocketInfo topicSocketInfo1 = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.SUB_UNI, UUID.randomUUID(), "topic1", UUID.randomUUID(), intAddress, 28300, 2,
                hostname);
        final AutoDiscTopicSocketInfo topicSocketInfo2 = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.SUB_UNI, UUID.randomUUID(), "topic2", UUID.randomUUID(), intAddress, 28301, 2, hostname);
        final AutoDiscTopicSocketInfo topicSocketInfo3 = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.SUB_UNI, UUID.randomUUID(), "topic3", UUID.randomUUID(), intAddress, 28301, 3, hostname);
        final AutoDiscTopicSocketInfo topicSocketInfo4 = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.SUB_UNI, UUID.randomUUID(), "topic4", UUID.randomUUID(), intAddress, 28301, 3, hostname);
        final AutoDiscTopicSocketInfo topicSocketInfo5 = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.SUB_UNI, UUID.randomUUID(), "topic4", UUID.randomUUID(), intAddress, 28301, 3, hostname);

        // Try to add a secure topic socket, it should fail because the publisher is not secured
        final AutoDiscTopicSocketInfo securedTopicSocket = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.SUB_UNI, UUID.randomUUID(), "topic4", UUID.randomUUID(), intAddress, 28301,
                3, hostname, 33);
        this.publisherManager.onNewAutoDiscTopicSocketInfo(securedTopicSocket);
        Thread.sleep(NEW_EVENT_WAIT_TIME);
        this.sendMessageAndCheckArrival(topicPublisher, simpleReceiver1, false);

        // Tell the publisher about the existing receiver, wait for connection to be ready and send a message, should arrive to the receiver 1
        this.publisherManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo1);
        Thread.sleep(NEW_EVENT_WAIT_TIME);
        this.sendMessageAndCheckArrival(topicPublisher, simpleReceiver1, true);

        // Repeat for next receiver
        this.publisherManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo2);
        Thread.sleep(NEW_EVENT_WAIT_TIME);
        this.sendMessageAndCheckArrival(topicPublisher2, simpleReceiver2, true);

        // Repeat for next receiver
        this.publisherManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo3);
        Thread.sleep(NEW_EVENT_WAIT_TIME);
        this.sendMessageAndCheckArrival(topicPublisher3, simpleReceiver3, true);

        // Now with the topic socket that repeats information
        this.publisherManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo4);
        Thread.sleep(NEW_EVENT_WAIT_TIME);
        this.sendMessageAndCheckArrival(topicPublisher4, simpleReceiver3, true);

        // Now with the topic socket that repeats information on the same topic!
        this.publisherManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo5);
        Thread.sleep(NEW_EVENT_WAIT_TIME);
        this.sendMessageAndCheckArrival(topicPublisher4, simpleReceiver3, true);

        // ----------------------------------------------------------------
        // Now simulate time outs
        // ----------------------------------------------------------------
        this.publisherManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo1);
        Thread.sleep(NEW_EVENT_WAIT_TIME);
        this.sendMessageAndCheckArrival(topicPublisher, simpleReceiver1, false);

        // Repeat for next receiver
        this.publisherManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo2);
        Thread.sleep(NEW_EVENT_WAIT_TIME);
        this.sendMessageAndCheckArrival(topicPublisher2, simpleReceiver2, false);

        // Repeat for next receiver, this time since there is other socket we can still try to send on the other topic
        this.publisherManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo3);
        Thread.sleep(NEW_EVENT_WAIT_TIME);
        this.sendMessageAndCheckArrival(topicPublisher3, simpleReceiver3, false);
        this.sendMessageAndCheckArrival(topicPublisher4, simpleReceiver3, true);

        // Now with the topic socket that repeats information
        this.publisherManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo4);
        Thread.sleep(NEW_EVENT_WAIT_TIME);
        this.sendMessageAndCheckArrival(topicPublisher4, simpleReceiver3, true);

        this.publisherManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo5);
        Thread.sleep(NEW_EVENT_WAIT_TIME);
        this.sendMessageAndCheckArrival(topicPublisher4, simpleReceiver3, false);

        // -----------------------------------------------------------------------
        //       EDGE CASES
        // -----------------------------------------------------------------------

        // Add topic socket that dont correspond to any topic publisher
        this.publisherManager.onNewAutoDiscTopicSocketInfo(new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.SUB_UNI, UUID.randomUUID(), "topic14", UUID.randomUUID(), 23, 343, 2323, hostname));

        // Add topic topic socket twice
        this.publisherManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo1);
        this.publisherManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo1);

        // Remove a topic socket on non existing topic
        this.publisherManager.onTimedOutAutoDiscTopicSocketInfo(new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.SUB_UNI, UUID.randomUUID(), "topic14", UUID.randomUUID(), 23, 343, 2323
                , hostname));

        // Remove topic socket on existing topic but not exisint topic socket id
        this.publisherManager.onTimedOutAutoDiscTopicSocketInfo(new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.SUB_UNI, UUID.randomUUID(), "topic1", UUID.randomUUID(), 23, 343, 2323, hostname));

        // Non used events
        this.publisherManager.onNewAutoDiscTopicInfo(null);
        this.publisherManager.onTimedOutAutoDiscTopicInfo(null);

        // Destroy topic publisher with and without removing the socket
        this.publisherManager.onNewAutoDiscTopicInfo(topicInfo3);
        this.publisherManager.onNewAutoDiscTopicInfo(topicInfo4);

        this.publisherManager.destroyTopicPublisher("topic1");
        this.publisherManager.destroyTopicPublisher("topic4");

        // Finally close
        this.publisherManager.close();

        // Add and remove on closed
        this.publisherManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo3);
        this.publisherManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo3);
    }

    @Test
    public void testSecureCreateRemoveSend() throws Exception
    {
        final UUID instanceId = UUID.randomUUID();

        final String ipAddress = SUBNET_ADDRESS.getIpAddres().getHostAddress();
        final String hostname = SUBNET_ADDRESS.getIpAddres().getHostName();
        final int intAddress = InetUtil.convertIpAddressToInt(ipAddress);

        // Create the topic configuration
        final TopicTemplateConfig templateUcast = TopicTemplateConfig.builder().
                name("template1").
                transportType(TransportMediaType.UNICAST).
                numStreamsPerPort(2).
                minPort(28300).
                maxPort(28302).
                subnetAddress(SUBNET_ADDRESS).
                hostname("").isResolveHostname(false).
                build();

        // Create topic publisher
        final Set<Integer> secureSubs = new HashSet<>(Collections.singletonList(22222));
        final Set<Integer> securePubs = new HashSet<>(Collections.singletonList(11111));
        final TopicSecurityTemplateConfig securityTemplateConfig = new TopicSecurityTemplateConfig("topic1", 100L, securePubs, secureSubs);
        final ITopicPublisher topicPublisher = publisherManager.createTopicPublisher("topic1", templateUcast, securityTemplateConfig);
        final AESCrypto aesCrypto = new AESCrypto(((SecureTopicPublisherUnicast) topicPublisher).getSessionKey());

        // Create receiver and their corresponding "TopicSocketInfs"
        SimpleReceiver simpleReceiver1 = new SimpleReceiver(AERON, TransportMediaType.UNICAST, ipAddress, 28300, 2, SUBNET_ADDRESS);

        // Create a topic socket info, the secure id is not allowed, it should fail
        final AutoDiscTopicSocketInfo topicSocketInfo1 = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.SUB_UNI, UUID.randomUUID(), "topic1", UUID.randomUUID(), intAddress, 28300, 2,
                hostname, 11111);
        this.publisherManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo1);
        Thread.sleep(NEW_EVENT_WAIT_TIME);
        this.sendSecureMessageAndCheckArrival(topicPublisher, aesCrypto, simpleReceiver1, false);

        // Create a topic socket info with no security, it should fail
        final AutoDiscTopicSocketInfo topicSocketInfoNonSecured = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.SUB_UNI, UUID.randomUUID(), "topic1", UUID.randomUUID(), intAddress,
                28300, 2, hostname, 0);
        this.publisherManager.onNewAutoDiscTopicSocketInfo(topicSocketInfoNonSecured);
        Thread.sleep(NEW_EVENT_WAIT_TIME);
        this.sendSecureMessageAndCheckArrival(topicPublisher, aesCrypto, simpleReceiver1, false);

        // It should not have registered a new publisher either
        Assert.assertFalse(this.secureChangesListener.containsPubId(topicSocketInfo1.getUniqueId()));

        // Create a topic socket info, the secure id is allowed, it should work
        final AutoDiscTopicSocketInfo topicSocketInfo2 = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.SUB_UNI, UUID.randomUUID(), "topic1", UUID.randomUUID(), intAddress, 28300, 2,
                hostname, 22222);
        this.publisherManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo2);
        Thread.sleep(NEW_EVENT_WAIT_TIME);
        this.sendSecureMessageAndCheckArrival(topicPublisher, aesCrypto, simpleReceiver1, true);

        // It should have registered a new publisher either
        Assert.assertTrue(this.secureChangesListener.containsPubId(topicPublisher.getUniqueId()));

        // Finally close
        this.publisherManager.close();

        Assert.assertFalse(this.secureChangesListener.containsPubId(topicPublisher.getUniqueId()));

        // Add and remove on closed
        this.publisherManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo2);
        this.publisherManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo2);
    }

    private void sendMessageAndCheckArrival(ITopicPublisher topicPublisher, SimpleReceiver simpleReceiver, boolean shouldArrive) throws InterruptedException
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(128));
        buffer.putInt(0, 128);

        topicPublisher.sendMsg(buffer, 0, 4);
        Thread.sleep(SEND_POLL_WAIT);
        simpleReceiver.pollReceivedMessage();

        if (shouldArrive)
        {
            Assert.assertTrue((simpleReceiver.getReusableReceivedMsg().getTopicPublisherId() != null));
        }
        else
        {
            Assert.assertTrue((simpleReceiver.getReusableReceivedMsg().getTopicPublisherId() == null));
        }

        simpleReceiver.reset();
    }

    private void sendSecureMessageAndCheckArrival(ITopicPublisher topicPublisher, AESCrypto decoder, SimpleReceiver simpleReceiver, boolean shouldArrive) throws Exception
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(128));
        buffer.putInt(0, 128);

        topicPublisher.sendMsg(buffer, 0, 4);
        Thread.sleep(SEND_POLL_WAIT);
        simpleReceiver.pollReceivedMessage();

        final IRcvMessage rcvMessage = simpleReceiver.getReusableReceivedMsg();

        if (!shouldArrive)
        {
            Assert.assertNull(rcvMessage.getContents());
            return;
        }
        else
        {
            Assert.assertNotNull(rcvMessage.getContents());
        }

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