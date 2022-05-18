package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.TestConstants;
import com.bbva.kyof.vega.autodiscovery.daemon.CommandLineParserTest;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.config.general.*;
import com.bbva.kyof.vega.msg.*;
import com.bbva.kyof.vega.protocol.AutoDiscManagerMock;
import com.bbva.kyof.vega.protocol.common.KeySecurityType;
import com.bbva.kyof.vega.protocol.common.SecurityParams;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.control.ISecurityRequesterNotifier;
import com.bbva.kyof.vega.protocol.publisher.AeronPublisher;
import com.bbva.kyof.vega.protocol.publisher.AeronPublisherParams;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.easymock.EasyMock;
import org.junit.*;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by cnebrera on 11/08/16.
 */
public class SubscribersManagerIpcMcastTest implements ITopicSubListener
{
    private static final String KEYS_DIR_PATH = Objects.requireNonNull(CommandLineParserTest.class.getClassLoader().getResource("keys")).getPath();

    private static final String validConfigFile = Objects.requireNonNull(ConfigReaderTest.class.getClassLoader().getResource("config/subscribersManagerTestConfig.xml")).getPath();

    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static SubnetAddress SUBNET_ADDRESS;
    private static VegaContext VEGA_CONTEXT;
    private static SubscribersPollersManager POLLERS_MANAGER;
    private static final ReceiverListener POLLER_LISTENER = new ReceiverListener();
    private static final TopicSubAndTopicPubIdRelations RELATIONS = new TopicSubAndTopicPubIdRelations();

    private SubscribersManagerIpcMcast subscriberManager;

    // Create the topic configuration
    private final TopicTemplateConfig templateMcast = TopicTemplateConfig.builder().
            name("template1").
            transportType(TransportMediaType.MULTICAST).
            numStreamsPerPort(2).
            minPort(28300).
            maxPort(28302).
            rcvPoller("poller1").
            subnetAddress(SUBNET_ADDRESS).build();


    @BeforeClass
    public static void beforeClass() throws Exception
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx1 = new Aeron.Context();
        ctx1.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON = Aeron.connect(ctx1);

        SUBNET_ADDRESS = InetUtil.getDefaultSubnet();
        VEGA_CONTEXT = new VegaContext(AERON, ConfigReader.readConfiguration(validConfigFile));

        // Mock auto-discovery manager calls
        AutoDiscManagerMock AUTO_DISC_MANAGER_MOCK = new AutoDiscManagerMock();
        VEGA_CONTEXT.setAutodiscoveryManager(AUTO_DISC_MANAGER_MOCK.getMock());

        final SecurityParams securityParams = SecurityParams.builder().
                keySecurityType(KeySecurityType.PLAIN_KEY_FILE).
                privateKeyDirPath(KEYS_DIR_PATH).
                publicKeysDirPath(KEYS_DIR_PATH).
                securityId(11111).build();

        // Set in the vega contexts
        VEGA_CONTEXT.initializeSecurity(securityParams);

        // Create the pollers manager
        POLLERS_MANAGER = new SubscribersPollersManager(VEGA_CONTEXT, POLLER_LISTENER);

        // Give it time to start
        Thread.sleep(2000);
    }

    @AfterClass
    public static void afterClass()
    {
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Before
    public void before()
    {
        final ISecurityRequesterNotifier securityRequesterNotifier = EasyMock.createNiceMock(ISecurityRequesterNotifier.class);
        EasyMock.replay(securityRequesterNotifier);
        subscriberManager = new SubscribersManagerIpcMcast(VEGA_CONTEXT, POLLERS_MANAGER, RELATIONS, securityRequesterNotifier);
    }

    @After
    public void after()
    {
        subscriberManager.close();
    }

    @Test
    public void testMultipleTopicSocketsWithSameParameters() throws Exception
    {
        final UUID instanceId = UUID.randomUUID();

        // Subscribe to topic
        subscriberManager.subscribeToTopic("mtopic1", templateMcast, null, this);

        // Now create multiple "AeronPublishers"
        final AeronPublisherParams pubParams1 = new AeronPublisherParams(TransportMediaType.MULTICAST, InetUtil.convertIpAddressToInt("224.1.1.1"), 28300, 2, SUBNET_ADDRESS, TestConstants.EMPTY_HOSTNAME);
        final AeronPublisher publisher1 = new AeronPublisher(VEGA_CONTEXT, pubParams1);

        final AutoDiscTopicSocketInfo topicSocketInfo1 = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "mtopic1", UUID.randomUUID(),
                pubParams1.getIpAddress(), pubParams1.getPort(), pubParams1.getStreamId(),TestConstants.EMPTY_HOSTNAME);
        final AutoDiscTopicSocketInfo topicSocketInfo2 = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "mtopic1", UUID.randomUUID(),
                pubParams1.getIpAddress(), pubParams1.getPort(), pubParams1.getStreamId(), pubParams1.getHostname());
        final AutoDiscTopicSocketInfo secureTopicSocketInfo = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "mtopic1", UUID.randomUUID(),
                pubParams1.getIpAddress(), pubParams1.getPort(), pubParams1.getStreamId(), TestConstants.EMPTY_HOSTNAME ,22);

        // Wait a bit
        Thread.sleep(2000);

        // Tell the subscriber about the secure publisher
        subscriberManager.onNewAutoDiscTopicSocketInfo(secureTopicSocketInfo);

        // Publish, nothing should arrive
        sendMessageAndCheckArrival(publisher1, false);

        // Tell the subscriber about the first publisher
        subscriberManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo1);

        // Wait a bit
        Thread.sleep(1000);

        // Now messages should arrive for the publisher1
        sendMessageAndCheckArrival(publisher1, true);

        // Register other topic socket with same params, it should reuse the created subscriber
        subscriberManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo2);

        // If send with any of them should arrive
        sendMessageAndCheckArrival(publisher1, true);

        // Now time out the first one
        subscriberManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo1);

        sendMessageAndCheckArrival(publisher1, true);

        // Timeout the second one, now it should really destroy the subscriber
        subscriberManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo2);

        sendMessageAndCheckArrival(publisher1, false);

        Thread.sleep(100);

        // Now with topic un-subscription
        subscriberManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo1);
        subscriberManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo2);

        Thread.sleep(1000);

        sendMessageAndCheckArrival(publisher1, true);

        // Unsubscribe from topic
        subscriberManager.unsubscribeFromTopic("mtopic1");

        sendMessageAndCheckArrival(publisher1, false);
    }

    @Test
    public void testMultipleTopicSocketsDifferentParameters() throws Exception
    {
        final UUID instanceId = UUID.randomUUID();

        // Subscribe to topic
        subscriberManager.subscribeToTopic("mtopic1", templateMcast, null, this);
        subscriberManager.subscribeToTopic("mtopic2", templateMcast, null, this);

        // Now create multiple "AeronPublishers"
        final AeronPublisherParams pubParams1 = new AeronPublisherParams(TransportMediaType.MULTICAST, InetUtil.convertIpAddressToInt("224.1.1.1"), 28300, 2, SUBNET_ADDRESS, TestConstants.EMPTY_HOSTNAME);
        final AeronPublisher publisher1 = new AeronPublisher(VEGA_CONTEXT, pubParams1);
        final AeronPublisherParams pubParams2 = new AeronPublisherParams(TransportMediaType.MULTICAST, InetUtil.convertIpAddressToInt("224.1.1.1"), 28300, 4, SUBNET_ADDRESS,
                TestConstants.EMPTY_HOSTNAME);
        final AeronPublisher publisher2 = new AeronPublisher(VEGA_CONTEXT, pubParams2);
        final AeronPublisherParams pubParams3 = new AeronPublisherParams(TransportMediaType.MULTICAST, InetUtil.convertIpAddressToInt("224.1.1.3"), 28300, 2, SUBNET_ADDRESS,TestConstants.EMPTY_HOSTNAME);
        final AeronPublisher publisher3 = new AeronPublisher(VEGA_CONTEXT, pubParams3);

        final AutoDiscTopicSocketInfo topicSocketInfo1 = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "mtopic1", UUID.randomUUID(),
                pubParams1.getIpAddress(), pubParams1.getPort(), pubParams1.getStreamId(), pubParams1.getHostname());
        final AutoDiscTopicSocketInfo topicSocketInfo2 = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "mtopic1", UUID.randomUUID(),
                pubParams2.getIpAddress(), pubParams2.getPort(), pubParams2.getStreamId(), pubParams2.getHostname());
        final AutoDiscTopicSocketInfo topicSocketInfo3 = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "mtopic1", UUID.randomUUID(),
                pubParams3.getIpAddress(), pubParams3.getPort(), pubParams3.getStreamId(), pubParams3.getHostname());
        final AutoDiscTopicSocketInfo topicSocketInfo4 = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "mtopic2", UUID.randomUUID(),
                pubParams3.getIpAddress(), pubParams3.getPort(), pubParams3.getStreamId(), pubParams3.getHostname());

        // Wait a bit
        Thread.sleep(2000);

        // Tell the subscriber about all the publishers
        subscriberManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo1);
        subscriberManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo2);
        subscriberManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo3);
        subscriberManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo4);

        // Wait a bit
        Thread.sleep(1000);

        // Now messages should arrive for the publisher1
        sendMessageAndCheckArrival(publisher1, true);
        sendMessageAndCheckArrival(publisher2, true);
        sendMessageAndCheckArrival(publisher3, true);

        // Start un-registering
        subscriberManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo1);
        sendMessageAndCheckArrival(publisher1, false);
        sendMessageAndCheckArrival(publisher2, true);
        sendMessageAndCheckArrival(publisher3, true);

        // Start un-registering
        subscriberManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo2);
        sendMessageAndCheckArrival(publisher1, false);
        sendMessageAndCheckArrival(publisher2, false);
        sendMessageAndCheckArrival(publisher3, true);

        subscriberManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo3);
        sendMessageAndCheckArrival(publisher1, false);
        sendMessageAndCheckArrival(publisher2, false);
        sendMessageAndCheckArrival(publisher3, true);

        subscriberManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo4);
        sendMessageAndCheckArrival(publisher1, false);
        sendMessageAndCheckArrival(publisher2, false);
        sendMessageAndCheckArrival(publisher3, false);
    }

    @Test
    public void testCallsOnClosed() throws Exception
    {
        final UUID instanceId = UUID.randomUUID();

        // Subscribe to topic
        subscriberManager.subscribeToTopic("mtopic1", templateMcast, null, this);
        subscriberManager.close();

        final AutoDiscTopicSocketInfo topicSocketInfo1 = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "mtopic1", UUID.randomUUID(), 34, 34534, 23,
                TestConstants.EMPTY_HOSTNAME);
        subscriberManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo1);
        subscriberManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo1);
    }

    @Test
    public void testEdgeCases() throws Exception
    {
        final UUID instanceId = UUID.randomUUID();

        // Subscribe to topic
        subscriberManager.subscribeToTopic("mtopic1", templateMcast, null, this);

        // New and timed out topic socket on non subscribed topic
        final AutoDiscTopicSocketInfo topicSocketInfo1 = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "mtopic2", UUID.randomUUID(), 34, 34534, 23, TestConstants.EMPTY_HOSTNAME);
        subscriberManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo1);
        subscriberManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo1);

        // New and timed out non existing topic socket
        final AutoDiscTopicSocketInfo topicSocketInfo2 = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "mtopic1", UUID.randomUUID(), 34, 34534, 23, TestConstants.EMPTY_HOSTNAME);
        subscriberManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo2);
        subscriberManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo2);
        subscriberManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo2);
        subscriberManager.onTimedOutAutoDiscTopicSocketInfo(topicSocketInfo2);
    }

    @Test
    public void testSecure() throws Exception
    {
        final UUID instanceId = UUID.randomUUID();

        // Create security template configuration for the subscriber topic
        final Set<Integer> pubTopic1SecureSubs = new HashSet<>(Collections.singletonList(22222));
        final Set<Integer> pubTopic1SecurePubs = new HashSet<>(Arrays.asList(11111, 22222, 33333));
        final TopicSecurityTemplateConfig securityTemplateConfig = new TopicSecurityTemplateConfig("secureConfig", 1000L, pubTopic1SecurePubs, pubTopic1SecureSubs);

        // Subscribe to topic
        subscriberManager.subscribeToTopic("mtopic1", templateMcast, securityTemplateConfig, this);

        // Now create a publisher
        final AeronPublisherParams pubParams1 = new AeronPublisherParams(TransportMediaType.MULTICAST, InetUtil.convertIpAddressToInt("224.1.1.1"), 28300, 2, SUBNET_ADDRESS,TestConstants.EMPTY_HOSTNAME);
        final AeronPublisher publisher1 = new AeronPublisher(VEGA_CONTEXT, pubParams1);

        // Notify with no security, the messages should not arrive
        AutoDiscTopicSocketInfo topicSocketInfo = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "mtopic1", UUID.randomUUID(), pubParams1.getIpAddress(),
                pubParams1.getPort(), pubParams1.getStreamId(),pubParams1.getHostname());
        subscriberManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo);
        Thread.sleep(1000);
        sendMessageAndCheckArrival(publisher1, false);

        // Now a secured one, but with a wrong secure id
        topicSocketInfo = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "mtopic1", UUID.randomUUID(), pubParams1.getIpAddress(), pubParams1.getPort(),
                pubParams1.getStreamId(), pubParams1.getHostname(), 88);
        subscriberManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo);
        Thread.sleep(1000);
        sendMessageAndCheckArrival(publisher1, false);

        // Now with the right security parameters, but wont work because the RSA don't have the pub key
        topicSocketInfo = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "mtopic1", UUID.randomUUID(), pubParams1.getIpAddress(), pubParams1.getPort(),
                pubParams1.getStreamId(), pubParams1.getHostname(), 33333);
        subscriberManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo);
        Thread.sleep(1000);
        sendMessageAndCheckArrival(publisher1, false);

        // Now with something really valid
        topicSocketInfo = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "mtopic1", UUID.randomUUID(), pubParams1.getIpAddress(), pubParams1.getPort(),
                pubParams1.getStreamId(), pubParams1.getHostname(), 22222);
        subscriberManager.onNewAutoDiscTopicSocketInfo(topicSocketInfo);
        Thread.sleep(1000);
        sendMessageAndCheckArrival(publisher1, true);

        // Unsubscribe from topic
        subscriberManager.unsubscribeFromTopic("mtopic1");
    }

    private void sendMessageAndCheckArrival(final AeronPublisher aeronPublisher, boolean shouldArrive) throws InterruptedException
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(128));
        buffer.putInt(0, 128);

        aeronPublisher.sendMessage(MsgType.DATA, UUID.randomUUID(), buffer, new Random().nextLong(), 0, 4);
        Thread.sleep(500);

        if (shouldArrive)
        {
            Assert.assertNotNull(POLLER_LISTENER.receivedMsg);
        }
        else
        {
            Assert.assertNull(POLLER_LISTENER.receivedMsg);
        }

        POLLER_LISTENER.reset();
    }

    @Override
    public void onMessageReceived(IRcvMessage receivedMessage)
    {

    }

    @Override
    public void onRequestReceived(IRcvRequest receivedRequest)
    {

    }

    static class ReceiverListener implements ISubscribersPollerListener
    {
        volatile IRcvMessage receivedMsg = null;

        @Override
        public void onDataMsgReceived(RcvMessage msg)
        {
            this.receivedMsg = msg.promote();
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
        public void onHeartbeatRequestMsgReceived(MsgReqHeader heartbeatReqMsgHeader)
        {

        }

        private void reset()
        {
            this.receivedMsg = null;
        }
    }
}