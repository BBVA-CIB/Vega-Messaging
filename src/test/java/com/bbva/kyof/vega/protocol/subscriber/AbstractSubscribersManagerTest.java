package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.autodiscovery.daemon.CommandLineParserTest;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.config.general.*;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.msg.*;
import com.bbva.kyof.vega.protocol.AutoDiscManagerMock;
import com.bbva.kyof.vega.protocol.common.KeySecurityType;
import com.bbva.kyof.vega.protocol.common.SecurityParams;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.control.ISecurityRequesterNotifier;
import com.bbva.kyof.vega.util.collection.HashMapOfHashSet;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.junit.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Created by cnebrera on 11/08/16.
 */
public class AbstractSubscribersManagerTest implements ITopicSubListener
{
    private static final String KEYS_DIR_PATH = CommandLineParserTest.class.getClassLoader().getResource("keys").getPath();

    TopicTemplateConfig topicConfigUnicast;
    TopicTemplateConfig topicConfigIpc;
    TopicTemplateConfig topicConfigMulticast;
    SubscriberManagerImpl subscribersManager;

    private static AutoDiscManagerMock AUTO_DISC_MANAGER_MOCK;
    private static TopicSubAndTopicPubIdRelations relations = new TopicSubAndTopicPubIdRelations();
    static final String validConfigFile = ConfigReaderTest.class.getClassLoader().getResource("config/subscribersManagerTestConfig.xml").getPath();

    static MediaDriver MEDIA_DRIVER;
    static Aeron AERON;
    static SubnetAddress SUBNET_ADDRESS;
    static VegaContext VEGA_CONTEXT;
    static SubscribersPollersManager POLLERS_MANAGER;

    private SecurityRequesterNotifier securityRequesterNotifier = new SecurityRequesterNotifier();

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx1 = new Aeron.Context();
        ctx1.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON = Aeron.connect(ctx1);

        SUBNET_ADDRESS = InetUtil.getDefaultSubnet();
        VEGA_CONTEXT = new VegaContext(AERON, ConfigReader.readConfiguration(validConfigFile));

        // Initialize the security
        final SecurityParams securityParams = SecurityParams.builder().
                keySecurityType(KeySecurityType.PLAIN_KEY_FILE).
                privateKeyDirPath(KEYS_DIR_PATH).
                publicKeysDirPath(KEYS_DIR_PATH).
                securityId(11111).build();

        // Set in the vega contexts
        VEGA_CONTEXT.initializeSecurity(securityParams);

        POLLERS_MANAGER = new SubscribersPollersManager(VEGA_CONTEXT, new ISubscribersPollerListener()
        {
            @Override
            public void onDataMsgReceived(RcvMessage msg) {}

            @Override
            public void onEncryptedDataMsgReceived(RcvMessage msg)
            {

            }

            @Override public void onDataRequestMsgReceived(RcvRequest request) {}
            @Override public void onDataResponseMsgReceived(RcvResponse response) {}
            @Override public void onHeartbeatRequestMsgReceived(MsgReqHeader heartbeatReqMsgHeader) {}
        });

        Thread.sleep(1000);
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Before
    public void beforeTest()
    {
        topicConfigUnicast = TopicTemplateConfig.builder().name("name").transportType(TransportMediaType.UNICAST).build();
        topicConfigMulticast = TopicTemplateConfig.builder().name("name2").transportType(TransportMediaType.MULTICAST).build();
        topicConfigIpc = TopicTemplateConfig.builder().name("name3").transportType(TransportMediaType.IPC).build();

        AUTO_DISC_MANAGER_MOCK = new AutoDiscManagerMock();
        VEGA_CONTEXT.setAutodiscoveryManager(AUTO_DISC_MANAGER_MOCK.getMock());

        subscribersManager = new SubscriberManagerImpl(VEGA_CONTEXT, POLLERS_MANAGER, relations, securityRequesterNotifier);
    }

    @After
    public void afterTest() throws Exception
    {
        // First close should end calling clean
        subscribersManager.close();
        Assert.assertTrue(this.subscribersManager.cleanCalled);

        // Change flag and try again
        this.subscribersManager.cleanCalled = false;
        subscribersManager.close();

        // The second close should not do anything since is already closed
        Assert.assertFalse(this.subscribersManager.cleanCalled);
    }

    @Test(expected = VegaException.class)
    public void testSubscribeToTopicTwice() throws Exception
    {
        subscribersManager.subscribeToTopic("ipc", topicConfigIpc, null, this);
        subscribersManager.subscribeToTopic("ipc", topicConfigIpc, null, this);
    }

    @Test(expected = VegaException.class)
    public void testSubscribeOnClosed() throws Exception
    {
        subscribersManager.close();
        subscribersManager.subscribeToTopic("ipc", topicConfigIpc, null, this);
    }

    @Test(expected = VegaException.class)
    public void testUnsubscribeOnClosed() throws Exception
    {
        subscribersManager.close();
        subscribersManager.unsubscribeFromTopic("ipc");
    }

    @Test(expected = VegaException.class)
    public void testUnsubscribeNonExistingTopic() throws Exception
    {
        subscribersManager.subscribeToTopic("ipc", topicConfigIpc, null, this);
        subscribersManager.unsubscribeFromTopic("perico");
    }

    @Test
    public void testSubscribeUnsubscribe() throws Exception
    {
        subscribersManager.subscribeToTopic("ipc", topicConfigIpc, null, this);
        subscribersManager.subscribeToTopic("multicast", topicConfigMulticast, null, this);
        subscribersManager.subscribeToTopic("unicast", topicConfigUnicast, null, this);

        Assert.assertNull(subscribersManager.getTopicSubscriberForTopicName("perico"));
        Assert.assertNotNull(subscribersManager.getTopicSubscriberForTopicName("ipc"));

        // Check if the topic infos have been registered
        Assert.assertTrue(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().stream().filter((topicInfo -> topicInfo.getTopicName().equals("ipc"))).count() == 1);
        Assert.assertTrue(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().stream().filter((topicInfo -> topicInfo.getTopicName().equals("multicast"))).count() == 1);
        Assert.assertTrue(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().stream().filter((topicInfo -> topicInfo.getTopicName().equals("unicast"))).count() == 1);

        subscribersManager.unsubscribeFromTopic("ipc");
        subscribersManager.unsubscribeFromTopic("multicast");
        subscribersManager.unsubscribeFromTopic("unicast");

        Assert.assertTrue(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().stream().filter((topicInfo -> topicInfo.getTopicName().equals("ipc"))).count() == 0);
        Assert.assertTrue(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().stream().filter((topicInfo -> topicInfo.getTopicName().equals("multicast"))).count() == 0);
        Assert.assertTrue(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().stream().filter((topicInfo -> topicInfo.getTopicName().equals("unicast"))).count() == 0);

        Assert.assertTrue(AUTO_DISC_MANAGER_MOCK.getRegTopicSocketInfos().stream().filter((topicInfo -> topicInfo.getTopicName().equals("unicast"))).count() == 0);
    }

    @Test
    public void testNewPubTopicForPattern() throws Exception
    {
        subscribersManager.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(),"ipc"), "i.*", topicConfigIpc, null, this);

        // Get the created subscriber
        TopicSubscriber subscriberForPattern = subscribersManager.getTopicSubscriberForTopicName("ipc");
        Assert.assertNotNull(subscriberForPattern);

        // Check if it has been registered
        Assert.assertTrue(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().stream().filter((topicInfo -> topicInfo.getTopicName().equals("ipc"))).count() == 1);

        // Now do a normal topic subscription
        subscribersManager.subscribeToTopic("ipc", topicConfigIpc, null, this);

        // Should reuse the same topic subscriber
        Assert.assertEquals(subscriberForPattern, subscribersManager.getTopicSubscriberForTopicName("ipc"));

        // Now unsubscribe
        subscribersManager.onPubTopicForPatternRemoved("ipc", "i.*");

        // There is still one listener, it should not be unregistered
        Assert.assertTrue(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().stream().filter((topicInfo -> topicInfo.getTopicName().equals("ipc"))).count() == 1);
        subscriberForPattern = subscribersManager.getTopicSubscriberForTopicName("ipc");
        Assert.assertFalse(subscriberForPattern.hasNoListeners());
        Assert.assertNotNull(subscriberForPattern);

        // Remove now the normal listener
        subscribersManager.unsubscribeFromTopic("ipc");

        // Now it should have been really unregistered
        Assert.assertTrue(AUTO_DISC_MANAGER_MOCK.getRegTopicInfos().stream().filter((topicInfo -> topicInfo.getTopicName().equals("ipc"))).count() == 0);
        Assert.assertTrue(subscriberForPattern.hasNoListeners());
        Assert.assertNull(subscribersManager.getTopicSubscriberForTopicName("ipc"));
    }

    @Test
    public void testTopicPatternUnsubscription() throws Exception
    {
        subscribersManager.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(),"ipc"), "i.*", topicConfigIpc, null, this);
        subscribersManager.onNewPubTopicForPattern(new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_MUL, UUID.randomUUID(),"multicast"), "i.*", topicConfigMulticast, null, this);

        // Get the created subscribers
        Assert.assertNotNull(subscribersManager.getTopicSubscriberForTopicName("ipc"));
        Assert.assertNotNull(subscribersManager.getTopicSubscriberForTopicName("multicast"));

        // Test pattern unsubscription
        subscribersManager.onTopicPatternUnsubscription("i.*");

        // It should have removed both TOPICS
        Assert.assertNull(subscribersManager.getTopicSubscriberForTopicName("ipc"));
        Assert.assertNull(subscribersManager.getTopicSubscriberForTopicName("multicast"));
    }

    @Test
    public void testOnNewAutodiscTopicInfo() throws Exception
    {
        AutoDiscTopicInfo autoDiscTopicInfo = new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic2");
        subscribersManager.onNewAutoDiscTopicInfo(autoDiscTopicInfo);

        // Since we have no subscriber for that topic, it should do nothing
        Assert.assertNull(relations.getTopicSubscriberForTopicPublisherId(autoDiscTopicInfo.getUniqueId()));

        subscribersManager.onTimedOutAutoDiscTopicInfo(autoDiscTopicInfo);

        // Now subscribe to topic
        subscribersManager.subscribeToTopic("ipc", topicConfigIpc, null, this);

        autoDiscTopicInfo = new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "ipc");
        subscribersManager.onNewAutoDiscTopicInfo(autoDiscTopicInfo);

        // Now it should be there
        Assert.assertNotNull(relations.getTopicSubscriberForTopicPublisherId(autoDiscTopicInfo.getUniqueId()));

        // Now add a topic info with security, it should not be included
        AutoDiscTopicInfo secureAutoDiscTopicInfo = new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "ipc", 55);
        subscribersManager.onNewAutoDiscTopicInfo(secureAutoDiscTopicInfo);

        Assert.assertNull(relations.getTopicSubscriberForTopicPublisherId(secureAutoDiscTopicInfo.getUniqueId()));

        // Remove the topic info
        subscribersManager.onTimedOutAutoDiscTopicInfo(autoDiscTopicInfo);
        Assert.assertNull(relations.getTopicSubscriberForTopicPublisherId(autoDiscTopicInfo.getUniqueId()));

        // Close and try to add new info
        subscribersManager.close();
        subscribersManager.onNewAutoDiscTopicInfo(autoDiscTopicInfo);

        Assert.assertNull(relations.getTopicSubscriberForTopicPublisherId(autoDiscTopicInfo.getUniqueId()));

        subscribersManager.onTimedOutAutoDiscTopicInfo(autoDiscTopicInfo);
    }

    @Test
    public void testUnsubscribeAutoDiscTopicInfo() throws Exception
    {
        // Subscribe to topic
        subscribersManager.subscribeToTopic("ipc", topicConfigIpc, null, this);

        // Several new auto-disc topic info
        AutoDiscTopicInfo autoDiscTopicInfo1 = new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "ipc");
        subscribersManager.onNewAutoDiscTopicInfo(autoDiscTopicInfo1);
        AutoDiscTopicInfo autoDiscTopicInfo2 = new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "ipc");
        subscribersManager.onNewAutoDiscTopicInfo(autoDiscTopicInfo2);
        AutoDiscTopicInfo autoDiscTopicInfo3 = new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "ipc");
        subscribersManager.onNewAutoDiscTopicInfo(autoDiscTopicInfo3);

        // Check
        Assert.assertNotNull(relations.getTopicSubscriberForTopicPublisherId(autoDiscTopicInfo1.getUniqueId()));
        Assert.assertNotNull(relations.getTopicSubscriberForTopicPublisherId(autoDiscTopicInfo2.getUniqueId()));
        Assert.assertNotNull(relations.getTopicSubscriberForTopicPublisherId(autoDiscTopicInfo3.getUniqueId()));

        Assert.assertEquals(relations.getTopicSubscriberForTopicPublisherId(autoDiscTopicInfo1.getUniqueId()),
                relations.getTopicSubscriberForTopicPublisherId(autoDiscTopicInfo2.getUniqueId()));

        // Unsubscribe
        subscribersManager.unsubscribeFromTopic("ipc");

        Assert.assertNull(relations.getTopicSubscriberForTopicPublisherId(autoDiscTopicInfo1.getUniqueId()));
        Assert.assertNull(relations.getTopicSubscriberForTopicPublisherId(autoDiscTopicInfo2.getUniqueId()));
        Assert.assertNull(relations.getTopicSubscriberForTopicPublisherId(autoDiscTopicInfo3.getUniqueId()));
    }

    @Test
    public void testUnsubscribeSecureAutoDiscTopicInfo() throws Exception
    {
        // Create security template configuration for the subscriber topic
        final Set<Integer> pubTopic1SecureSubs = new HashSet<>(Arrays.asList(22222));
        final Set<Integer> pubTopic1SecurePubs = new HashSet<>(Arrays.asList(11111, 22222, 33333));
        final TopicSecurityTemplateConfig securityTemplateConfig = new TopicSecurityTemplateConfig("secureConfig", 1000L, pubTopic1SecurePubs, pubTopic1SecureSubs);

        // Subscribe to topic
        subscribersManager.subscribeToTopic("ipc", topicConfigIpc, securityTemplateConfig, this);
        final TopicSubscriber topicSubscriber = subscribersManager.getTopicSubscriberForTopicName("ipc");

        // First use a non secure auto-disc topic info, should not be included
        AutoDiscTopicInfo autoDiscTopicInfo = new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "ipc");
        subscribersManager.onNewAutoDiscTopicInfo(autoDiscTopicInfo);
        Assert.assertNull(relations.getTopicSubscriberForTopicPublisherId(autoDiscTopicInfo.getUniqueId()));

        // Repeat with a non allowed id
        autoDiscTopicInfo = new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "ipc", 55);
        subscribersManager.onNewAutoDiscTopicInfo(autoDiscTopicInfo);
        Assert.assertNull(relations.getTopicSubscriberForTopicPublisherId(autoDiscTopicInfo.getUniqueId()));

        // Now use an allowed id from which we don't have the public key
        autoDiscTopicInfo = new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "ipc", 33333);
        subscribersManager.onNewAutoDiscTopicInfo(autoDiscTopicInfo);
        Assert.assertNull(relations.getTopicSubscriberForTopicPublisherId(autoDiscTopicInfo.getUniqueId()));

        // Now finally use something valid!
        autoDiscTopicInfo = new AutoDiscTopicInfo(VEGA_CONTEXT.getInstanceUniqueId(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "ipc", 22222);
        subscribersManager.onNewAutoDiscTopicInfo(autoDiscTopicInfo);
        Assert.assertNotNull(relations.getTopicSubscriberForTopicPublisherId(autoDiscTopicInfo.getUniqueId()));

        // It should also have been registered to try to get the session key
        Assert.assertTrue(this.securityRequesterNotifier.topicPubsIdRequestedBySubId.containsValue(topicSubscriber.getUniqueId(), autoDiscTopicInfo.getUniqueId()));

        // Removing the topic info should delete the entry
        subscribersManager.onTimedOutAutoDiscTopicInfo(autoDiscTopicInfo);
        Assert.assertFalse(this.securityRequesterNotifier.topicPubsIdRequestedBySubId.containsValue(topicSubscriber.getUniqueId(), autoDiscTopicInfo.getUniqueId()));

        // Add again and retry if entry is removed when the topic is unsubscribed
        subscribersManager.onNewAutoDiscTopicInfo(autoDiscTopicInfo);
        Assert.assertTrue(this.securityRequesterNotifier.topicPubsIdRequestedBySubId.containsValue(topicSubscriber.getUniqueId(), autoDiscTopicInfo.getUniqueId()));

        subscribersManager.unsubscribeFromTopic("ipc");
        Assert.assertFalse(this.securityRequesterNotifier.topicPubsIdRequestedBySubId.containsValue(topicSubscriber.getUniqueId(), autoDiscTopicInfo.getUniqueId()));
    }

    @Override
    public void onMessageReceived(IRcvMessage receivedMessage)
    {

    }

    @Override
    public void onRequestReceived(IRcvRequest receivedRequest)
    {

    }

    class SubscriberManagerImpl extends AbstractSubscribersManager
    {
        boolean cleanCalled = false;
        Set<String> topicProcessedBeforeDestroy = new HashSet<>();

        SubscriberManagerImpl(VegaContext vegaContext,
                              SubscribersPollersManager pollersManager,
                              TopicSubAndTopicPubIdRelations topicSubAndTopicPubIdRelations,
                              ISecurityRequesterNotifier securityRequesterNotifier)
        {
            super(vegaContext, pollersManager, topicSubAndTopicPubIdRelations, securityRequesterNotifier);
        }

        @Override
        protected void processTopicSubscriberBeforeDestroy(TopicSubscriber topicSubscriber)
        {
            topicProcessedBeforeDestroy.add(topicSubscriber.getTopicName());
        }

        @Override
        protected void cleanAfterClose()
        {
            this.cleanCalled = true;
        }


        @Override
        public void onNewAutoDiscTopicSocketInfo(AutoDiscTopicSocketInfo info)
        {

        }

        @Override
        public void onTimedOutAutoDiscTopicSocketInfo(AutoDiscTopicSocketInfo info)
        {

        }
    }

    class SecurityRequesterNotifier implements ISecurityRequesterNotifier
    {
        final HashMapOfHashSet<UUID, UUID> topicPubsIdRequestedBySubId = new HashMapOfHashSet<>();

        @Override
        public void removedSecureSubTopic(UUID subTopicId)
        {
            this.topicPubsIdRequestedBySubId.removeKey(subTopicId);
        }

        @Override
        public void addedPubForSubTopic(AutoDiscTopicInfo pubTopicInfo, UUID subTopicId, TopicSecurityTemplateConfig subSecurityConfig)
        {
            this.topicPubsIdRequestedBySubId.put(subTopicId, pubTopicInfo.getUniqueId());
        }

        @Override
        public void removedPubForSubTopic(AutoDiscTopicInfo pubTopicInfo, UUID subTopicId)
        {
            this.topicPubsIdRequestedBySubId.remove(subTopicId, pubTopicInfo.getUniqueId());
        }
    }
}