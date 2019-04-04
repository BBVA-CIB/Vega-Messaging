package com.bbva.kyof.vega.autodiscovery;

import com.bbva.kyof.vega.autodiscovery.daemon.DaemonParameters;
import com.bbva.kyof.vega.autodiscovery.daemon.UnicastDaemon;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.autodiscovery.subscriber.IAutodiscInstanceListener;
import com.bbva.kyof.vega.autodiscovery.subscriber.IAutodiscPubTopicPatternListener;
import com.bbva.kyof.vega.autodiscovery.subscriber.IAutodiscTopicSubListener;
import com.bbva.kyof.vega.config.general.AutoDiscoType;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.agrona.CloseHelper;
import org.junit.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Test {@link AutodiscManager} class
 * 
 * Created by XE52727 on 31/05/16.
 */
@Slf4j
public class AutodiscManagerTest
{
    private final static SubnetAddress SUBNET = InetUtil.getDefaultSubnet();
    private final static String IP = SUBNET.getIpAddres().getHostAddress();

    private final static int PORT_UNICAST_DAEMON = 23403;

    private final static int SYSTEM_INIT_TIME = 3000;
    private final static int MANAGERS_INIT_TIME = 3000;
    public static final long CLIENT_TIMEOUT_TIME = 3000L;
    public static final int SUBSCRIBE_REGISTER_TIME = 600;

    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON1;
    private static Aeron AERON2;
    private static UnicastDaemon DAEMON;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx1 = new Aeron.Context();
        ctx1.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        final Aeron.Context ctx2 = new Aeron.Context();
        ctx2.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON1 = Aeron.connect(ctx1);
        AERON2 = Aeron.connect(ctx2);

        // Create and start unicast daemon
        final DaemonParameters daemonParameters = DaemonParameters.builder().
                subnet(SUBNET.toString()).
                port(PORT_UNICAST_DAEMON).
                clientTimeout(CLIENT_TIMEOUT_TIME).
                aeronDriverType(DaemonParameters.AeronDriverType.EMBEDDED).build();

        daemonParameters.completeAndValidateParameters();
        DAEMON = new UnicastDaemon(daemonParameters);
        DAEMON.start("UnicastDaemon");

        Thread.sleep(SYSTEM_INIT_TIME);
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        // Stop daemon and managers
        DAEMON.close();
        AERON1.close();
        AERON2.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @After
    public void after() throws Exception
    {
        Thread.sleep(2000);
    }

    @Test
    public void testMcastSubscribeToInstance() throws Exception
    {
        final AutodiscManager mcastManager1 = this.createMulticastManager(AERON1);
        final AutodiscManager mcastManager2 = this.createMulticastManager(AERON2);

        // Give the system time to start
        Thread.sleep(MANAGERS_INIT_TIME);

        this.testSubscribeToInstance(mcastManager1, mcastManager2);

        mcastManager1.close();
        mcastManager2.close();
    }

    @Test
    public void testUcastSubscribeToInstance() throws Exception
    {
        final AutodiscManager ucastManager1 = this.createUnicastManager(AERON1);
        final AutodiscManager ucastManager2 = this.createUnicastManager(AERON2);

        this.testSubscribeToInstance(ucastManager1, ucastManager2);

        ucastManager1.close();
        ucastManager2.close();
    }

    private void testSubscribeToInstance(final AutodiscManager manager1, final AutodiscManager manager2) throws Exception
    {
        final AutoDiscInstanceInfo manager1InstanceInfo = new AutoDiscInstanceInfo("manager1", manager1.getInstanceId(), 122, 34566, 55, 132, 34567, 6);
        final AutoDiscInstanceInfo manager2InstanceInfo = new AutoDiscInstanceInfo("manager2", manager2.getInstanceId(), 122, 34566, 55, 132, 34567, 6);

        // Start the manager and give it some time to be "discovered", it is really only required in unicast
        manager1.start();
        Thread.sleep(MANAGERS_INIT_TIME);

        // Create a listener
        final AutoDiscInstanceListener instanceListener1 = new AutoDiscInstanceListener();

        // Subscribe to instance changes, nothing should happen yet
        manager1.subscribeToInstances(instanceListener1);
        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        Assert.assertTrue(instanceListener1.getActiveInstances().isEmpty());

        // Now publish information, we should get it
        manager1.registerInstanceInfo(manager1InstanceInfo);

        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        Assert.assertTrue(instanceListener1.getActiveInstances().contains(manager1InstanceInfo));

        // Start the other manager and give it some time to be "discovered", it is really required only in unicast
        manager2.start();
        Thread.sleep(MANAGERS_INIT_TIME);

        // Create a listener
        final AutoDiscInstanceListener instanceListener2 = new AutoDiscInstanceListener();

        // Subscribe to instance changes, since we also publish our instance info, the result should be almost immediate
        manager2.subscribeToInstances(instanceListener2);
        manager2.registerInstanceInfo(manager2InstanceInfo);
        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        Assert.assertTrue(instanceListener1.getActiveInstances().contains(manager1InstanceInfo));
        Assert.assertTrue(instanceListener1.getActiveInstances().contains(manager2InstanceInfo));
        Assert.assertTrue(instanceListener2.getActiveInstances().contains(manager1InstanceInfo));
        Assert.assertTrue(instanceListener2.getActiveInstances().contains(manager2InstanceInfo));

        // Stop the fist manager instance alerts, since it is going to stop sending adverts the second manager should notice
        manager1.unregisterInstanceInfo();

        Thread.sleep(2000);

        Assert.assertFalse(instanceListener1.getActiveInstances().contains(manager1InstanceInfo));
        Assert.assertTrue(instanceListener1.getActiveInstances().contains(manager2InstanceInfo));
        Assert.assertFalse(instanceListener2.getActiveInstances().contains(manager1InstanceInfo));
        Assert.assertTrue(instanceListener2.getActiveInstances().contains(manager2InstanceInfo));

        // Now unsubscribe from the manager 2 and register an instance info
        manager2.unsubscribeFromInstances(instanceListener2);
        instanceListener2.getActiveInstances().clear();
        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        // Register a new instance, we should receive nothing
        manager1.registerInstanceInfo(manager1InstanceInfo);
        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        Assert.assertFalse(instanceListener2.getActiveInstances().contains(manager1InstanceInfo));

        // Subscribe, now we should have info
        manager2.subscribeToInstances(instanceListener2);
        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        Assert.assertTrue(instanceListener2.getActiveInstances().contains(manager1InstanceInfo));
    }

    @Test
    public void testMcastSubscribeToTopic() throws Exception
    {
        final AutodiscManager ucastManager1 = this.createMulticastManager(AERON1);
        final AutodiscManager ucastManager2 = this.createMulticastManager(AERON2);

        this.testSubscribeToTopic(ucastManager1, ucastManager2);

        ucastManager1.close();
        ucastManager2.close();
    }

    @Test
    public void testUcastSubscribeToTopic() throws Exception
    {
        final AutodiscManager ucastManager1 = this.createUnicastManager(AERON1);
        final AutodiscManager ucastManager2 = this.createUnicastManager(AERON2);

        this.testSubscribeToTopic(ucastManager1, ucastManager2);

        ucastManager1.close();
        ucastManager2.close();
    }

    private void testSubscribeToTopic(final AutodiscManager manager1, final AutodiscManager manager2) throws Exception
    {
        final UUID instanceId = UUID.randomUUID();

        AutoDiscTopicInfo topicInfo1PubIpc = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1");
        AutoDiscTopicInfo topicInfo2PubIpc = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic2");
        AutoDiscTopicInfo topicInfo1PubUni = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_UNI, UUID.randomUUID(), "topic1");

        AutoDiscTopicInfo topicInfo3PubIpc = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic3");
        AutoDiscTopicInfo topicInfo4PubMul = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_MUL, UUID.randomUUID(), "topic4");

        AutoDiscTopicInfo topicInfo3SubIpc = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.SUB_IPC, UUID.randomUUID(), "topic3");
        AutoDiscTopicInfo topicInfo4SubMul = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.SUB_MUL, UUID.randomUUID(), "topic4");

        // Start the managers and give it some time to be "discovered", it is really only required in unicast
        manager1.start();
        manager2.start();
        Thread.sleep(MANAGERS_INIT_TIME);

        // Create a listener
        final AutoDiscTopicListener topicListener1 = new AutoDiscTopicListener();
        final AutoDiscTopicListener topicListener2 = new AutoDiscTopicListener();

        // Subscribe to topic 1, 2 and 3 pub ipc adverts
        manager1.subscribeToTopic("topic1", AutoDiscTransportType.PUB_IPC, topicListener1);
        manager1.subscribeToTopic("topic3", AutoDiscTransportType.PUB_IPC, topicListener1);
        manager1.subscribeToTopic("topic3", AutoDiscTransportType.SUB_IPC, topicListener1);
        manager1.subscribeToTopic("topic4", AutoDiscTransportType.PUB_MUL, topicListener1);
        manager1.subscribeToTopic("topic4", AutoDiscTransportType.SUB_MUL, topicListener1);

        // Now register some wrong topic information
        manager1.registerTopicInfo(topicInfo1PubUni);
        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        Assert.assertFalse(topicListener1.getActiveTopics().contains(topicInfo1PubUni));

        // Now register some wrong topic information
        manager1.registerTopicInfo(topicInfo2PubIpc);
        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        Assert.assertFalse(topicListener1.getActiveTopics().contains(topicInfo2PubIpc));

        // Now register something on the right topic and transport
        manager1.registerTopicInfo(topicInfo1PubIpc);
        manager1.registerTopicInfo(topicInfo3PubIpc);
        manager1.registerTopicInfo(topicInfo4PubMul);
        manager1.registerTopicInfo(topicInfo3SubIpc);
        manager1.registerTopicInfo(topicInfo4SubMul);
        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        Assert.assertTrue(topicListener1.getActiveTopics().contains(topicInfo1PubIpc));
        Assert.assertTrue(topicListener1.getActiveTopics().contains(topicInfo3PubIpc));
        Assert.assertTrue(topicListener1.getActiveTopics().contains(topicInfo4PubMul));
        Assert.assertTrue(topicListener1.getActiveTopics().contains(topicInfo3SubIpc));
        Assert.assertTrue(topicListener1.getActiveTopics().contains(topicInfo4SubMul));

        // Subscribe with manager 2, we should get the info immediately
        manager2.subscribeToTopic("topic1", AutoDiscTransportType.PUB_IPC, topicListener2);
        manager2.subscribeToTopic("topic3", AutoDiscTransportType.PUB_IPC, topicListener2);
        manager2.subscribeToTopic("topic4", AutoDiscTransportType.PUB_MUL, topicListener2);
        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        Assert.assertTrue(topicListener2.getActiveTopics().contains(topicInfo1PubIpc));
        Assert.assertTrue(topicListener2.getActiveTopics().contains(topicInfo3PubIpc));
        Assert.assertTrue(topicListener2.getActiveTopics().contains(topicInfo4PubMul));

        // Now unregister the topic
        manager1.unregisterTopicInfo(topicInfo1PubIpc);
        Thread.sleep(2000);

        // It shouldn't be there anymore
        Assert.assertFalse(topicListener1.getActiveTopics().contains(topicInfo1PubIpc));
        Assert.assertFalse(topicListener2.getActiveTopics().contains(topicInfo1PubIpc));

        // Now unsubscribe from topic and register the topic info again, it should never arrive
        manager2.unsubscribeFromTopic("topic1", AutoDiscTransportType.PUB_IPC, topicListener2);
        manager1.registerTopicInfo(topicInfo1PubIpc);

        Thread.sleep(2000);
        Assert.assertFalse(topicListener2.getActiveTopics().contains(topicInfo1PubIpc));
    }

    @Test
    public void testMcastSubscribeToTopicSocket() throws Exception
    {
        final AutodiscManager ucastManager1 = this.createMulticastManager(AERON1);
        final AutodiscManager ucastManager2 = this.createMulticastManager(AERON2);

        this.testSubscribeToTopicSocket(ucastManager1, ucastManager2);

        ucastManager1.close();
        ucastManager2.close();
    }

    @Test
    public void testUcastSubscribeToTopicSocket() throws Exception
    {
        final AutodiscManager ucastManager1 = this.createUnicastManager(AERON2);
        final AutodiscManager ucastManager2 = this.createUnicastManager(AERON2);

        this.testSubscribeToTopicSocket(ucastManager1, ucastManager2);

        ucastManager1.close();
        ucastManager2.close();
    }

    private void testSubscribeToTopicSocket(final AutodiscManager manager1, final AutodiscManager manager2) throws Exception
    {
        final UUID instanceId = UUID.randomUUID();

        AutoDiscTopicSocketInfo topicInfo1PubIpc = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1", UUID.randomUUID(), 4566, 2344, 23444);
        AutoDiscTopicSocketInfo topicInfo2PubIpc = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic2", UUID.randomUUID(), 4566, 2344, 23444);
        AutoDiscTopicSocketInfo topicInfo1PubUni = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_UNI, UUID.randomUUID(), "topic1", UUID.randomUUID(), 4566, 2344, 23444);

        // Start the managers and give it some time to be "discovered", it is really only required in unicast
        manager1.start();
        manager2.start();
        Thread.sleep(MANAGERS_INIT_TIME);

        // Create a listener
        final AutoDiscTopicListener topicListener1 = new AutoDiscTopicListener();
        final AutoDiscTopicListener topicListener2 = new AutoDiscTopicListener();

        // Subscribe to topic 1 pub ipc adverts
        manager1.subscribeToTopic("topic1", AutoDiscTransportType.PUB_IPC, topicListener1);

        // Now register some wrong topic information
        manager1.registerTopicSocketInfo(topicInfo1PubUni);
        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        Assert.assertFalse(topicListener1.getActiveTopicSockets().contains(topicInfo1PubUni));

        // Now register some wrong topic information
        manager1.registerTopicSocketInfo(topicInfo2PubIpc);
        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        Assert.assertFalse(topicListener1.getActiveTopicSockets().contains(topicInfo2PubIpc));

        // Now register something on the right topic and transport
        manager1.registerTopicSocketInfo(topicInfo1PubIpc);
        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        Assert.assertTrue(topicListener1.getActiveTopicSockets().contains(topicInfo1PubIpc));

        // Subscribe with manager 2, we should get the info immediately
        manager2.subscribeToTopic("topic1", AutoDiscTransportType.PUB_IPC, topicListener2);
        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        Assert.assertTrue(topicListener2.getActiveTopicSockets().contains(topicInfo1PubIpc));

        // Now unregister the topic
        manager1.unregisterTopicSocketInfo(topicInfo1PubIpc);
        Thread.sleep(2000);

        // It shouldn't be there anymore
        Assert.assertFalse(topicListener1.getActiveTopicSockets().contains(topicInfo1PubIpc));
        Assert.assertFalse(topicListener2.getActiveTopicSockets().contains(topicInfo1PubIpc));
    }

    @Test
    public void testMcastSubscribeToPattern() throws Exception
    {
        final AutodiscManager mcastManager1 = this.createMulticastManager(AERON1);
        final AutodiscManager mcastManager2 = this.createMulticastManager(AERON2);

        this.testSubscribeToPattern(mcastManager1, mcastManager2);

        mcastManager1.close();
        mcastManager2.close();
    }

    @Test
    public void testUcastSubscribeToPattern() throws Exception
    {
        final AutodiscManager ucastManager1 = this.createUnicastManager(AERON1);
        final AutodiscManager ucastManager2 = this.createUnicastManager(AERON2);

        this.testSubscribeToPattern(ucastManager1, ucastManager2);

        ucastManager1.close();
        ucastManager2.close();
    }

    private void testSubscribeToPattern(final AutodiscManager manager1, final AutodiscManager manager2) throws Exception
    {
        final UUID instanceId = UUID.randomUUID();

        AutoDiscTopicInfo topicInfo1PubIpc = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1");
        AutoDiscTopicInfo topicInfo2PubUni = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_UNI, UUID.randomUUID(), "topic2");

        // Start the managers and give it some time to be "discovered", it is really only required in unicast
        manager1.start();
        manager2.start();
        Thread.sleep(MANAGERS_INIT_TIME);

        // Create a listener
        final AutoDiscPatternListener patternListener1 = new AutoDiscPatternListener();
        final AutoDiscPatternListener patternListener2 = new AutoDiscPatternListener();

        // Subscribe to topic 1 pub ipc adverts
        manager1.subscribeToPubTopicPattern("topic1", patternListener1);
        manager2.subscribeToPubTopicPattern("topic2", patternListener2);

        // Now register some topic information
        manager1.registerTopicInfo(topicInfo1PubIpc);
        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        Assert.assertTrue(patternListener1.getAliveTopics().contains(topicInfo1PubIpc.getTopicName()));
        Assert.assertFalse(patternListener2.getAliveTopics().contains(topicInfo1PubIpc.getTopicName()));

        // Unregister
        manager1.unregisterTopicInfo(topicInfo1PubIpc);
        Thread.sleep(2000);
        Assert.assertFalse(patternListener1.getAliveTopics().contains(topicInfo1PubIpc.getTopicName()));

        // Now register some topic information
        manager1.registerTopicInfo(topicInfo2PubUni);
        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        Assert.assertFalse(patternListener1.getAliveTopics().contains(topicInfo2PubUni.getTopicName()));
        Assert.assertTrue(patternListener2.getAliveTopics().contains(topicInfo2PubUni.getTopicName()));

        // Unregister
        manager1.unregisterTopicInfo(topicInfo2PubUni);
        Thread.sleep(2000);
        Assert.assertFalse(patternListener2.getAliveTopics().contains(topicInfo2PubUni.getTopicName()));

        // Now test unsubscribe
        manager2.unsubscribeFromPubTopicPattern("topic2");
        manager1.registerTopicInfo(topicInfo2PubUni);
        Thread.sleep(SUBSCRIBE_REGISTER_TIME);
        Assert.assertFalse(patternListener2.getAliveTopics().contains(topicInfo2PubUni.getTopicName()));
    }

    private AutodiscManager createMulticastManager(final Aeron aeron) throws VegaException
    {
        // Create multicast autodiscovery manager
        AutoDiscoveryConfig multicastConfig = AutoDiscoveryConfig.builder().
                autoDiscoType(AutoDiscoType.MULTICAST).
                refreshInterval(1000L).
                timeout(2000L).build();
        multicastConfig.completeAndValidateConfig();

        return new AutodiscManager(aeron, multicastConfig, UUID.randomUUID());
    }

    private AutodiscManager createUnicastManager(final Aeron aeron) throws VegaException
    {
        // Create unicast autodiscovery manager
        AutoDiscoveryConfig unicastConfig = AutoDiscoveryConfig.builder().
                autoDiscoType(AutoDiscoType.UNICAST_DAEMON).
                refreshInterval(1000L).
                timeout(2000L).
                resolverDaemonAddress(IP).
                resolverDaemonPort(PORT_UNICAST_DAEMON).
                build();
        unicastConfig.completeAndValidateConfig();

        return new AutodiscManager(aeron, unicastConfig, UUID.randomUUID());
    }

    private static class AutoDiscPatternListener implements IAutodiscPubTopicPatternListener
    {
        @Getter final Set<String> aliveTopics = Collections.synchronizedSet(new HashSet<>());

        @Override
        public void onNewPubTopicForPattern(AutoDiscTopicInfo topicInfo, String topicPattern)
        {
            aliveTopics.add(topicInfo.getTopicName());
        }

        @Override
        public void onPubTopicForPatternRemoved(AutoDiscTopicInfo topicInfo, String topicPattern)
        {
            aliveTopics.remove(topicInfo.getTopicName());
        }
    }

    private static class AutoDiscInstanceListener implements IAutodiscInstanceListener
    {
        @Getter final Set<AutoDiscInstanceInfo> activeInstances = Collections.synchronizedSet(new HashSet<>());

        @Override
        public void onNewAutoDiscInstanceInfo(AutoDiscInstanceInfo info)
        {
            activeInstances.add(info);
        }

        @Override
        public void onTimedOutAutoDiscInstanceInfo(AutoDiscInstanceInfo info)
        {
            activeInstances.remove(info);
        }
    }

    private static class AutoDiscTopicListener implements IAutodiscTopicSubListener
    {
        @Getter final Set<AutoDiscTopicInfo> activeTopics = Collections.synchronizedSet(new HashSet<>());
        @Getter final Set<AutoDiscTopicSocketInfo> activeTopicSockets = Collections.synchronizedSet(new HashSet<>());

        @Override
        public void onNewAutoDiscTopicInfo(AutoDiscTopicInfo info)
        {
            this.activeTopics.add(info);
        }

        @Override
        public void onTimedOutAutoDiscTopicInfo(AutoDiscTopicInfo info)
        {
            this.activeTopics.remove(info);
        }

        @Override
        public void onNewAutoDiscTopicSocketInfo(AutoDiscTopicSocketInfo info)
        {
            this.activeTopicSockets.add(info);
        }

        @Override
        public void onTimedOutAutoDiscTopicSocketInfo(AutoDiscTopicSocketInfo info)
        {
            this.activeTopicSockets.remove(info);
        }
    }
}
