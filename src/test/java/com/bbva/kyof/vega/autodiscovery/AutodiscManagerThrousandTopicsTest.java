package com.bbva.kyof.vega.autodiscovery;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.bbva.kyof.vega.TestConstants;
import com.bbva.kyof.vega.autodiscovery.daemon.DaemonParameters;
import com.bbva.kyof.vega.autodiscovery.daemon.UnicastDaemon;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Test connecting and subscribing to thousands of TOPICS
 */
@Slf4j
public class AutodiscManagerThrousandTopicsTest
{
    private final static SubnetAddress SUBNET = InetUtil.getDefaultSubnet();
    private final static String IP = SUBNET.getIpAddres().getHostAddress();

    private final static int PORT_UNICAST_DAEMON = 23403;

    private final static int SYSTEM_INIT_TIME = 3000;
    private final static int MANAGERS_INIT_TIME = 3000;
    private final static int TEST_TIME = 20000;

    private final static int NUM_TOPICS = 5000;

    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static UnicastDaemon DAEMON;

    private static Level ORIG_LOG_LEVEL;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        // Set log level to Info to prevent crazy amount of logging for stress test
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        ORIG_LOG_LEVEL = root.getLevel();
        root.setLevel(Level.INFO);

        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx1 = new Aeron.Context();
        ctx1.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        final Aeron.Context ctx2 = new Aeron.Context();
        ctx2.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON = Aeron.connect(ctx1);

        // Create and start unicast daemon
        final DaemonParameters daemonParameters = DaemonParameters.builder().
                subnet(SUBNET.toString()).
                port(PORT_UNICAST_DAEMON).
                clientTimeout(10000L).
                aeronDriverType(DaemonParameters.AeronDriverType.EMBEDDED).
                externalDriverDir(MEDIA_DRIVER.aeronDirectoryName()).build();

        daemonParameters.completeAndValidateParameters();
        DAEMON = new UnicastDaemon(daemonParameters);
        DAEMON.start("UnicastDaemon");

        log.info("Waiting {} millis to allow the system to initialize", SYSTEM_INIT_TIME);
        Thread.sleep(SYSTEM_INIT_TIME);
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        // Stop daemon and managers
        DAEMON.close();
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);

        // Return log level to trace for normal unit tests
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(ORIG_LOG_LEVEL);
    }

    @Test
    public void testMcastSubscribeToTopic() throws Exception
    {
        final AutodiscManager ucastManager1 = this.createMulticastManager(AERON);

        this.testSubscribeToTopic(ucastManager1);

        ucastManager1.close();
    }

    @Test
    public void testUcastSubscribeToTopic() throws Exception
    {
        final AutodiscManager ucastManager1 = this.createUnicastManager(AERON);

        this.testSubscribeToTopic(ucastManager1);

        ucastManager1.close();
    }

    private void testSubscribeToTopic(final AutodiscManager manager1) throws Exception
    {
        // Create the instance id value
        final UUID instanceId = UUID.randomUUID();

        // Create the infos
        final AutoDiscTopicInfo[] topicsInfo = new AutoDiscTopicInfo[NUM_TOPICS];
        final AutoDiscTopicSocketInfo[] topicsSocketsInfo = new AutoDiscTopicSocketInfo[NUM_TOPICS];

        // Fill with random created TOPICS
        log.info("Creating {} TOPICS, topic sockets info", NUM_TOPICS);
        for (int i = 0; i < NUM_TOPICS; i++)
        {
            topicsInfo[i] = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic" + i);
            topicsSocketsInfo[i] = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), topicsInfo[i].getTopicName(), topicsInfo[i].getUniqueId(), 34, 36, 33, TestConstants.EMPTY_HOSTNAME);
        }

        // Start the managers and give it some time to be "discovered", it is really only required in unicast
        manager1.start();
        log.info("Waiting {} millis to allow the manager to initialize", MANAGERS_INIT_TIME);
        Thread.sleep(MANAGERS_INIT_TIME);

        // Create a listener
        final Listener listener = new Listener();

        // Subscribe to all the TOPICS
        log.info("Subscribing to TOPICS");
        for (int i = 0; i < NUM_TOPICS; i++)
        {
            manager1.subscribeToTopic("topic" + i, AutoDiscTransportType.PUB_IPC, listener);
        }

        // Now register all the TOPICS
        log.info("Registering all");
        for (int i = 0; i < NUM_TOPICS; i++)
        {
            manager1.registerTopicInfo(topicsInfo[i]);
            manager1.registerTopicSocketInfo(topicsSocketsInfo[i]);
        }

        // Give the system time to do its job
        log.info("Wait for {} milliseconds", TEST_TIME);
        Thread.sleep(TEST_TIME);

        log.info("Active topic sockets {}", listener.getActiveTopicSockets().size());
        log.info("Active TOPICS {}", listener.getActiveTopics().size());

        // Check if it has worked
        log.info("Checking results");
        for (int i = 0; i < NUM_TOPICS; i++)
        {
            Assert.assertTrue(listener.getActiveTopics().contains(topicsInfo[i]));
            Assert.assertTrue(listener.getActiveTopicSockets().contains(topicsSocketsInfo[i]));
        }
    }

    private AutodiscManager createMulticastManager(final Aeron aeron) throws VegaException
    {
        // Create multicast autodiscovery manager
        AutoDiscoveryConfig multicastConfig = AutoDiscoveryConfig.builder().
                autoDiscoType(AutoDiscoType.MULTICAST).
                refreshInterval(1000L).
                timeout(5000L).build();
        multicastConfig.completeAndValidateConfig();

        return new AutodiscManager(aeron, multicastConfig, UUID.randomUUID());
    }

    private AutodiscManager createUnicastManager(final Aeron aeron) throws VegaException
    {
        // Create unicast autodiscovery manager
        AutoDiscoveryConfig unicastConfig = AutoDiscoveryConfig.builder().
                autoDiscoType(AutoDiscoType.UNICAST_DAEMON).
                refreshInterval(1000L).
                timeout(5000L).
                resolverDaemonAddress(IP).
                resolverDaemonPort(PORT_UNICAST_DAEMON).
                build();
        unicastConfig.completeAndValidateConfig();

        return new AutodiscManager(aeron, unicastConfig, UUID.randomUUID());
    }

    static class Listener implements IAutodiscTopicSubListener
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
