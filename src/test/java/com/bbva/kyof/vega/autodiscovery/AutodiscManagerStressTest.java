package com.bbva.kyof.vega.autodiscovery;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * Stress the auto discovery manager
 */
@Slf4j
public class AutodiscManagerStressTest
{
    private final static SubnetAddress SUBNET = InetUtil.getDefaultSubnet();
    private final static String IP = SUBNET.getIpAddres().getHostAddress();

    private final static int PORT_UNICAST_DAEMON = 23403;

    private final static int NUM_TOPICS = 100;
    private final static int STRESS_TEST_TIME = 10000;

    private final static int SYSTEM_INIT_TIME = 3000;
    private final static int STABILIZATION_TIME = 3000;
    private final static int MANAGERS_INIT_TIME = 3000;

    private final static AutoDiscTopicInfo[] TOPICS_INFO_1 = new AutoDiscTopicInfo[NUM_TOPICS];
    private final static AutoDiscTopicSocketInfo[] TOPIC_SOCKETS_INFO_1 = new AutoDiscTopicSocketInfo[NUM_TOPICS];
    private final static AutoDiscTopicInfo[] TOPICS_INFO_2 = new AutoDiscTopicInfo[NUM_TOPICS];
    private final static AutoDiscTopicSocketInfo[] TOPIC_SOCKETS_INFO_2 = new AutoDiscTopicSocketInfo[NUM_TOPICS];

    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static Aeron AERON2;
    private static UnicastDaemon DAEMON;

    private static Level ORIG_LOG_LEVEL;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        final UUID instanceId1 = UUID.randomUUID();
        final UUID instanceId2 = UUID.randomUUID();

        // Set log level to Info to prevent crazy amount of logging for stress test
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        ORIG_LOG_LEVEL = root.getLevel();
        root.setLevel(Level.INFO);

        // Init TOPICS info
        log.info("Creating {} TOPICS, topic sockets info", NUM_TOPICS);
        for (int i = 0; i < NUM_TOPICS; i++)
        {
            TOPICS_INFO_1[i] = new AutoDiscTopicInfo(instanceId1, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic" + i);
            TOPIC_SOCKETS_INFO_1[i] = new AutoDiscTopicSocketInfo(instanceId1, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), TOPICS_INFO_1[i].getTopicName(), TOPICS_INFO_1[i].getUniqueId(), 34, 36, 33);
            TOPICS_INFO_2[i] = new AutoDiscTopicInfo(instanceId2, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic" + i);
            TOPIC_SOCKETS_INFO_2[i] = new AutoDiscTopicSocketInfo(instanceId2, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), TOPICS_INFO_2[i].getTopicName(), TOPICS_INFO_2[i].getUniqueId(), 34, 36, 33);
        }

        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx1 = new Aeron.Context();
        ctx1.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        final Aeron.Context ctx2 = new Aeron.Context();
        ctx2.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON = Aeron.connect(ctx1);
        AERON2 = Aeron.connect(ctx2);

        // Create and start unicast daemon
        final DaemonParameters daemonParameters = DaemonParameters.builder().
                subnet(SUBNET.toString()).
                port(PORT_UNICAST_DAEMON).
                clientTimeout(10000L).
                aeronDriverType(DaemonParameters.AeronDriverType.EMBEDDED).
                build();

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
        AERON2.close();
        CloseHelper.quietClose(MEDIA_DRIVER);

        // Return log level to trace for normal unit tests
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(ORIG_LOG_LEVEL);
    }

    @Test
    public void testMcastSubscribeToTopic() throws Exception
    {
        final AutodiscManager manager1 = this.createMulticastManager(AERON);
        final AutodiscManager manager2 = this.createMulticastManager(AERON2);

        this.runStressTest(manager1, manager2);

        manager1.close();
    }

    @Test
    public void testUcastSubscribeToTopic() throws Exception
    {
        final AutodiscManager manager1 = this.createUnicastManager(AERON);
        final AutodiscManager manager2 = this.createUnicastManager(AERON2);

        this.runStressTest(manager1, manager2);

        manager1.close();
    }

    private void runStressTest(final AutodiscManager manager1, final AutodiscManager manager2) throws Exception
    {
        // Start the managers and give it some time to be "discovered", it is really only required in unicast
        manager1.start();
        manager2.start();
        log.info("Waiting {} millis to allow the manager to initialize", MANAGERS_INIT_TIME);
        Thread.sleep(MANAGERS_INIT_TIME);

        // Create a listeners
        final Listener listener = new Listener();
        final Listener listener2 = new Listener();

        final Thread thread1 = new Thread(() -> performRandomOperations(manager1, TOPICS_INFO_1, TOPIC_SOCKETS_INFO_1, listener));
        final Thread thread2 = new Thread(() -> performRandomOperations(manager2, TOPICS_INFO_2, TOPIC_SOCKETS_INFO_2, listener2));

        // Run the threads and wait for them to finish
        log.info("Running random tests for {} milliseconds", STRESS_TEST_TIME);
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        // Wait for the system to stabilize
        log.info("Wait for {} seconds for stabilization", STABILIZATION_TIME);
        Thread.sleep(STABILIZATION_TIME);

        // Check the final status
        log.info("Manager 1, registered TOPICS {}, subscribed TOPICS {}, in auto-discovery {}",
            listener.getRegisteredTopics().size(),
            listener.getSubscribedTopics().size(),
            listener.getActiveTopicsInAutodisc().size());

        log.info("Manager 1, registered TOPICS sockets {}, subscribed TOPICS {}, in auto-discovery {}",
                listener.getRegisteredTopicSockets().size(),
                listener.getSubscribedTopics().size(),
                listener.getActiveTopicSocketsInAutoDisc().size());

        log.info("Manager 2, registered TOPICS {}, subscribed TOPICS {}, in auto-discovery {}",
                listener2.getRegisteredTopics().size(),
                listener2.getSubscribedTopics().size(),
                listener2.getActiveTopicsInAutodisc().size());

        log.info("Manager 2, registered TOPICS sockets {}, subscribed TOPICS {}, in auto-discovery {}",
                listener2.getRegisteredTopicSockets().size(),
                listener2.getSubscribedTopics().size(),
                listener2.getActiveTopicSocketsInAutoDisc().size());

        // Check if it has worked
        log.info("Checking results");

        checkResults(listener, listener2);
        checkResults(listener2, listener);
    }

    private void checkResults(Listener listener, Listener listener2)
    {
        // Check that all registered TOPICS that are subscribed are in autodiscovery
        for (final AutoDiscTopicInfo topicInfo : listener.getRegisteredTopics())
        {
            if (listener.getSubscribedTopics().contains(topicInfo.getTopicName()))
            {
                Assert.assertTrue(listener.getActiveTopicsInAutodisc().contains(topicInfo));
            }

            if (listener2.getSubscribedTopics().contains(topicInfo.getTopicName()))
            {
                Assert.assertTrue(listener2.getActiveTopicsInAutodisc().contains(topicInfo));
            }
        }

        for (final AutoDiscTopicSocketInfo topicSocketInfo : listener.getRegisteredTopicSockets())
        {
            if (listener.getSubscribedTopics().contains(topicSocketInfo.getTopicName()))
            {
                Assert.assertTrue(listener.getActiveTopicSocketsInAutoDisc().contains(topicSocketInfo));
            }

            if (listener2.getSubscribedTopics().contains(topicSocketInfo.getTopicName()))
            {
                Assert.assertTrue(listener2.getActiveTopicSocketsInAutoDisc().contains(topicSocketInfo));
            }
        }
    }

    private void performRandomOperations(AutodiscManager manager1, AutoDiscTopicInfo[] topicsInfo, AutoDiscTopicSocketInfo[] topicSocketsInfo, Listener listener)
    {
        // Perform random operations for some time every millisecond
        final Random rnd = new Random(System.nanoTime());
        final long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < STRESS_TEST_TIME)
        {
            final int operation = rnd.nextInt(6);
            final int topicNum = rnd.nextInt(NUM_TOPICS);

            switch (operation)
            {
                case 0:
                    manager1.subscribeToTopic(topicsInfo[topicNum].getTopicName(), AutoDiscTransportType.PUB_IPC, listener);
                    listener.addSubscribedTopic(topicsInfo[topicNum].getTopicName());
                    break;
                case 1:
                    manager1.unsubscribeFromTopic(topicsInfo[topicNum].getTopicName(), AutoDiscTransportType.PUB_IPC, listener);
                    listener.removeSubscribedTopic(topicsInfo[topicNum].getTopicName());
                    break;
                case 2:
                    manager1.registerTopicInfo(topicsInfo[topicNum]);
                    listener.registerTopicInfo(topicsInfo[topicNum]);
                    break;
                case 3:
                    manager1.unregisterTopicInfo(topicsInfo[topicNum]);
                    listener.unregisterTopicInfo(topicsInfo[topicNum]);
                    break;
                case 4:
                    manager1.registerTopicSocketInfo(topicSocketsInfo[topicNum]);
                    listener.registerTopicSocketInfo(topicSocketsInfo[topicNum]);
                    break;
                case 5:
                    manager1.unregisterTopicSocketInfo(topicSocketsInfo[topicNum]);
                    listener.unregisterTopicSocketInfo(topicSocketsInfo[topicNum]);
                    break;
            }

            try
            {
                Thread.sleep(1);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }

    private AutodiscManager createMulticastManager(final Aeron aeron) throws VegaException
    {
        // Create multicast autodiscovery manager
        AutoDiscoveryConfig multicastConfig = AutoDiscoveryConfig.builder().
                autoDiscoType(AutoDiscoType.MULTICAST).
                refreshInterval(100L).
                timeout(5000L).build();
        multicastConfig.completeAndValidateConfig();

        return new AutodiscManager(aeron, multicastConfig, UUID.randomUUID());
    }

    private AutodiscManager createUnicastManager(final Aeron aeron) throws VegaException
    {
        // Create unicast autodiscovery manager
        AutoDiscoveryConfig unicastConfig = AutoDiscoveryConfig.builder().
                autoDiscoType(AutoDiscoType.UNICAST_DAEMON).
                refreshInterval(100L).
                timeout(5000L).
                resolverDaemonAddress(IP).
                resolverDaemonPort(PORT_UNICAST_DAEMON).
                build();
        unicastConfig.completeAndValidateConfig();

        return new AutodiscManager(aeron, unicastConfig, UUID.randomUUID());
    }

    static class Listener implements IAutodiscTopicSubListener
    {
        @Getter final Set<AutoDiscTopicInfo> registeredTopics = new HashSet<>();
        @Getter final Set<AutoDiscTopicSocketInfo> registeredTopicSockets = new HashSet<>();
        @Getter final Set<String> subscribedTopics = new HashSet<>();
        @Getter final Set<AutoDiscTopicInfo> activeTopicsInAutodisc = new HashSet<>();
        @Getter final Set<AutoDiscTopicSocketInfo> activeTopicSocketsInAutoDisc = new HashSet<>();

        synchronized public void addSubscribedTopic(final String topic)
        {
            subscribedTopics.add(topic);
        }

        synchronized public void removeSubscribedTopic(final String topic)
        {
            subscribedTopics.remove(topic);

            // Remove active topic and topic socket information
            activeTopicsInAutodisc.removeIf(topicInfo -> topicInfo.getTopicName().equals(topic));
            activeTopicSocketsInAutoDisc.removeIf(topicInfo -> topicInfo.getTopicName().equals(topic));
        }

        synchronized public void registerTopicInfo(final AutoDiscTopicInfo topic)
        {
            registeredTopics.add(topic);
        }

        synchronized public void unregisterTopicInfo(final AutoDiscTopicInfo topic)
        {
            registeredTopics.remove(topic);
        }

        synchronized public void registerTopicSocketInfo(final AutoDiscTopicSocketInfo topic)
        {
            registeredTopicSockets.add(topic);
        }

        synchronized public void unregisterTopicSocketInfo(final AutoDiscTopicSocketInfo topic)
        {
            registeredTopicSockets.remove(topic);
        }

        @Override
        synchronized public void onNewAutoDiscTopicInfo(AutoDiscTopicInfo info)
        {
            if (this.subscribedTopics.contains(info.getTopicName()))
            {
                this.activeTopicsInAutodisc.add(info);
            }
        }

        @Override
        synchronized public void onTimedOutAutoDiscTopicInfo(AutoDiscTopicInfo info)
        {
            if (this.subscribedTopics.contains(info.getTopicName()))
            {
                this.activeTopicsInAutodisc.remove(info);
            }
        }

        @Override
        synchronized public void onNewAutoDiscTopicSocketInfo(AutoDiscTopicSocketInfo info)
        {
            if (this.subscribedTopics.contains(info.getTopicName()))
            {
                this.activeTopicSocketsInAutoDisc.add(info);
            }
        }

        @Override
        synchronized public void onTimedOutAutoDiscTopicSocketInfo(AutoDiscTopicSocketInfo info)
        {
            if (this.subscribedTopics.contains(info.getTopicName()))
            {
                this.activeTopicSocketsInAutoDisc.remove(info);
            }
        }
    }
}
