package com.bbva.kyof.vega.protocol;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.bbva.kyof.vega.config.general.ConfigReaderTest;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.msg.*;
import com.bbva.kyof.vega.protocol.common.KeySecurityType;
import com.bbva.kyof.vega.protocol.common.SecurityParams;
import com.bbva.kyof.vega.protocol.common.VegaInstanceParams;
import com.bbva.kyof.vega.protocol.publisher.ITopicPublisher;
import com.bbva.kyof.vega.protocol.subscriber.ITopicSubListener;
import com.bbva.kyof.vega.util.threads.RecurrentRunner;
import com.bbva.kyof.vega.util.threads.RecurrentTask;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Test for the {@link VegaInstance} class
 * Created by XE48745 on 15/09/2015.
 */
public class VegaInstanceStressTest
{
    // Configuration
    private static final String STAND_ALONE_CONFIG = ConfigReaderTest.class.getClassLoader().getResource("config/vegaInstanceSecureStandAloneDriverTestConfig.xml").getPath();

    private static final String KEYS_DIR_PATH = VegaInstanceStressTest.class.getClassLoader().getResource("keys").getPath();

    // Media driver
    private static MediaDriver MEDIA_DRIVER;

    public static final int NUM_THREADS = 5;
    public static final int NUM_TOPICS = 20;
    public static final int TEST_TIME = 10000;

    // All the TOPICS used for the test
    public static final String[] TOPICS = new String[NUM_TOPICS * 4];

    private static Level ORIG_LOG_LEVEL;

    @BeforeClass
    public static void beforeClass()
    {
        // Return log level to trace for normal unit tests
        Logger root = (Logger) LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        ORIG_LOG_LEVEL = root.getLevel();
        root.setLevel(Level.OFF);

        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        // Create a list of TOPICS, 20 TOPICS of each type, including secure topics
        for (int i = 0; i < NUM_TOPICS; i++)
        {
            TOPICS[i] = "utopic" + i;
            TOPICS[i + NUM_TOPICS] = "mtopic" + i;
            TOPICS[i + NUM_TOPICS * 2] = "itopic" + i;
            TOPICS[i + NUM_TOPICS * 3] = "stopic" + i;
        }
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        CloseHelper.quietClose(MEDIA_DRIVER);

        // Return log level to trace for normal unit tests
        Logger root = (Logger) LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        root.setLevel(ORIG_LOG_LEVEL);
    }

    @Test
    public void testSendReceiveMultipleInstances() throws Exception
    {
        final SecurityParams securityParams1 = SecurityParams.builder().
                keySecurityType(KeySecurityType.PLAIN_KEY_FILE).
                privateKeyDirPath(KEYS_DIR_PATH).
                publicKeysDirPath(KEYS_DIR_PATH).
                securityId(11111).build();

        final SecurityParams securityParams2 = SecurityParams.builder().
                keySecurityType(KeySecurityType.PLAIN_KEY_FILE).
                privateKeyDirPath(KEYS_DIR_PATH).
                publicKeysDirPath(KEYS_DIR_PATH).
                securityId(22222).build();

        final VegaInstanceParams params1 = VegaInstanceParams.builder().
                instanceName("Instance1").
                configurationFile(STAND_ALONE_CONFIG).
                unmanagedMediaDriver(MEDIA_DRIVER).
                securityParams(securityParams1).build();

        final VegaInstanceParams params2 = VegaInstanceParams.builder().
                instanceName("Instance2").
                configurationFile(STAND_ALONE_CONFIG).
                unmanagedMediaDriver(MEDIA_DRIVER).
                securityParams(securityParams2).build();

        // Create 2 application instances, use auto-closeable just in case
        try(final IVegaInstance instance1 = VegaInstance.createNewInstance(params1);
            final IVegaInstance instance2 = VegaInstance.createNewInstance(params2))
        {
            final List<ActionExecutor> executors = new LinkedList<>();

            // Create a thread per instance
            for (int i = 0; i < NUM_THREADS; i++)
            {
                final ActionExecutor newExecutor1 = new ActionExecutor(TOPICS, instance1);
                final ActionExecutor newExecutor2 = new ActionExecutor(TOPICS, instance2);
                executors.add(newExecutor1);
                executors.add(newExecutor2);
            }

            // Run all
            executors.forEach((executor) -> executor.start("TreadExecutor"));

            // Wait for the test time
            Thread.sleep(TEST_TIME);

            // Stop all executors
            executors.forEach(RecurrentRunner::close);
        }
    }

    public static class ActionType
    {
        public static final int SUB = 0;
        public static final int UNSUB = 1;
        public static final int WILDCARD_SUB = 2;
        public static final int WILDCARD_UNSUB = 3;
        public static final int CREATE_PUB = 4;
        public static final int DESTROY_PUB = 5;
        public static final int PUB = 6;
        public static final int REQ = 7;
    }

    private static class ActionExecutor extends RecurrentTask implements ITopicSubListener, IResponseListener
    {
        private final List<ITopicPublisher> topicPublishers = new LinkedList<>();
        private final List<String> subscriptions = new LinkedList<>();
        private final List<String> wildcardSubscriptions = new LinkedList<>();
        private final Random rnd = new Random(System.nanoTime());
        private final String[] topics;
        private final IVegaInstance vegaInstance;

        public ActionExecutor(final String[] topics, final IVegaInstance vegaInstance)
        {
            super(new SleepingIdleStrategy(TimeUnit.MILLISECONDS.toNanos(1)));
            this.topics = topics;
            this.vegaInstance = vegaInstance;
        }

        private String getRandomTopic()
        {
            return topics[rnd.nextInt(topics.length)];
        }

        @Override
        public int action()
        {
            switch (rnd.nextInt(8))
            {
                case ActionType.SUB:
                    this.subscribe();
                    break;
                case ActionType.UNSUB:
                    this.unsubscribe();
                    break;
                case ActionType.WILDCARD_SUB:
                    this.wildcardSub();
                    break;
                case ActionType.WILDCARD_UNSUB:
                    this.wildcardUnsub();
                    break;
                case ActionType.CREATE_PUB:
                    this.createPub();
                    break;
                case ActionType.DESTROY_PUB:
                    this.destroyPub();
                    break;
                case ActionType.PUB:
                    this.pub();
                    break;
                case ActionType.REQ:
                    this.req();
                    break;
            }

            return 0;
        }

        private void req()
        {
            if (topicPublishers.isEmpty())
            {
                return;
            }

            // Get a random publisher
            UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(128));
            buffer.putInt(0, 11);
            this.topicPublishers.get(rnd.nextInt(topicPublishers.size())).sendRequest(buffer, 0, 4, 1000, this);
        }

        private void pub()
        {
            if (topicPublishers.isEmpty())
            {
                return;
            }

            // Get a random publisher
            UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(128));
            buffer.putInt(0, 11);
            this.topicPublishers.get(rnd.nextInt(topicPublishers.size())).sendMsg(buffer, 0, 4);
        }

        private void destroyPub()
        {
            // Get random topic
            final String topic = this.getRandomTopic();

            // Destroy
            try
            {
               this.vegaInstance.destroyPublisher(topic);
            }
            catch (VegaException e)
            {
                return;
            }

            // If correct remove the publisher
            this.topicPublishers.removeIf((topicPub) -> topicPub.getTopicName().equals(topic));
        }

        private void createPub()
        {
            // Get random topic
            final String topic = this.getRandomTopic();
            ITopicPublisher publisher = null;

            // Subscribe
            try
            {
                publisher = this.vegaInstance.createPublisher(topic);
            }
            catch (VegaException e)
            {
                return;
            }

            // If correct store the subscription
            this.topicPublishers.add(publisher);
        }

        private void wildcardUnsub()
        {
            // Get random topic
            final String topic = this.getRandomTopic();

            // Unsubscribe
            try
            {
                this.vegaInstance.unsubscribeFromPattern(topic);
            }
            catch (VegaException e)
            {
                return;
            }

            // If correct store the unsubscription
            this.wildcardSubscriptions.remove(topic);
        }

        private void wildcardSub()
        {
            // Get random topic
            final String topic = this.getRandomTopic();

            // Unsubscribe
            try
            {
                this.vegaInstance.subscribeToPattern(topic, this);
            }
            catch (VegaException e)
            {
                return;
            }

            // If correct store the unsubscription
            this.wildcardSubscriptions.add(topic);
        }

        private void unsubscribe()
        {
            // Get random topic
            final String topic = this.getRandomTopic();

            // Unsubscribe
            try
            {
                this.vegaInstance.unsubscribeFromTopic(topic);
            }
            catch (VegaException e)
            {
                return;
            }

            // If correct store the unsubscription
            this.subscriptions.remove(topic);
        }

        private void subscribe()
        {
            // Get random topic
            final String topic = this.getRandomTopic();

            // Subscribe
            try
            {
                this.vegaInstance.subscribeToTopic(topic, this);
            }
            catch (VegaException e)
            {
                return;
            }

            // If correct store the subscription
            this.subscriptions.add(topic);
        }

        @Override
        public void cleanUp()
        {

        }

        @Override
        public void onMessageReceived(IRcvMessage receivedMessage)
        {
        }

        @Override
        public void onRequestReceived(IRcvRequest receivedRequest)
        {
            UnsafeBuffer responseBuffer = new UnsafeBuffer(ByteBuffer.allocate(128));
            responseBuffer.putInt(0, 55);
            receivedRequest.sendResponse(responseBuffer, 0, 4);
        }

        @Override
        public void onRequestTimeout(ISentRequest originalSentRequest)
        {

        }

        @Override
        public void onResponseReceived(ISentRequest originalSentRequest, IRcvResponse response)
        {
        }
    }
}