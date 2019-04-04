package com.bbva.kyof.vega.protocol;

import com.bbva.kyof.vega.config.general.ConfigReaderTest;
import com.bbva.kyof.vega.msg.*;
import com.bbva.kyof.vega.protocol.common.VegaInstanceParams;
import com.bbva.kyof.vega.protocol.heartbeat.HeartbeatParameters;
import com.bbva.kyof.vega.protocol.heartbeat.IClientConnectionListener;
import com.bbva.kyof.vega.protocol.publisher.ITopicPublisher;
import com.bbva.kyof.vega.protocol.subscriber.ITopicSubListener;
import com.bbva.kyof.vega.util.collection.HashMapOfHashSet;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Test for the {@link VegaInstance} class
 * Created by XE48745 on 15/09/2015.
 */
public class VegaInstanceHeartbeatsTest
{
    private static final String STAND_ALONE_CONFIG = ConfigReaderTest.class.getClassLoader().getResource("config/vegaInstanceStandAloneDriverTestConfig.xml").getPath();
    private UnsafeBuffer sendBuffer = new UnsafeBuffer(ByteBuffer.allocate(128));
    private static MediaDriver MEDIA_DRIVER;

    @BeforeClass
    public static void beforeClass()
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Test
    public void testSendReceiveMultipleInstances() throws Exception
    {
        final VegaInstanceParams params1 = VegaInstanceParams.builder().
                instanceName("Instance1").
                configurationFile(STAND_ALONE_CONFIG).
                unmanagedMediaDriver(MEDIA_DRIVER).build();

        params1.validateParams();

        final VegaInstanceParams params2 = VegaInstanceParams.builder().
                instanceName("Instance2").
                configurationFile(STAND_ALONE_CONFIG).
                unmanagedMediaDriver(MEDIA_DRIVER).build();

        params2.validateParams();

        // Create 2 application instances, use auto-closeable just in case
        try(final IVegaInstance subInstance = VegaInstance.createNewInstance(params1);
            final IVegaInstance pubInstance = VegaInstance.createNewInstance(params2))
        {
            // Create the client connection listener
            final ClientConnectionListener connectionListener = new ClientConnectionListener();

            // Create 1 publisher
            final ITopicPublisher publisher = pubInstance.createPublisher("utopic1");

            // Activate heartbeats
            publisher.activateHeartbeats(connectionListener, HeartbeatParameters.builder().heartbeatRate(100).build());

            // Create a subscriber for the topic
            subInstance.subscribeToTopic("utopic1", new ReceiverListener());

            // Wait a bit, the client should be discovered
            Thread.sleep(1000);
            Assert.assertTrue(connectionListener.activeClientInstancesByTopicIc.containsValue("utopic1", subInstance.getInstanceId()));

            // Add another instance by using a second subscription
            pubInstance.subscribeToTopic("utopic1", new ReceiverListener());
            // Wait a bit, the client should be discovered
            Thread.sleep(1000);
            Assert.assertTrue(connectionListener.activeClientInstancesByTopicIc.containsValue("utopic1", pubInstance.getInstanceId()));

            // Unsubscribe one of them
            subInstance.unsubscribeFromTopic("utopic1");
            // Wait a bit, the client should be undiscovered
            Thread.sleep(1000);
            Assert.assertFalse(connectionListener.activeClientInstancesByTopicIc.containsValue("utopic1", subInstance.getInstanceId()));

            // Repeat for the other
            pubInstance.unsubscribeFromTopic("utopic1");
            // Wait a bit, the client should be undiscovered
            Thread.sleep(1000);
            Assert.assertFalse(connectionListener.activeClientInstancesByTopicIc.containsValue("utopic1", pubInstance.getInstanceId()));
        }
    }

    final static class ReceiverListener implements ITopicSubListener
    {
        @Override
        public void onMessageReceived(final IRcvMessage receivedMessage)
        {
        }

        @Override
        public void onRequestReceived(IRcvRequest receivedRequest)
        {
        }
    }

    final static class ClientConnectionListener implements IClientConnectionListener
    {
        final HashMapOfHashSet<String, UUID> activeClientInstancesByTopicIc = new HashMapOfHashSet<>();

        @Override
        public void onClientConnected(String topicName, UUID clientInstanceId)
        {
            this.activeClientInstancesByTopicIc.put(topicName, clientInstanceId);
        }

        @Override
        public void onClientDisconnected(String topicName, UUID clientInstanceId)
        {
            this.activeClientInstancesByTopicIc.remove(topicName, clientInstanceId);
        }
    }
}