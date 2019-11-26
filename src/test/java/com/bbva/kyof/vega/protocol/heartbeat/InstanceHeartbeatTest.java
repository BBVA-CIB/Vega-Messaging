package com.bbva.kyof.vega.protocol.heartbeat;

import com.bbva.kyof.vega.config.general.ConfigReaderTest;
import com.bbva.kyof.vega.msg.IRcvMessage;
import com.bbva.kyof.vega.msg.IRcvRequest;
import com.bbva.kyof.vega.protocol.IVegaInstance;
import com.bbva.kyof.vega.protocol.VegaInstance;
import com.bbva.kyof.vega.protocol.common.VegaInstanceParams;
import com.bbva.kyof.vega.protocol.publisher.ITopicPublisher;
import com.bbva.kyof.vega.protocol.subscriber.ITopicSubListener;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Test for the {@link VegaInstance} class
 * Created by XE48745 on 15/09/2015.
 */
public class InstanceHeartbeatTest
{
    private static final String STAND_ALONE_CONFIG = ConfigReaderTest.class.getClassLoader().getResource("config/vegaInstanceStandAloneDriverTestConfig.xml").getPath();

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
    public void testHeartbeatInstances() throws Exception
    {
        final VegaInstanceParams params1 = VegaInstanceParams.builder().
                instanceName("Instance1").
                configurationFile(STAND_ALONE_CONFIG).
                unmanagedMediaDriver(MEDIA_DRIVER).build();

        final VegaInstanceParams params2 = VegaInstanceParams.builder().
                instanceName("Instance2").
                configurationFile(STAND_ALONE_CONFIG).
                unmanagedMediaDriver(MEDIA_DRIVER).build();

        // Create 2 application instances, use auto-closeable just in case
        try(final IVegaInstance subInstance = VegaInstance.createNewInstance(params1);
            final IVegaInstance pubInstance = VegaInstance.createNewInstance(params2))
        {
            // Wait to for the initialization of the instances
            Thread.sleep(4000);

            final ClientListener clientListener = new ClientListener();

            // Subscribe to 2 topics of each type
            subInstance.subscribeToTopic("itopic1", new ITopicSubListener()
            {
                @Override
                public void onMessageReceived(IRcvMessage receivedMessage)
                {
                }

                @Override
                public void onRequestReceived(IRcvRequest receivedRequest)
                {
                }
            });

            // Create a publisher per topic
            final ITopicPublisher utopic1Pub = pubInstance.createPublisher("itopic1");
            utopic1Pub.activateHeartbeats(clientListener, HeartbeatParameters.builder().heartbeatRate(200).build());

            // Wait to give the auto-discovery time to work and for at least a heartbeat to arrive
            Thread.sleep(6000);

            // The client should be there
            Assert.assertTrue(clientListener.clients.size() == 1);
            Assert.assertTrue(utopic1Pub.isHeartbeatsActive());

            // Remove subscriber and wait
            subInstance.unsubscribeFromTopic("itopic1");
            Thread.sleep(1000);

            Assert.assertTrue(clientListener.clients.size() == 0);
        }
    }

    private class ClientListener implements IClientConnectionListener
    {
        final Set<UUID> clients = new HashSet<>();

        @Override
        public void onClientConnected(String topicName, UUID clientInstanceId)
        {
            clients.add(clientInstanceId);
            Assert.assertEquals(topicName, "itopic1");
        }

        @Override
        public void onClientDisconnected(String topicName, UUID clientInstanceId)
        {
            clients.remove(clientInstanceId);
            Assert.assertEquals(topicName, "itopic1");
        }
    }
}