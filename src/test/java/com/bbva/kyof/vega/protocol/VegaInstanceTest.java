package com.bbva.kyof.vega.protocol;

import com.bbva.kyof.vega.config.general.ConfigReaderTest;
import com.bbva.kyof.vega.msg.*;
import com.bbva.kyof.vega.protocol.common.VegaInstanceParams;
import com.bbva.kyof.vega.protocol.publisher.ITopicPublisher;
import com.bbva.kyof.vega.protocol.subscriber.ITopicSubListener;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Test for the {@link VegaInstance} class
 * Created by XE48745 on 15/09/2015.
 */
public class VegaInstanceTest
{
    private static final String STAND_ALONE_CONFIG = Objects.requireNonNull(ConfigReaderTest.class.getClassLoader().getResource("config/vegaInstanceStandAloneDriverTestConfig.xml")).getPath();
    private static final String EMBEDDED_CONFIG = Objects.requireNonNull(ConfigReaderTest.class.getClassLoader().getResource("config/vegaInstanceEmbeddedDriverTestConfig.xml")).getPath();
    private static final String LOWLATENCY_EMBEDDED_CONFIG = Objects.requireNonNull(ConfigReaderTest.class.getClassLoader().getResource("config/vegaInstanceLowLatencyEmbeddedDriverTestConfig.xml")).getPath();

    private UnsafeBuffer sendBuffer = new UnsafeBuffer(ByteBuffer.allocate(128));

    private static MediaDriver MEDIA_DRIVER;

    @BeforeClass
    public static void beforeClass()
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();
    }

    @AfterClass
    public static void afterClass()
    {
        //CloseHelper.quietClose(MEDIA_DRIVER);
        MEDIA_DRIVER.close();
    }

    @Test
    public void testSendReceiveMultipleInstances() throws Exception
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
            // Subscribe to 2 topis of each type
            final ReceiverListener utopic1Listener = new ReceiverListener();
            final ReceiverListener utopic2Listener = new ReceiverListener();
            final ReceiverListener itopic1Listener = new ReceiverListener();
            final ReceiverListener itopic2Listener = new ReceiverListener();
            final ReceiverListener mtopic1Listener = new ReceiverListener();
            final ReceiverListener mtopic2Listener = new ReceiverListener();
            subInstance.subscribeToTopic("utopic1", utopic1Listener);
            subInstance.subscribeToTopic("utopic2", utopic2Listener);
            subInstance.subscribeToTopic("itopic1", itopic1Listener);
            subInstance.subscribeToTopic("itopic2", itopic2Listener);
            subInstance.subscribeToTopic("mtopic1", mtopic1Listener);
            subInstance.subscribeToTopic("mtopic2", mtopic2Listener);

            // Create a publisher per topic
            final ITopicPublisher utopic1Pub = pubInstance.createPublisher("utopic1");
            final ITopicPublisher utopic2Pub = pubInstance.createPublisher("utopic2");
            final ITopicPublisher itopic1Pub = pubInstance.createPublisher("itopic1");
            final ITopicPublisher itopic2Pub = pubInstance.createPublisher("itopic2");
            final ITopicPublisher mtopic1Pub = pubInstance.createPublisher("mtopic1");
            final ITopicPublisher mtopic2Pub = pubInstance.createPublisher("mtopic2");

            // Wait to give the auto-discovery time to work
            Thread.sleep(4000);

            // Test send receive
            this.testSendReceive(utopic1Pub, utopic1Listener, true);
            this.testSendReceive(utopic2Pub, utopic2Listener, true);
            this.testSendReceive(itopic1Pub, itopic1Listener, true);
            this.testSendReceive(itopic2Pub, itopic2Listener, true);
            this.testSendReceive(mtopic1Pub, mtopic1Listener, true);
            this.testSendReceive(mtopic2Pub, mtopic2Listener, true);

            // Test req/resp
            this.testReqResp(utopic1Pub, utopic1Listener, true);
            this.testReqResp(utopic2Pub, utopic2Listener, true);
            this.testReqResp(itopic1Pub, itopic1Listener, true);
            this.testReqResp(itopic2Pub, itopic2Listener, true);
            this.testReqResp(mtopic1Pub, mtopic1Listener, true);
            this.testReqResp(mtopic2Pub, mtopic2Listener, true);


            // Unsubscribe and test again
            subInstance.unsubscribeFromTopic("utopic1");
            subInstance.unsubscribeFromTopic("itopic1");
            subInstance.unsubscribeFromTopic("mtopic1");

            this.testSendReceive(utopic1Pub, utopic1Listener, false);
            this.testSendReceive(utopic2Pub, utopic2Listener, true);
            this.testSendReceive(itopic1Pub, itopic1Listener, false);
            this.testSendReceive(itopic2Pub, itopic2Listener, true);
            this.testSendReceive(mtopic1Pub, mtopic1Listener, false);
            this.testSendReceive(mtopic2Pub, mtopic2Listener, true);

            this.testReqResp(utopic1Pub, utopic1Listener, false);
            this.testReqResp(utopic2Pub, utopic2Listener, true);
            this.testReqResp(itopic1Pub, itopic1Listener, false);
            this.testReqResp(itopic2Pub, itopic2Listener, true);
            this.testReqResp(mtopic1Pub, mtopic1Listener, false);
            this.testReqResp(mtopic2Pub, mtopic2Listener, true);

            // Destroy publishers
            pubInstance.destroyPublisher("utopic1");
            pubInstance.destroyPublisher("utopic2");
            pubInstance.destroyPublisher("itopic1");
            pubInstance.destroyPublisher("itopic2");
            pubInstance.destroyPublisher("mtopic1");
            pubInstance.destroyPublisher("mtopic2");
        }
    }

    @Test
    public void testSendReceiveSingleInstance() throws Exception
    {
        final VegaInstanceParams params1 = VegaInstanceParams.builder().
                instanceName("Instance1").
                configurationFile(STAND_ALONE_CONFIG).
                unmanagedMediaDriver(MEDIA_DRIVER).build();

        // Create 2 application instances, use auto-closeable just in case
        try(final IVegaInstance instance = VegaInstance.createNewInstance(params1))
        {
            // Subscribe to 2 topis of each type
            final ReceiverListener utopic1Listener = new ReceiverListener();
            final ReceiverListener utopic2Listener = new ReceiverListener();
            final ReceiverListener itopic1Listener = new ReceiverListener();
            final ReceiverListener itopic2Listener = new ReceiverListener();
            final ReceiverListener mtopic1Listener = new ReceiverListener();
            final ReceiverListener mtopic2Listener = new ReceiverListener();
            instance.subscribeToTopic("utopic1", utopic1Listener);
            instance.subscribeToTopic("utopic2", utopic2Listener);
            instance.subscribeToTopic("itopic1", itopic1Listener);
            instance.subscribeToTopic("itopic2", itopic2Listener);
            instance.subscribeToTopic("mtopic1", mtopic1Listener);
            instance.subscribeToTopic("mtopic2", mtopic2Listener);

            // Create a publisher per topic
            final ITopicPublisher utopic1Pub = instance.createPublisher("utopic1");
            final ITopicPublisher utopic2Pub = instance.createPublisher("utopic2");
            final ITopicPublisher itopic1Pub = instance.createPublisher("itopic1");
            final ITopicPublisher itopic2Pub = instance.createPublisher("itopic2");
            final ITopicPublisher mtopic1Pub = instance.createPublisher("mtopic1");
            final ITopicPublisher mtopic2Pub = instance.createPublisher("mtopic2");

            // Wait to give the auto-discovery time to work
            Thread.sleep(4000);

            // Test send receive
            this.testSendReceive(utopic1Pub, utopic1Listener, true);
            this.testSendReceive(utopic2Pub, utopic2Listener, true);
            this.testSendReceive(itopic1Pub, itopic1Listener, true);
            this.testSendReceive(itopic2Pub, itopic2Listener, true);
            this.testSendReceive(mtopic1Pub, mtopic1Listener, true);
            this.testSendReceive(mtopic2Pub, mtopic2Listener, true);

            // Test req / resp
            this.testReqResp(utopic1Pub, utopic1Listener, true);
            this.testReqResp(utopic2Pub, utopic2Listener, true);
            this.testReqResp(itopic1Pub, itopic1Listener, true);
            this.testReqResp(itopic2Pub, itopic2Listener, true);
            this.testReqResp(mtopic1Pub, mtopic1Listener, true);
            this.testReqResp(mtopic2Pub, mtopic2Listener, true);

            // Unsubscribe and test again
            instance.unsubscribeFromTopic("utopic1");
            instance.unsubscribeFromTopic("itopic1");
            instance.unsubscribeFromTopic("mtopic1");

            this.testSendReceive(utopic1Pub, utopic1Listener, false);
            this.testSendReceive(utopic2Pub, utopic2Listener, true);
            this.testSendReceive(itopic1Pub, itopic1Listener, false);
            this.testSendReceive(itopic2Pub, itopic2Listener, true);
            this.testSendReceive(mtopic1Pub, mtopic1Listener, false);
            this.testSendReceive(mtopic2Pub, mtopic2Listener, true);

            this.testReqResp(utopic1Pub, utopic1Listener, false);
            this.testReqResp(utopic2Pub, utopic2Listener, true);
            this.testReqResp(itopic1Pub, itopic1Listener, false);
            this.testReqResp(itopic2Pub, itopic2Listener, true);
            this.testReqResp(mtopic1Pub, mtopic1Listener, false);
            this.testReqResp(mtopic2Pub, mtopic2Listener, true);
        }
    }

    @Test
    public void testWildcardSubscriptions() throws Exception
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
            // Subscribe to 2 topis of each type
            final ReceiverListener listener = new ReceiverListener();

            // Subscribe to anything that ends in 1
            subInstance.subscribeToPattern(".*1", listener);

            // Create a publisher per topic
            final ITopicPublisher utopic1Pub = pubInstance.createPublisher("utopic1");
            final ITopicPublisher utopic2Pub = pubInstance.createPublisher("utopic2");
            final ITopicPublisher itopic1Pub = pubInstance.createPublisher("itopic1");
            final ITopicPublisher itopic2Pub = pubInstance.createPublisher("itopic2");
            final ITopicPublisher mtopic1Pub = pubInstance.createPublisher("mtopic1");
            final ITopicPublisher mtopic2Pub = pubInstance.createPublisher("mtopic2");

            // Wait to give the auto-discovery time to work
            Thread.sleep(4000);

            // Test send receive
            this.testSendReceive(utopic1Pub, listener, true);
            this.testSendReceive(utopic2Pub, listener, false);
            this.testSendReceive(itopic1Pub, listener, true);
            this.testSendReceive(itopic2Pub, listener, false);
            this.testSendReceive(mtopic1Pub, listener, true);
            this.testSendReceive(mtopic2Pub, listener, false);

            // Test req/resp
            this.testReqResp(utopic1Pub, listener, true);
            this.testReqResp(utopic2Pub, listener, false);
            this.testReqResp(itopic1Pub, listener, true);
            this.testReqResp(itopic2Pub, listener, false);
            this.testReqResp(mtopic1Pub, listener, true);
            this.testReqResp(mtopic2Pub, listener, false);

            // Unsubscribe and test again
            subInstance.unsubscribeFromPattern(".*1");

            this.testSendReceive(utopic1Pub, listener, false);
            this.testSendReceive(utopic2Pub, listener, false);
            this.testSendReceive(itopic1Pub, listener, false);
            this.testSendReceive(itopic2Pub, listener, false);
            this.testSendReceive(mtopic1Pub, listener, false);
            this.testSendReceive(mtopic2Pub, listener, false);

            this.testReqResp(utopic1Pub, listener, false);
            this.testReqResp(utopic2Pub, listener, false);
            this.testReqResp(itopic1Pub, listener, false);
            this.testReqResp(itopic2Pub, listener, false);
            this.testReqResp(mtopic1Pub, listener, false);
            this.testReqResp(mtopic2Pub, listener, false);

            // Double stop test
            subInstance.close();
        }
    }

    @Test
    public void testEmbeddedDriver() throws Exception
    {
        testEmbeddedDriver(EMBEDDED_CONFIG);
    }

    @Test
    public void testEmbeddedLowLatencyDriver() throws Exception
    {
        testEmbeddedDriver(LOWLATENCY_EMBEDDED_CONFIG);
    }

    private void testEmbeddedDriver(String config) throws Exception
    {
        final VegaInstanceParams params1 = VegaInstanceParams.builder().
                instanceName("Instance1").
                configurationFile(config).build();

        try(final IVegaInstance instance = VegaInstance.createNewInstance(params1))
        {
            // Subscribe to 2 topics of each type
            final ReceiverListener utopic1Listener = new ReceiverListener();
            instance.subscribeToTopic("utopic1", utopic1Listener);

            // Create a publisher per topic
            final ITopicPublisher utopic1Pub = instance.createPublisher("utopic1");

            // Wait to give the auto-discovery time to work
            Thread.sleep(4000);

            // Test send receive
            this.testSendReceive(utopic1Pub, utopic1Listener, true);

            // Test req / resp
            this.testReqResp(utopic1Pub, utopic1Listener, true);

            // Unsubscribe and test again
            instance.unsubscribeFromTopic("utopic1");

            this.testSendReceive(utopic1Pub, utopic1Listener, false);
            this.testReqResp(utopic1Pub, utopic1Listener, false);
        }
    }

    private void testSendReceive(final ITopicPublisher publisher, final ReceiverListener listener, final boolean shouldReceive) throws Exception
    {
        // Reset the listener contents
        listener.reset();

        // Write the message in the buffer
        sendBuffer.putInt(0, 33);

        // Send and wait
        publisher.sendMsg(sendBuffer, 0, 4);
        Thread.sleep(500);

        // Check for reception
        if (shouldReceive)
        {
            Assert.assertEquals(33, listener.receivedMsg.getContents().getInt(0));
            Assert.assertEquals(listener.receivedMsg.getTopicName(), publisher.getTopicName());
        }
        else
        {
            Assert.assertNull(listener.receivedMsg);
        }
    }

    private void testReqResp(final ITopicPublisher publisher, final ReceiverListener listener, final boolean shouldArrive) throws Exception
    {
        // Reset the listener contents
        listener.reset();

        // Write the message in the buffer
        sendBuffer.putInt(0, 33);

        // Send and wait
        publisher.sendRequest(sendBuffer, 0, 4, 1000, listener);
        Thread.sleep(500);

        // Check for reception of response and request
        if (shouldArrive)
        {
            Assert.assertEquals(33, listener.receivedReq.getContents().getInt(0));
            Assert.assertEquals(listener.receivedReq.getTopicName(), publisher.getTopicName());
            Assert.assertEquals(33, listener.receivedResponse.getContents().getInt(0));
            Assert.assertEquals(listener.receivedResponse.getTopicName(), publisher.getTopicName());
        }
        else
        {
            Assert.assertNull(listener.receivedReq);
            Assert.assertNull(listener.receivedResponse);
            Assert.assertNull(listener.receivedReq);
        }
    }

    static class ReceiverListener implements ITopicSubListener, IResponseListener
    {
        volatile IRcvMessage receivedMsg = null;
        volatile IRcvRequest receivedReq = null;
        volatile IRcvResponse receivedResponse = null;
        volatile ISentRequest timedOutRequest = null;

        private void reset()
        {
            this.receivedMsg = null;
            this.receivedReq = null;
            this.receivedResponse = null;
            this.timedOutRequest = null;
        }

        @Override
        public void onMessageReceived(final IRcvMessage receivedMessage)
        {
            this.receivedMsg = receivedMessage.promote();
        }

        @Override
        public void onRequestReceived(IRcvRequest receivedRequest)
        {
            this.receivedReq = receivedRequest.promote();

            // Send a response
            UnsafeBuffer responseBuffer = new UnsafeBuffer(ByteBuffer.allocate(128));
            responseBuffer.putInt(0, receivedRequest.getContents().getInt(receivedRequest.getContentOffset()));

            this.receivedReq.sendResponse(responseBuffer, 0, 4);
        }

        @Override
        public void onRequestTimeout(ISentRequest originalSentRequest)
        {
            this.timedOutRequest = originalSentRequest;
        }

        @Override
        public void onResponseReceived(ISentRequest originalSentRequest, IRcvResponse response)
        {
            this.receivedResponse = response.promote();

            // Close the original request
            originalSentRequest.closeRequest();
        }
    }
}