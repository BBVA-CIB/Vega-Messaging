package com.bbva.kyof.vega.protocol;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.bbva.kyof.vega.config.general.ConfigReaderTest;
import com.bbva.kyof.vega.msg.*;
import com.bbva.kyof.vega.protocol.common.VegaInstanceParams;
import com.bbva.kyof.vega.protocol.publisher.ITopicPublisher;
import com.bbva.kyof.vega.protocol.subscriber.ITopicSubListener;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Test for the {@link VegaInstance} class
 * Created by XE48745 on 15/09/2015.
 */
public class VegaInstanceMemoryTest
{
    private static final String STAND_ALONE_CONFIG = ConfigReaderTest.class.getClassLoader().getResource("config/vegaInstanceLowLatencyEmbeddedDriverTestConfig.xml").getPath();
    private static int NUM_THREADS = 2;
    private static Level ORIG_LOG_LEVEL;

    @BeforeClass
    public static void beforeClass()
    {
        // Return log level to trace for normal unit tests
        Logger root = (Logger) LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        ORIG_LOG_LEVEL = root.getLevel();
        root.setLevel(Level.INFO);
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        // Return log level to trace for normal unit tests
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(ORIG_LOG_LEVEL);
    }

    @Test
    @Ignore
    public void infiniteSendReceive() throws Exception
    {
        final VegaInstanceParams params1 = VegaInstanceParams.builder().
                instanceName("Instance1").
                configurationFile(STAND_ALONE_CONFIG).build();


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
            Thread.sleep(1000);

            for (int i = 0; i < NUM_THREADS; i++)
            {
                new Thread(() ->
                {
                    UnsafeBuffer sendBuffer = new UnsafeBuffer(ByteBuffer.allocate(128));

                    while (true)
                    {
                        // Test send receive
                        this.send(utopic1Pub, sendBuffer);
                        this.send(utopic2Pub, sendBuffer);
                        this.send(itopic1Pub, sendBuffer);
                        this.send(itopic2Pub, sendBuffer);
                        this.send(mtopic1Pub, sendBuffer);
                        this.send(mtopic2Pub, sendBuffer);

                        // Test req/resp
                        this.sendReq(utopic1Pub, utopic1Listener, sendBuffer);
                        this.sendReq(utopic2Pub, utopic2Listener, sendBuffer);
                        this.sendReq(itopic1Pub, itopic1Listener, sendBuffer);
                        this.sendReq(itopic2Pub, itopic2Listener, sendBuffer);
                        this.sendReq(mtopic1Pub, mtopic1Listener, sendBuffer);
                        this.sendReq(mtopic2Pub, mtopic2Listener, sendBuffer);
                    }
                }).start();
            }

            while (true)
            {
                Thread.sleep(1000);
            }
        }
    }


    private void send(final ITopicPublisher publisher, final UnsafeBuffer sendBuffer)
    {
        // Write the message in the buffer
        sendBuffer.putInt(0, 33);

        // Send and wait
        publisher.sendMsg(sendBuffer, 0, 4);
    }

    private void sendReq(final ITopicPublisher publisher, final ReceiverListener listener, final UnsafeBuffer sendBuffer)
    {
        // Write the message in the buffer
        sendBuffer.putInt(0, 33);

        // Send and wait
        publisher.sendRequest(sendBuffer, 0, 4, 1000, listener);
    }

    static class ReceiverListener implements ITopicSubListener, IResponseListener
    {
        @Override
        public void onMessageReceived(final IRcvMessage receivedMessage)
        {
        }

        @Override
        public void onRequestReceived(IRcvRequest receivedRequest)
        {
            final int receiveReqInt = receivedRequest.getContents().getInt(receivedRequest.getContentOffset());

            // Send a response
            UnsafeBuffer responseBuffer = new UnsafeBuffer(ByteBuffer.allocate(128));
            responseBuffer.putInt(0, receiveReqInt);

            receivedRequest.sendResponse(responseBuffer, 0, 4);
        }

        @Override
        public void onRequestTimeout(ISentRequest originalSentRequest)
        {
        }

        @Override
        public void onResponseReceived(ISentRequest originalSentRequest, IRcvResponse response)
        {
            // Close the original request
            originalSentRequest.closeRequest();
        }
    }
}