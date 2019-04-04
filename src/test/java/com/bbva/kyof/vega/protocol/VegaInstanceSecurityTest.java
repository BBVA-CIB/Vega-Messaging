package com.bbva.kyof.vega.protocol;

import com.bbva.kyof.vega.config.general.ConfigReaderTest;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.msg.IRcvMessage;
import com.bbva.kyof.vega.msg.IRcvRequest;
import com.bbva.kyof.vega.protocol.common.KeySecurityType;
import com.bbva.kyof.vega.protocol.common.SecurityParams;
import com.bbva.kyof.vega.protocol.common.VegaInstanceParams;
import com.bbva.kyof.vega.protocol.publisher.ITopicPublisher;
import com.bbva.kyof.vega.protocol.subscriber.ITopicSubListener;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Test for the {@link VegaInstance} class
 * Created by XE48745 on 15/09/2015.
 */
public class VegaInstanceSecurityTest
{
    private static final String KEYS_DIR_PATH = VegaInstanceSecurityTest.class.getClassLoader().getResource("keys").getPath();
    private static final String SECURE_CONFIG = ConfigReaderTest.class.getClassLoader().getResource("config/vegaInstanceSecureTestConfig.xml").getPath();
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

    @Test(expected = VegaException.class)
    public void testSecureConfigNoSecureParams() throws Exception
    {
        final VegaInstanceParams params1 = VegaInstanceParams.builder().
                instanceName("Instance1").
                configurationFile(SECURE_CONFIG).
                unmanagedMediaDriver(MEDIA_DRIVER).build();

        // Create 2 application instances, use auto-closeable just in case
        VegaInstance.createNewInstance(params1);
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
                configurationFile(SECURE_CONFIG).
                unmanagedMediaDriver(MEDIA_DRIVER).
                securityParams(securityParams1).build();

        final VegaInstanceParams params2 = VegaInstanceParams.builder().
                instanceName("Instance2").
                configurationFile(SECURE_CONFIG).
                unmanagedMediaDriver(MEDIA_DRIVER).
                securityParams(securityParams2).build();

        // Create 2 application instances, use auto-closeable just in case
        try(final IVegaInstance subInstance = VegaInstance.createNewInstance(params1);
            final IVegaInstance pubInstance = VegaInstance.createNewInstance(params2))
        {
            // Subscribe to a topic, we should have permissions
            final ReceiverListener stopic1Listener = new ReceiverListener();
            subInstance.subscribeToTopic("stopic1", stopic1Listener);

            // Create a publisher for the topic, we should have permissions
            final ITopicPublisher stopic1Pub = pubInstance.createPublisher("stopic1");

            // Wait to give the auto-discovery time to work and the session key to be discovered
            Thread.sleep(5000);

            // Test send receive
            this.testSendReceive(stopic1Pub, stopic1Listener, true);

            // Destroy publisher
            pubInstance.destroyPublisher("stopic1");
        }
    }

    @Test
    public void testWildcardSubscriptions() throws Exception
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
                configurationFile(SECURE_CONFIG).
                unmanagedMediaDriver(MEDIA_DRIVER).
                securityParams(securityParams1).build();

        final VegaInstanceParams params2 = VegaInstanceParams.builder().
                instanceName("Instance2").
                configurationFile(SECURE_CONFIG).
                unmanagedMediaDriver(MEDIA_DRIVER).
                securityParams(securityParams2).build();

        // Create 2 application instances, use auto-closeable just in case
        try(final IVegaInstance subInstance = VegaInstance.createNewInstance(params1);
            final IVegaInstance pubInstance = VegaInstance.createNewInstance(params2))
        {
            // Subscribe to 2 topis of each type
            final ReceiverListener listener = new ReceiverListener();

            // Subscribe to anything that ends in 1
            subInstance.subscribeToPattern(".*1", listener);

            // Create a publisher per topic
            final ITopicPublisher utopic1Pub = pubInstance.createPublisher("stopic1");

            // Wait to give the auto-discovery time to work
            Thread.sleep(5000);

            // Test send receive
            this.testSendReceive(utopic1Pub, listener, true);

            // Unsubscribe and test again
            subInstance.unsubscribeFromPattern(".*1");

            this.testSendReceive(utopic1Pub, listener, false);

            // Double stop test
            subInstance.close();
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
        Thread.sleep(1000);

        // Check for reception
        if (shouldReceive)
        {
            Assert.assertTrue(listener.receivedMsg.getContents().getInt(0) == 33);
            Assert.assertEquals(listener.receivedMsg.getTopicName(), publisher.getTopicName());
        }
        else
        {
            Assert.assertNull(listener.receivedMsg);
        }
    }

    private static class ReceiverListener implements ITopicSubListener
    {
        volatile IRcvMessage receivedMsg = null;

        private void reset()
        {
            this.receivedMsg = null;
        }

        @Override
        public void onMessageReceived(final IRcvMessage receivedMessage)
        {
            this.receivedMsg = receivedMessage.promote();
        }

        @Override
        public void onRequestReceived(IRcvRequest receivedRequest)
        {
        }
    }
}