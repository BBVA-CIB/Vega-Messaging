package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.msg.IRcvMessage;
import com.bbva.kyof.vega.msg.IRcvRequest;
import com.bbva.kyof.vega.msg.RcvMessage;
import com.bbva.kyof.vega.util.crypto.AESCrypto;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by cnebrera on 16/11/2016.
 */
public class SecureTopicSubscriberTest
{
    private final Random rnd = new Random();
    // Create an AES Crypto
    private AESCrypto aesCrypto;
    private final Listener listener = new Listener();
    private SecureTopicSubscriber topicSubscriber;


    @Before
    public void before() throws Exception
    {
        aesCrypto = AESCrypto.createNewInstance();

        final TopicTemplateConfig config = new TopicTemplateConfig();
        // Create security template configuration for the subscriber topic
        final Set<Integer> pubTopic1SecureSubs = new HashSet<>(Collections.singletonList(22222));
        final Set<Integer> pubTopic1SecurePubs = new HashSet<>(Arrays.asList(11111, 22222, 33333));
        final TopicSecurityTemplateConfig securityTemplateConfig = new TopicSecurityTemplateConfig("secureConfig", 1000L, pubTopic1SecurePubs, pubTopic1SecureSubs);
        topicSubscriber = new SecureTopicSubscriber("topic1", config, securityTemplateConfig);
        topicSubscriber.setNormalListener(listener);

        // Test basic getters
        Assert.assertEquals(topicSubscriber.getTopicName(), "topic1");
        Assert.assertEquals(topicSubscriber.getTopicConfig(), config);
        Assert.assertEquals(topicSubscriber.getTopicSecurityConfig(), securityTemplateConfig);
        Assert.assertTrue(topicSubscriber.hasSecurity());
        Assert.assertTrue(topicSubscriber.isTopicPubSecureIdAllowed(11111));
        Assert.assertFalse(topicSubscriber.isTopicPubSecureIdAllowed(55));
    }


    @Test
    public void testInternalBufferGrow() throws Exception
    {
        ByteBuffer msg = this.createMsg(128);
        ByteBuffer encodedMsg = this.encodeMsg(msg, aesCrypto);

        this.simulateRcvMsg(aesCrypto, encodedMsg);
        final UnsafeBuffer buffer1 = listener.lastRcvMsg.getContents();

        // Send a bigger one
        msg = this.createMsg(256);
        encodedMsg = this.encodeMsg(msg, aesCrypto);

        this.simulateRcvMsg(aesCrypto, encodedMsg);
        final UnsafeBuffer buffer2 = listener.lastRcvMsg.getContents();

        // Both buffers should be the same
        Assert.assertSame(buffer1, buffer2);

        // Now a message over internal buffer sizes
        msg = this.createMsg(2128);
        encodedMsg = this.encodeMsg(msg, aesCrypto);
        this.simulateRcvMsg(aesCrypto, encodedMsg);
        final UnsafeBuffer buffer3 = listener.lastRcvMsg.getContents();

        // Both buffers should be the different
        Assert.assertSame(buffer2, buffer3);

        // Try again, this time should be the same again
        msg = this.createMsg(3000);
        encodedMsg = this.encodeMsg(msg, aesCrypto);
        this.simulateRcvMsg(aesCrypto, encodedMsg);
        final UnsafeBuffer buffer4 = listener.lastRcvMsg.getContents();

        // Both buffers should be the different
        Assert.assertSame(buffer3, buffer4);
    }

    @Test
    public void onSecureMsgReceivedDifferentSizes() throws Exception
    {
        for (int i = 0; i < 4096; i++)
        {
            // Create and encode a message
            final ByteBuffer msg = this.createMsg(i);
            // Create the byte buffer that will contain the encoded msg
            final ByteBuffer encodedMsg = this.encodeMsg(msg, aesCrypto);

            this.simulateRcvMsg(aesCrypto, encodedMsg);

            // Now get the message and check if it has been correctly decoded
            Assert.assertEquals(0, listener.lastRcvMsg.getContentOffset());
            Assert.assertEquals(listener.lastRcvMsg.getContentLength(), i);

            // Make sure the contents are the same
            final byte[] rcvMsg = new byte[listener.lastRcvMsg.getContentLength()];
            listener.lastRcvMsg.getContents().getBytes(0, rcvMsg);

            Assert.assertArrayEquals(msg.array(), rcvMsg);
        }
    }

    @Test
    public void onSecureMsgWrongEncryption()
    {
        // Create a message without encoding
        final ByteBuffer msg = this.createMsg(32);
        listener.lastRcvMsg = null;

        this.simulateRcvMsg(aesCrypto, msg);

        // Should have caused an exception and never arrive
        Assert.assertNull(listener.lastRcvMsg);
    }

    private void simulateRcvMsg(AESCrypto aesCrypto, ByteBuffer encodedMsg)
    {
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(encodedMsg, 0, encodedMsg.limit());
        final RcvMessage rcvMessage = new RcvMessage();
        rcvMessage.setUnsafeBufferContent(unsafeBuffer);
        rcvMessage.setContentOffset(0);
        rcvMessage.setContentLength(encodedMsg.limit());
        rcvMessage.setTopicPublisherId(UUID.randomUUID());
        topicSubscriber.onSecureMsgReceived(rcvMessage, aesCrypto);
    }

    private ByteBuffer createMsg(final int size)
    {
        // Create and encode a message
        final byte[] msg = new byte[size];
        rnd.nextBytes(msg);

        final ByteBuffer byteBufferMsg = ByteBuffer.wrap(msg);
        byteBufferMsg.limit(size);
        byteBufferMsg.position(0);

        return byteBufferMsg;
    }

    private ByteBuffer encodeMsg(final ByteBuffer msg, final AESCrypto aesCrypto) throws VegaException
    {
        final ByteBuffer encodedMsg = ByteBuffer.allocate(aesCrypto.expectedEncryptedSize(msg));
        aesCrypto.encode(msg, encodedMsg);
        encodedMsg.flip();

        return encodedMsg;
    }

    private class Listener implements ITopicSubListener
    {
        private IRcvMessage lastRcvMsg;

        @Override
        public void onMessageReceived(IRcvMessage receivedMessage)
        {
            this.lastRcvMsg = receivedMessage;
        }

        @Override
        public void onRequestReceived(IRcvRequest receivedRequest)
        {
        }
    }
}