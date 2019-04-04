package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.msg.RcvMessage;
import com.bbva.kyof.vega.util.crypto.AESCrypto;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * Represent a secure subscription to a topic name and keep track of all the AeronSubscribers sockets that are related to the topic name.
 *
 * The class assumes that listeners changes and aeron subscriber changes are always performed in thread-safe mode. It allows concurrent actions
 * of change listeners and receive messages.
 */
@Slf4j
class SecureTopicSubscriber extends TopicSubscriber
{
    /** Topic security configuration, null if security is not configured */
    @Getter private final TopicSecurityTemplateConfig topicSecurityConfig;

    /** Reusable byte buffer to hold the message contents */
    private ByteBuffer byteBufferMsgContents = ByteBuffer.allocate(1024);

    /** Reusable unsafe buffer for the decoded message contents */
    private final UnsafeBuffer decodedMsgBuffer = new UnsafeBuffer(ByteBuffer.allocate(1024));

    /**
     * Constructs a new topic subscriber
     *
     * @param topicName Topic name the subscriber is associated to
     * @param topicConfig Topic configuration
     * @param topicSecurityConfig the topic security configuration
     */
    SecureTopicSubscriber(final String topicName, final TopicTemplateConfig topicConfig, final TopicSecurityTemplateConfig topicSecurityConfig)
    {
        super(topicName, topicConfig);
        this.topicSecurityConfig = topicSecurityConfig;
    }

    /**
     * Method called when a secure message is received. The message will be "decoded" using the provided AesCrypto.
     *
     * @param receivedMessage the received message
     * @param aesCryptoDecoder the AES crypto decoder to decode the received message
     */
    void onSecureMsgReceived(final RcvMessage receivedMessage, final AESCrypto aesCryptoDecoder)
    {
        // Make sure we can hold the message and the decoded message
        if (this.byteBufferMsgContents.capacity() < receivedMessage.getContentLength())
        {
            this.byteBufferMsgContents = ByteBuffer.allocate(receivedMessage.getContentLength() * 2);
        }

        if (this.decodedMsgBuffer.byteBuffer().capacity() < receivedMessage.getContentLength())
        {
            this.decodedMsgBuffer.wrap(ByteBuffer.allocate((receivedMessage.getContentLength()) * 2));
        }

        // Copy the message contents into the reusable byte buffer
        receivedMessage.getContents().getBytes(receivedMessage.getContentOffset(), this.byteBufferMsgContents, 0, receivedMessage.getContentLength());
        this.byteBufferMsgContents.limit(receivedMessage.getContentLength());
        this.byteBufferMsgContents.position(0);

        // Decode the message
        try
        {
            this.decodedMsgBuffer.byteBuffer().clear();
            aesCryptoDecoder.decode(this.byteBufferMsgContents, this.decodedMsgBuffer.byteBuffer());

            // Change the received message contents
            receivedMessage.setUnsafeBufferContent(this.decodedMsgBuffer);
            receivedMessage.setContentOffset(0);
            receivedMessage.setContentLength(this.decodedMsgBuffer.byteBuffer().position());
        }
        catch (final VegaException e)
        {
            log.warn("Unexpected error decoding secure received message on topic " + this.getTopicName(), e);
            return;
        }

        // Finally call the parent implementation
        super.onMessageReceived(receivedMessage);
    }

    /**
     * Returns true if the topic subscriber is allowed to receive messages from a topic publisher with the given security id
     * @param topicPubSecureId the secure id of the vega instance the topic publisher belongs to
     */
    boolean isTopicPubSecureIdAllowed(final int topicPubSecureId)
    {
        return this.topicSecurityConfig.getPubSecIds().contains(topicPubSecureId);
    }

    /**
     * True if the topic is configured to use security
     */
    @Override
    public boolean hasSecurity()
    {
        return true;
    }
}
