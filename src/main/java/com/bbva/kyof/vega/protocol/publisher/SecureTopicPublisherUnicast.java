package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.msg.PublishResult;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * Topic publisher implementation for unicast.
 *
 * In this case there may be multiple related aeron publishers since the subscriber is the end-point of the communication
 *
 * The class is thread-safe
 */
@Slf4j
class SecureTopicPublisherUnicast extends TopicPublisherUnicast
{
    /** Topic security configuration, null if security is not configured */
    @Getter private final TopicSecurityTemplateConfig topicSecurityConfig;

    /** Encoder to encrypt the user messages */
    private final AesTopicMsgEncoder topicMsgEncoder;

    /** Reusable buffer that will be used to wrap the encoded messages before sending */
    private final UnsafeBuffer encryptedUnsafeBuffer = new UnsafeBuffer(ByteBuffer.allocate(0));

    /**
     * Constructor of the class
     *
     * @param topicName Topic name that is going to sendMsg
     * @param topicConfig topic configuration
     * @param topicSecurityConfig security template configuration, null if not secured
     * @param vegaContext library instance configuration
     */
    SecureTopicPublisherUnicast(
            final String topicName,
            final TopicTemplateConfig topicConfig,
            final VegaContext vegaContext,
            final TopicSecurityTemplateConfig topicSecurityConfig) throws VegaException
    {
        super(topicName, topicConfig, vegaContext);
        this.topicSecurityConfig = topicSecurityConfig;
        this.topicMsgEncoder = new AesTopicMsgEncoder();
    }

    /**
     * Returns the encryption session key
     */
    byte[] getSessionKey()
    {
        return this.topicMsgEncoder.getAESKey();
    }

    @Override
    protected PublishResult sendToAeron(final DirectBuffer message, final long sequenceNumber, final int offset, final int length)
    {
        // Encrypt the message
        final ByteBuffer encrypedMsg;
        try
        {
            encrypedMsg = this.topicMsgEncoder.encryptMessage(message, offset, length);
        }
        catch (VegaException e)
        {
            log.error("Unexpected error trying to encrypt a message before sending it in a secure topic publisher", e);
            return PublishResult.UNEXPECTED_ERROR;
        }

        // Wrap the encoded message prior to send
        this.encryptedUnsafeBuffer.wrap(encrypedMsg);

        // Send the message
        return super.sendToAeron(MsgType.ENCRYPTED_DATA, this.encryptedUnsafeBuffer, sequenceNumber, 0, encrypedMsg.limit());
    }

    @Override
    boolean hasSecurity()
    {
        return true;
    }
}
