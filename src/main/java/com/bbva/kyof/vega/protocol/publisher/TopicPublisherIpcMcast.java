package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.msg.PublishResult;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import org.agrona.DirectBuffer;

import java.util.UUID;
import java.util.function.Consumer;

/**
 * Topic publisher implementation for multicast and ipc.
 *
 * In this case there is a single related aeron publisher because the sender is the end-point.
 *
 * The class is thread-safe
 */
class TopicPublisherIpcMcast extends AbstractTopicPublisher
{
    /** Publisher that can sendMsg the messages into real transport */
    AeronPublisher aeronPublisher;

    /**
     * Constructor of the class
     *
     * @param topicName Topic name that is going to sendMsg
     * @param topicConfig topic configuration

     * @param vegaContext library instance configuration
     */
    TopicPublisherIpcMcast(final String topicName, final TopicTemplateConfig topicConfig, final VegaContext vegaContext)
    {
        super(topicName, topicConfig, vegaContext);
    }

    @Override
    boolean hasSecurity()
    {
        return false;
    }

    @Override
    public TopicSecurityTemplateConfig getTopicSecurityConfig()
    {
        return null;
    }

    @Override
    protected PublishResult sendToAeron(final DirectBuffer message, final int offset, final int length)
    {
        return this.aeronPublisher.sendMessage(MsgType.DATA, this.getUniqueId(), message, offset, length);
    }

    @Override
    protected PublishResult sendRequestToAeron(final byte msgType, final UUID requestId, final DirectBuffer message, final int offset, final int length)
    {
        // Send the request to all the internal Aeron publishers
        return this.aeronPublisher.sendRequest(msgType, this.getUniqueId(), requestId, message, offset, length);
    }

    @Override
    protected void cleanAeronPublishers()
    {
        this.aeronPublisher = null;
    }

    /**
     * Set the Aeron Publisher related to this topic publisher
     * @param value the aeron publisher to store
     */
    void setAeronPublisher(final AeronPublisher value)
    {
        synchronized (this.lock)
        {
            this.aeronPublisher = value;
        }
    }

    /**
     * Run the consumer function for the related Aeron Publisher
     * @param consumer the consumer function to execute
     */
    void runForAeronPublisher(final Consumer<AeronPublisher> consumer)
    {
        synchronized (this.lock)
        {
            consumer.accept(this.aeronPublisher);
        }
    }
}
