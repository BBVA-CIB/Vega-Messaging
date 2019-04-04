package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.msg.PublishResult;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.util.collection.NativeArraySet;
import org.agrona.DirectBuffer;

import java.util.UUID;

/**
 * Topic publisher implementation for unicast.
 *
 * In this case there may be multiple related aeron publishers since the subscriber is the end-point of the communication
 *
 * The class is thread-safe
 */
class TopicPublisherUnicast extends AbstractTopicPublisher
{
    /** Default starting publishers size for the internal array */
    private static final int PUBS_NUMBER = 5;

    /** Publisher that can sendMsg the messages into real transport */
    private final NativeArraySet<AeronPublisher> aeronPublishers = new NativeArraySet<>(AeronPublisher.class, PUBS_NUMBER);

    /**
     * Constructor of the class
     *
     * @param topicName Topic name that is going to sendMsg
     * @param topicConfig topic configuration
     * @param vegaContext library instance configuration
     */
    TopicPublisherUnicast(final String topicName, final TopicTemplateConfig topicConfig, final VegaContext vegaContext)
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
        return this.sendToAeron(MsgType.DATA, message, offset, length);
    }

    /**
     * Send message to all the AeronPublishers related to the topic
     *
     * @param msgType the message type for the header
     * @param message the message to send
     * @param offset message offset in the byte buffer
     * @param length message length starting from the offset
     * @return the result of the send process
     */
    PublishResult sendToAeron(final byte msgType, final DirectBuffer message, final int offset, final int length)
    {
        // Get the publishers internal array
        final IAeronPublisher[] publishers = this.aeronPublishers.getInternalArray();

        boolean hasBackPressure = false;

        for (int i = 0; i < this.aeronPublishers.getNumElements(); i++)
        {
            final PublishResult sendResult = publishers[i].sendMessage(msgType, this.getUniqueId(), message, offset, length);

            // If there is an unexpected error, return without trying with any other publisher
            if (sendResult == PublishResult.UNEXPECTED_ERROR)
            {
                return PublishResult.UNEXPECTED_ERROR;
            }
            else if (sendResult == PublishResult.BACK_PRESSURED)
            {
                hasBackPressure = true;
            }
        }

        if (hasBackPressure)
        {
            return PublishResult.BACK_PRESSURED;
        }
        else
        {
            return PublishResult.OK;
        }
    }

    @Override
    protected PublishResult sendRequestToAeron(final byte msgType, final UUID requestId, final DirectBuffer message, final int offset, final int length)
    {
        // Get the publishers internal array
        final IAeronPublisher[] publishers = this.aeronPublishers.getInternalArray();

        boolean hasBackPressure = false;

        for (int i = 0; i < this.aeronPublishers.getNumElements(); i++)
        {
            final PublishResult sendResult = publishers[i].sendRequest(msgType, this.getUniqueId(), requestId, message, offset, length);

            // If there is an unexpected error, return without trying with any other publisher
            if (sendResult == PublishResult.UNEXPECTED_ERROR)
            {
                return sendResult;
            }
            else if (sendResult == PublishResult.BACK_PRESSURED)
            {
                hasBackPressure = true;
            }
        }

        if (hasBackPressure)
        {
            return PublishResult.BACK_PRESSURED;
        }
        else
        {
            return PublishResult.OK;
        }
    }

    @Override
    protected void cleanAeronPublishers()
    {
        this.aeronPublishers.clear();
    }

    /**
     * Add an aeron publisher to the internal list of publishers
     * @param publisher the publisher to add
     */
    void addAeronPublisher(final AeronPublisher publisher)
    {
        synchronized (this.lock)
        {
            this.aeronPublishers.addElement(publisher);
        }
    }

    /**
     * Remove an aeron publisher from the internal list of publishers
     * @param publisher the publisher to remove
     */
    void removeAeronPublisher(final AeronPublisher publisher)
    {
        synchronized (this.lock)
        {
            this.aeronPublishers.removeElement(publisher);
        }
    }
}
