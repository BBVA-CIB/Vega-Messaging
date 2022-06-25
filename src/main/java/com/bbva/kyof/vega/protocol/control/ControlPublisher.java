
package com.bbva.kyof.vega.protocol.control;

import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.msg.BaseHeader;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.msg.PublishResult;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import com.bbva.kyof.vega.util.net.InetUtil;
import io.aeron.Publication;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;

import java.io.Closeable;
import java.nio.ByteBuffer;

/**
 * Wrapper class to encapsulate an Aeron Publisher to send control messages <p>
 *
 * This class is thread safe!!
 */
@Slf4j
class ControlPublisher implements Closeable
{
    /** Buffer size, should be more than enough fort he control messages */
    private static final int WRITE_BUFFER_SIZE = 1024;

    /** Aeron publication */
    private final Publication publication;

    /** Parameters of the publisher */
    @Getter private final ControlPublisherParams params;

    /** Reusable base header */
    private final BaseHeader reusableBaseHeader;

    /** Reusable serializer */
    private final UnsafeBufferSerializer unsafeSerializer = new UnsafeBufferSerializer();

    /** Idle strategy when retrying due to admin action */
    private final BusySpinIdleStrategy adminActionRetryIdle = new BusySpinIdleStrategy();

    /** Lock of the class for synchronization */
    private final Object lock = new Object();

    /**
     * Create a new publisher instance given the context of the library and the parameters for the publisher
     * @param vegaContext context of the instance
     * @param params publisher parameters
     */
    ControlPublisher(final VegaContext vegaContext, final ControlPublisherParams params)
    {
        // Create the reusable base header
        this.reusableBaseHeader = new BaseHeader(MsgType.DATA, Version.LOCAL_VERSION);

        // Store the parameters
        this.params = params;

        // Create the aeron publisher channel
        final String publicationChannel =  AeronChannelHelper.createUnicastChannelString(params.getIpAddress(), params.getPort(), params.getSubnetAddress());

        log.info("Creating ControlPublisher with params [{}], channel [{}]", params, publicationChannel);

        // Create the aeron publisher
        this.publication = vegaContext.getAeron().addPublication(publicationChannel, params.getStreamId());

        // Prepare the reusable send buffer
        this.unsafeSerializer.wrap(ByteBuffer.allocate(WRITE_BUFFER_SIZE));
    }

    @Override
    public void close()
    {
        synchronized (this.lock)
        {
            if (this.publication.isClosed())
            {
                return;
            }

            log.info("Closing ControlPublisher with params [{}]", this.params.toString());

            // Close the publisher
            this.publication.close();
        }
    }

    /**
     * Return true if the control publisher has been closed
     *
     * @return true if the publisher has been closed
     */
    public boolean isClosed()
    {
        synchronized (this.lock)
        {
            return this.publication.isClosed();
        }
    }

    /**
     * Send a message of the given message type through the publisher
     *
     * @param msgType the type of the message to send
     * @param message the buffer containing the message in binary form
     * @param offset where the message starts in the buffer
     * @param length the lenght of the message
     *               
     * @return the result of the sending process
     */
    public PublishResult sendMessage(final byte msgType, final DirectBuffer message, final int offset, final int length)
    {
        synchronized (this.lock)
        {
            // If closed return OK. It may happen during an un-subscription
            if (this.publication.isClosed())
            {
                return PublishResult.OK;
            }

            // Set the message type in the base header
            this.reusableBaseHeader.setMsgType(msgType);

            // Calculate required size to sendMsg
            final int totalMsgSize = this.reusableBaseHeader.serializedSize() + length;

            if (log.isTraceEnabled())
            {
                log.trace("Sending message of type [{}], contents size [{}], total size [{}]", MsgType.toString(msgType), length, totalMsgSize);
            }

            return this.sendUsingOffer(message, offset, length, totalMsgSize);
        }
    }

    /**
     * Send a message . The message will be sent using the offer mechanism.
     *
     * @param message the buffer with the message to send
     * @param msgOffset offset in the buffer were the user message starts
     * @param userMsgSize size of the user message part without the headers
     * @param totalMsgSize total size of the message including the headers
     *
     * @return the result of the message send
     */
    private PublishResult sendUsingOffer(final DirectBuffer message, final int msgOffset, final int userMsgSize, final int totalMsgSize)
    {
        // Reset the buffer offset
        this.unsafeSerializer.setOffset(0);

        // Write base header
        this.reusableBaseHeader.toBinary(this.unsafeSerializer);

        // Write the user message
        this.unsafeSerializer.writeBytes(message, msgOffset, userMsgSize);

        // Send the message
        long offerResult = this.publication.offer(this.unsafeSerializer.getInternalBuffer(), 0, totalMsgSize);

        // Retry in case of admin action
        while (offerResult == Publication.ADMIN_ACTION)
        {
            // Wait a bit to avoid too many retries
            this.adminActionRetryIdle.idle();

            // Retry the send
            offerResult = this.publication.offer(this.unsafeSerializer.getInternalBuffer(), 0, totalMsgSize);
        }

        // Convert the result
        return PublishResult.fromAeronResult(offerResult);
    }
}  