
package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.msg.*;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.serialization.IUnsafeSerializable;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Wrapper class to encapsulate an Aeron Publisher. <p>
 *
 * The objective of the class is to ensure thread safety, store all topics publishers related to the aeron publisher
 * and encapsulate the method to send messages, requests and responses. <p>
 *
 * The publisher will always try to use "bufferClaim" if possible in order to avoid copying the header and contents
 * into a new buffer for each message. When the messages are too big (close to 4k) this cannot be done and we need to
 * use a normal "offer" to send the message. <p>
 *
 * It also handles the life-cicle of the publisher.
 *
 * This class is thread safe!!
 */
@Slf4j
public class AeronPublisher implements IAeronPublisher, Closeable
{
    /** Aeron publication */
    private final Publication publication;

    /** Parameters of the publisher */
    @Getter private final AeronPublisherParams params;

    /** Reusable base header */
    private final BaseHeader reusableBaseHeader;

    /** Reusable data msg header */
    private final MsgDataHeader reusableMsgHeader;

    /** Reusable data request header */
    private final MsgReqHeader reusableMsgReqHeader;

    /** Reusable data response header */
    private final MsgRespHeader reusableMsgRespHeader;

    /** Reusable buffer claim to serialize messages, allowing adding header and user message contents without extra copies */
    private final BufferClaim bufferClaim = new BufferClaim();

    /** Reusable serializer for claim send type */
    private final UnsafeBufferSerializer claimUnsafeSerializer = new UnsafeBufferSerializer();

    /** Reusable serializer for offer send type */
    private final UnsafeBufferSerializer offerUnsafeSerializer = new UnsafeBufferSerializer();

    /** Idle strategy when retrying due to admin action */
    private final BusySpinIdleStrategy adminActionRetryIdle = new BusySpinIdleStrategy();

    /** Lock for synchronization of the instance */
    private final Object lock = new Object();

    /**
     * Create a new publisher instance given the context of the library and the parameters for the publisher
     * @param vegaContext context of the instance
     * @param params publisher parameters
     */
    public AeronPublisher(final VegaContext vegaContext, final AeronPublisherParams params)
    {
        // Create the reusable base header
        this.reusableBaseHeader = new BaseHeader(MsgType.DATA, Version.LOCAL_VERSION);

        // Create the reusable headers for all message types
        this.reusableMsgHeader = new MsgDataHeader();
        this.reusableMsgReqHeader = new MsgReqHeader();
        this.reusableMsgRespHeader = new MsgRespHeader();

        // Preset the unique instance id since it is always the same for all messages sent with this publisher
        this.reusableMsgHeader.setInstanceId(vegaContext.getInstanceUniqueId());
        this.reusableMsgReqHeader.setInstanceId(vegaContext.getInstanceUniqueId());
        this.reusableMsgRespHeader.setInstanceId(vegaContext.getInstanceUniqueId());

        // Store the parameters
        this.params = params;

        // Create the aeron publisher channel
        final String publicationChannel = this.createPublicationChannel(params);

        log.info("Creating AeronPublisher with params [{}], channel [{}]", params.toString(), publicationChannel);

        // Create the aeron publisher
        this.publication = vegaContext.getAeron().addPublication(publicationChannel, params.getStreamId());

        // Start the offer serializer buffer to twice the size of the max claim size
        this.offerUnsafeSerializer.wrap(ByteBuffer.allocate(this.publication.maxPayloadLength() * 2));
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

            log.info("Closing aeron publisher [{}]", this.params.toString());

            // Close the publisher
            this.publication.close();
        }
    }

    @Override
    public PublishResult sendMessage(final byte msgType, final UUID topicUniqueId, final DirectBuffer message, final int offset, final int length)
    {
        synchronized (this.lock)
        {
            // If closed return OK. It may happen during an un-subscription
            if (this.publication.isClosed())
            {
                return PublishResult.OK;
            }

            // Set the topic unique id of the reusable header, the rest of fields don't change between consecutive sends
            this.reusableMsgHeader.setTopicPublisherId(topicUniqueId);

            // Send the message
            return this.send(msgType, this.reusableMsgHeader, message, offset, length);
        }
    }

    @Override
    public PublishResult sendRequest(final byte msgType, final UUID topicUniqueId, final UUID requestId, final DirectBuffer message, final int offset, final int length)
    {
        synchronized (this.lock)
        {
            // If closed return OK. It may happen during an un-subscription
            if (this.publication.isClosed())
            {
                return PublishResult.OK;
            }

            // Set the request header fields
            this.reusableMsgReqHeader.setTopicPublisherId(topicUniqueId);
            this.reusableMsgReqHeader.setRequestId(requestId);

            // Send the message
            return this.send(msgType, this.reusableMsgReqHeader, message, offset, length);
        }
    }

    @Override
    public PublishResult sendResponse(final UUID requestId, final DirectBuffer message, final int offset, final int length)
    {
        synchronized (this.lock)
        {
            // If closed return OK. It may happen during an un-subscription
            if (this.publication.isClosed())
            {
                return PublishResult.OK;
            }

            // Set the request header fields
            this.reusableMsgRespHeader.setRequestId(requestId);

            // Send the message
            return this.send(MsgType.RESP, this.reusableMsgRespHeader, message, offset, length);
        }
    }

    /**
     * Send a message given the message type, header to use and contents
     * @param msgType the type of the message
     * @param header the header of the message depending on the type (normal, request, response)
     * @param message the buffer with the message to send
     * @param offset the offset in the buffer where the message to send starts
     * @param length the length of the message starting from the given offset
     *
     * @return the result of the message send
     */
    private PublishResult send(final byte msgType, final IUnsafeSerializable header, final DirectBuffer message, final int offset, final int length)
    {
        // Set the message type in the base header
        this.reusableBaseHeader.setMsgType(msgType);

        // Calculate required size to sendMsg
        final int totalMsgSize = this.reusableBaseHeader.serializedSize() + header.serializedSize() + length;

        if (log.isTraceEnabled())
        {
            log.trace("Sending message of type [{}], contents size [{}], total size [{}]", MsgType.toString(msgType), length, totalMsgSize);
        }

        // If the message is bigger than max claim size cannot use the claim mechanism
        if (totalMsgSize > this.publication.maxPayloadLength())
        {
            return this.sendUsingOffer(header, message, offset, length, totalMsgSize);
        }
        else
        {
            return this.sendUsingClaim(header, message, offset, length, totalMsgSize);
        }
    }

    /**
     * Send a message given the header to use and contents. The message will be sent using the offer mechanism.
     *
     * @param header the header of the message depending on the type (normal, request, response)
     * @param message the buffer with the message to send
     * @param userMsgOffset offset in the buffer were the user message starts
     * @param userMsgSize size of the user message part without the headers
     * @param totalMsgSize total size of the message including the headers
     *
     * @return the result of the message send
     */
    private PublishResult sendUsingOffer(final IUnsafeSerializable header, final DirectBuffer message, final int userMsgOffset, final int userMsgSize, final int totalMsgSize)
    {
        // Make sure the buffer is big enough to serialize the message, if not increase the size
        if (this.offerUnsafeSerializer.getInternalBuffer().capacity() < totalMsgSize)
        {
            this.offerUnsafeSerializer.wrap(ByteBuffer.allocate(totalMsgSize * 2));
        }
        else
        {
            // Reset the offer buffer offset to start again
            this.offerUnsafeSerializer.setOffset(0);
        }

        // Write base header
        this.reusableBaseHeader.toBinary(this.offerUnsafeSerializer);
        // Write header
        header.toBinary(this.offerUnsafeSerializer);
        // Write the user message
        this.offerUnsafeSerializer.writeBytes(message, userMsgOffset, userMsgSize);

        // Send the message
        long offerResult = this.publication.offer(this.offerUnsafeSerializer.getInternalBuffer(), 0, totalMsgSize);

        // Retry in case of admin action
        while (offerResult == Publication.ADMIN_ACTION)
        {
            // Wait a bit to avoid too many retries
            this.adminActionRetryIdle.idle();

            // Retry the send
            offerResult = this.publication.offer(this.offerUnsafeSerializer.getInternalBuffer(), 0, totalMsgSize);
        }

        // Convert the result
        return PublishResult.fromAeronResult(offerResult);
    }

    /**
     * Send a message given the header to use and contents. The message will be sent using the claim mechanism.
     *
     * @param header the header of the message depending on the type (normal, request, response)
     * @param message the buffer with the message to send
     * @param userMsgOffset offset in the buffer were the user message starts
     * @param userMsgSize size of the user message part without the headers
     * @param totalMsgSize total size of the message including the headers
     *
     * @return the result of the message send
     */
    private PublishResult sendUsingClaim(final IUnsafeSerializable header, final DirectBuffer message, final int userMsgOffset, final int userMsgSize, final int totalMsgSize)
    {
        // Reserve space in the publication.
        long claimResult = this.publication.tryClaim(totalMsgSize, this.bufferClaim);

        // Retry in case of admin action
        while (claimResult == Publication.ADMIN_ACTION)
        {
            // Wait a bit to avoid too many retries
            this.adminActionRetryIdle.idle();

            // Retry the send
            claimResult = this.publication.tryClaim(totalMsgSize, this.bufferClaim);
        }

        // Claim success, write the data and sendMsg
        if (claimResult > 0)
        {
            // Wrap the buffer claim to serialize
            this.claimUnsafeSerializer.wrap(this.bufferClaim.buffer(), this.bufferClaim.offset(), totalMsgSize);
            // Write base header
            this.reusableBaseHeader.toBinary(this.claimUnsafeSerializer);
            // Write header
            header.toBinary(this.claimUnsafeSerializer);
            // Write the user message
            this.claimUnsafeSerializer.writeBytes(message, userMsgOffset, userMsgSize);
            // Send the message
            this.bufferClaim.commit();
            // Return Ok in the publication
            return PublishResult.OK;
        }

        // Convert the result
        return PublishResult.fromAeronResult(claimResult);
    }

    /**
     * Create the publication channel using the given parameters for the Aeron Publisher
     * @param params parameters for the publisher
     * @return the String representation of the channel
     */
    private String createPublicationChannel(final AeronPublisherParams params)
    {
        // Create the publication channel string
        switch (params.getTransportType())
        {
            case UNICAST:
                return AeronChannelHelper.createUnicastChannelString(params.getIpAddress(), params.getPort(), params.getSubnetAddress());
            case MULTICAST:
                return AeronChannelHelper.createMulticastChannelString(params.getIpAddress(), params.getPort(), params.getSubnetAddress());
            case IPC:
                return AeronChannelHelper.createIpcChannelString();
            default:
                return null;
        }
    }

    /**
     * Convert the given offer or claim result to an String value to print in the log
     * @param result the result to convert
     * @return the converted result
     */
    static String claimOfferResultToString(final long result)
    {
        if (result >= 0)
        {
            return Long.toString(result);
        }
        else if (result == Publication.CLOSED)
        {
            return "CLOSED";
        }
        else if (result == Publication.ADMIN_ACTION)
        {
            return "ADMIN_ACTION";
        }
        else if (result == Publication.BACK_PRESSURED)
        {
            return "BACK_PRESSURED";
        }
        else if (result == Publication.NOT_CONNECTED)
        {
            return "NOT_CONNECTED";
        }
        else
        {
            return "UNKNOWN";
        }
    }
}  