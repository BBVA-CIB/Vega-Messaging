package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.msg.IResponseListener;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.msg.PublishResult;
import com.bbva.kyof.vega.msg.SentRequest;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.heartbeat.HeartbeatController;
import com.bbva.kyof.vega.protocol.heartbeat.HeartbeatParameters;
import com.bbva.kyof.vega.protocol.heartbeat.IClientConnectionListener;
import com.bbva.kyof.vega.protocol.heartbeat.IHeartbeatSender;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.Closeable;
import java.util.Random;
import java.util.UUID;

/**
 * Base class for topic publisher implementations.
 *
 * The TopicPublisher is the class that conglomerates all the functionality to publish messages for a topicName
 *
 * Each publisher belongs to a single topicName and handles the sending of messages to all the "aeron publishers" associated to that topicName.
 *
 * The class is thread-safe
 */
@Slf4j
abstract class AbstractTopicPublisher implements ITopicPublisher, IHeartbeatSender, Closeable
{
    /** Topic name represented by the topic publisher */
    @Getter private final String topicName;

    /** Return the topic configuration for this publisher */
    @Getter private final TopicTemplateConfig topicConfig;

    /** Context of the instance */
    private final VegaContext vegaContext;

    /** Unique id of the topic publisher */
    @Getter private final UUID uniqueId = UUID.randomUUID();
    
    /** Sequence number of the message or request, it will increment for each sent message */
    @Getter private long sequenceNumber = 0;
    
    /** Random number generator for request id's */
    private final Random rnd = new Random(System.nanoTime());

    /** Content for a sent heartbeat request */
    private final UnsafeBuffer heartbeatContent = new UnsafeBuffer(new byte[0]);

    /** True if the topic publisher has been closed */
    private boolean closed = false;

    /** Heartbeat controller */
    private HeartbeatController heartBeatController = null;

    /** Lock for concurrent access */
    protected final Object lock = new Object();

    /**
     * Constructor of the class
     *
     * @param topicName Topic name that is going to sendMsg
     * @param topicConfig Topic configuration
     * @param vegaContext library instance configuration
     */
    AbstractTopicPublisher(final String topicName, final TopicTemplateConfig topicConfig, final VegaContext vegaContext)
    {
        this.topicName = topicName;
        this.topicConfig = topicConfig;
        this.vegaContext = vegaContext;
    }

    @Override
    public PublishResult sendMsg(final DirectBuffer message, final int offset, final int length)
    {
        synchronized (this.lock)
        {
            if (log.isTraceEnabled())
            {
                log.trace("Sending data message. TopicName [{}]. TopicId [{}]", this.topicName, this.uniqueId);
            }

            // Make sure the publisher has not been closed
            if (this.closed)
            {
                log.error("Error, trying to send a message on a closed publisher on topicName [{}]", this.topicName);
                return PublishResult.UNEXPECTED_ERROR;
            }

            // Add a unit to the sequence number
            this.sequenceNumber = this.sequenceNumber + 1;

            return this.sendToAeron(message, this.sequenceNumber, offset, length);
        }
    }

    @Override
    public SentRequest sendRequest(final DirectBuffer message, final int offset, final int length, final long timeout, final IResponseListener respListener)
    {
        synchronized (this.lock)
        {
            return this.sendRequest(MsgType.DATA_REQ, message, offset, length, timeout, respListener);
        }
    }

    @Override
    public void sendHeartbeat(final IResponseListener responseListener, final long timeout)
    {
        synchronized (this.lock)
        {
            this.sendRequest(MsgType.HEARTBEAT_REQ, this.heartbeatContent, 0, 0, timeout, responseListener);
        }
    }

    /**
     * Send a request on the topic.
     *
     * It will send the provided offset and length to get the contents in the buffer to send
     *
     * Try to reuse buffers if possible to reduce memory allocation.
     *
     * @param msgType framework type of message to send
     * @param message The request message to send
     * @param offset Offset for the message start in the buffer
     * @param length Length of the message starting in the given offset
     * @param timeout the timeout of the request in milliseconds
     * @param respListener Listener for responses on the request. If null no responses will be processed.
     *
     * @return an object that represent the sent request, containing the request ID and other useful information like the publish result
     */
    private SentRequest sendRequest(final byte msgType, final DirectBuffer message, final int offset, final int length, final long timeout, final IResponseListener respListener)
    {
        // Create the request object
        final SentRequest request = new SentRequest(this.topicName, timeout, respListener, this.rnd);

        if (log.isTraceEnabled())
        {
            log.trace("Sending request message. TopicName [{}]. TopicId [{}]. RequestId [{}]", this.topicName, this.uniqueId, request.getRequestId());
        }

        // Make sure the publisher has not been closed
        if (this.closed)
        {
            log.error("Error, trying to send a request on a closed publisher on topicName [{}]", this.topicName);
            request.setSentResult(PublishResult.UNEXPECTED_ERROR);
            return request;
        }

        // Add to the request manager
        this.vegaContext.getAsyncRequestManager().addNewRequest(request);

        // Add a unit to the sequence number
        this.sequenceNumber = this.sequenceNumber + 1;

        // Send the request to all the internal Aeron publishers
        request.setSentResult(this.sendRequestToAeron(msgType, request.getRequestId(), message, this.sequenceNumber, offset, length));

        return request;
    }

    @Override
    public void activateHeartbeats(@NonNull final IClientConnectionListener listener, @NonNull final HeartbeatParameters parameters)
    {
        synchronized (this.lock)
        {
            if (this.closed)
            {
                log.warn("Trying to activate heartbeats on a closed publisher on topicName [{}]", this.topicName);
                return;
            }

            // If the heartbeat controller is active, stop it
            if (this.heartBeatController != null)
            {
                this.heartBeatController.stop();
            }

            // Create a new controller
            this.heartBeatController = new HeartbeatController(this.vegaContext.getHeartbeatsTimer(), this.topicName, this, listener, parameters);
        }
    }

    @Override
    public void deactivateHeartbeats()
    {
        synchronized (this.lock)
        {
            if (this.heartBeatController == null)
            {
                log.warn("Heartbeats are not active for topicName [{}]. Ignoring call...", this.topicName);
            }
            else
            {
                log.info("Stopping Heartbeats on topicName [{}]", this.topicName);
                this.heartBeatController.stop();
                this.heartBeatController = null;
            }
        }
    }

    @Override
    public boolean isHeartbeatsActive()
    {
        synchronized (this.lock)
        {
            return this.heartBeatController != null;
        }
    }

    @Override
    public void close()
    {
        synchronized (this.lock)
        {
            // Deactivate hearbeats if active
            if (this.heartBeatController != null)
            {
                this.deactivateHeartbeats();
            }

            // Set as closed and clean internal information
            this.closed = true;
            this.cleanAeronPublishers();
        }
    }

    /**
     * True if the topic is configured to use security
     */
    abstract boolean hasSecurity();

    /**
     * Return the topic security configuration, null if not secure
     */
    public abstract TopicSecurityTemplateConfig getTopicSecurityConfig();

    /**
     * Send message to all the AeronPublishers related to the topic
     *
     * @param message the message to send
     * @param sequenceNumber the sequence number of the message
     * @param offset message offset in the byte buffer
     * @param length message length starting from the offset
     * @return the result of the send process
     */
    abstract PublishResult sendToAeron(DirectBuffer message, long sequenceNumber, int offset, int length);

    /**
     * Send request to all the AeronPublishers related to the topic
     *
     * @param msgType type of the message to send
     * @param requestId the unique id of the request object
     * @param message the message to send
     * @param sequenceNumber the sequence number of the message
     * @param offset message offset in the byte buffer
     * @param length message length starting from the offset
     * @return the result of the send process
     */
    abstract PublishResult sendRequestToAeron(byte msgType, UUID requestId, DirectBuffer message, long sequenceNumber, int offset, int length);

    /**
     * Clean related AeronPublishers information. Don't close the aeron publishers, just cleanAfterClose references.
     */
    protected abstract void cleanAeronPublishers();
}
