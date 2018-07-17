package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.config.general.RcvPollerConfig;
import com.bbva.kyof.vega.msg.*;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.collection.DelayedChangesArray;
import com.bbva.kyof.vega.util.collection.IDelayedChangesArray;
import com.bbva.kyof.vega.util.threads.RecurrentTask;
import io.aeron.FragmentAssembler;
import io.aeron.logbuffer.Header;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.agrona.DirectBuffer;

/**
 * Class that perform message polling over a set of subscribers
 *
 * This class is thread safe!!
 */
@Slf4j
class SubcribersPoller extends RecurrentTask
{
    /** Default initial subscribers number, it is used to reserve some memory in the subscribers array */
    private static final int DEFAULT_SUB_NUMBER = 10;

    /** Reusable base header for received messages */
    private final BaseHeader reusableBaseHeader = new BaseHeader();

    /** Reusable header for received data messages */
    private final MsgDataHeader reusableDataMsgHeader = new MsgDataHeader();

    /** Reusable header for received request */
    private final MsgReqHeader reusableReqMsgHeader = new MsgReqHeader();

    /** Reusable header for received responses */
    private final MsgRespHeader reusableRespMsgHeader = new MsgRespHeader();

    /** Reusable message object for received messages */
    private final RcvMessage reusableReceivedMsg = new RcvMessage();

    /** Reusable message object for received requests */
    private final RcvRequest reusableReceivedRequest = new RcvRequest();

    /** Reusable message object for received responses */
    private final RcvResponse reusableReceivedResponse = new RcvResponse();

    /** Reusable buffer serializer */
    private final UnsafeBufferSerializer bufferSerializer = new UnsafeBufferSerializer();

    /** Delayed changes collection with all subscriptors for the poller */
    private final IDelayedChangesArray<AeronSubscriber> subscribers;
  
    /** Fragment assembler that will handle the new messages in the subscribers */
    private final FragmentAssembler fragmentAssembler;

    /** Listener for received messages */
    private final ISubscribersPollerListener listener;

    /** Stores the configuration of the poller */
    @Getter private final RcvPollerConfig config;

    /**
     * Store the maximum number of fragments to look for when polling a single subscriber.
     * Each fragment is around 4k. Since we are using a fragment assembler, there is no need to define
     * a big number for big messages. It only make sense to change it if you want to poll more than one
     * message at a time.
     */
    private final int maxFragmentsPerPoll;

    /**
     * Create a new poller
     *
     * @param listener listener that will receive the polled messages
     * @param config poller configuration
     */
    SubcribersPoller(final ISubscribersPollerListener listener, final RcvPollerConfig config)
    {
        super(config.getIdleStrategy());
        this.listener = listener;
        this.config = config;
        this.fragmentAssembler = new FragmentAssembler(this::processAeronMsg);
        this.subscribers = new DelayedChangesArray<>(AeronSubscriber.class, DEFAULT_SUB_NUMBER);
        this.maxFragmentsPerPoll = config.getMaxFragmentsPerPoll();
    }

    /**
     * Add a subscription to be processed during polling
     * @param subscription the aeron subscription to poll
     */
    void addSubscription(final AeronSubscriber subscription)
    {
        this.subscribers.addElement(subscription);
    }

    /**
     * Remove a subscription from the poller
     * @param subscription the subscription to remove from the poller
     */
    void removeSubscription(final AeronSubscriber subscription)
    {
        this.subscribers.removeElement(subscription);
    }

    /**
     * Start the poller
     */
    void start()
    {
        log.info("Starting poller manager with name [{}]", this.config.getName());
        this.start("SubscriberPoller " + this.config.getName());
    }

    @Override
    public int action()
    {
        // Apply pending changes
        this.subscribers.applyPendingChanges();

        int fragmentsRead = 0;

        // Get the internal collection
        final AeronSubscriber[] subscriptionsArray = this.subscribers.getInternalArray();

        // Poll all the subscribers
        for (int i = 0; i < this.subscribers.getNumElements() && !this.shouldStop(); i++)
        {
            fragmentsRead += subscriptionsArray[i].poll(this.fragmentAssembler, this.maxFragmentsPerPoll);
        }

        // Return number of read fragments
        return fragmentsRead;
    }

    @Override
    public void cleanUp()
    {
        log.info("Cleaning poller manager [{}] after closing", this.config.getName());
        this.subscribers.clear();
    }

    /**
     * Process a polled message from one of the registered AeronSubscribers
     * @param buffer the buffer that contains the message
     * @param offset the offset where the message starts on the buffer
     * @param length the length of the message
     * @param header the Aeron header of the message
     */
    private void processAeronMsg(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        // Wrap the buffer into the serializer
        this.bufferSerializer.wrap(buffer, offset, length);

        // Read the base header
        this.reusableBaseHeader.fromBinary(this.bufferSerializer);

        // Check the version
        if (!this.reusableBaseHeader.isVersionCompatible())
        {
            log.warn("Message message received from incompatible library version [{}]", Version.toStringRep(this.reusableBaseHeader.getVersion()));
            return;
        }

        // Check the message type
        switch (this.reusableBaseHeader.getMsgType())
        {
            case MsgType.DATA:
                this.processDataMessage();
                break;
            case MsgType.DATA_REQ:
                this.processDataRequestMessage();
                break;
            case MsgType.HEARTBEAT_REQ:
                this.processHeartbeatRequestMessage();
                break;
            case MsgType.RESP:
                this.processDataResponseMessage();
                break;
            case MsgType.ENCRYPTED_DATA:
                this.processEncryptedDataMessage();
                break;
            default:
                log.warn("Unexpected message type received [{}]", this.reusableBaseHeader.getMsgType());
                break;
        }
    }

    /** Process a message of type data that has already been wrapped on the buffer serializer */
    private void processDataMessage()
    {
        if (log.isTraceEnabled())
        {
            log.trace("Data message received");
        }

        // Deserialize the header to get the id of the publisher that sent the message
        this.reusableDataMsgHeader.fromBinary(this.bufferSerializer);

        // Set the fields of the reusable received msg
        this.reusableReceivedMsg.setInstanceId(this.reusableDataMsgHeader.getInstanceId());
        this.reusableReceivedMsg.setTopicPublisherId(this.reusableDataMsgHeader.getTopicPublisherId());
        this.reusableReceivedMsg.setSequenceNumber(this.reusableDataMsgHeader.getSequenceNumber());
        this.reusableReceivedMsg.setUnsafeBufferContent(this.bufferSerializer.getInternalBuffer());
        this.reusableReceivedMsg.setContentOffset(this.bufferSerializer.getOffset());
        this.reusableReceivedMsg.setContentLength(this.bufferSerializer.getMsgLength() - this.bufferSerializer.getOffset());

        this.listener.onDataMsgReceived(this.reusableReceivedMsg);
    }

    /** Process a message of type data that has already been wrapped on the buffer serializer */
    private void processEncryptedDataMessage()
    {
        if (log.isTraceEnabled())
        {
            log.trace("Encrypted data message received");
        }

        // Deserialize the header to get the id of the publisher that sent the message
        this.reusableDataMsgHeader.fromBinary(this.bufferSerializer);

        // Set the fields of the reusable received msg
        this.reusableReceivedMsg.setInstanceId(this.reusableDataMsgHeader.getInstanceId());
        this.reusableReceivedMsg.setTopicPublisherId(this.reusableDataMsgHeader.getTopicPublisherId());
        this.reusableReceivedMsg.setUnsafeBufferContent(this.bufferSerializer.getInternalBuffer());
        this.reusableReceivedMsg.setSequenceNumber(this.reusableDataMsgHeader.getSequenceNumber());
        this.reusableReceivedMsg.setContentOffset(this.bufferSerializer.getOffset());
        this.reusableReceivedMsg.setContentLength(this.bufferSerializer.getMsgLength() - this.bufferSerializer.getOffset());

        this.listener.onEncryptedDataMsgReceived(this.reusableReceivedMsg);
    }

    /** Process a message of type data response that has already been wrapped on the buffer serializer */
    private void processDataResponseMessage()
    {
        if (log.isTraceEnabled())
        {
            log.trace("Response message received");
        }

        // Deserialize the header to get the id of the publisher that sent the message
        this.reusableRespMsgHeader.fromBinary(this.bufferSerializer);

        // Set the fields of the reusable received request
        this.reusableReceivedResponse.setInstanceId(this.reusableRespMsgHeader.getInstanceId());
        this.reusableReceivedResponse.setOriginalRequestId(this.reusableRespMsgHeader.getRequestId());
        this.reusableReceivedResponse.setUnsafeBufferContent(this.bufferSerializer.getInternalBuffer());
        this.reusableReceivedResponse.setContentOffset(this.bufferSerializer.getOffset());
        this.reusableReceivedResponse.setContentLength(this.bufferSerializer.getMsgLength() - this.bufferSerializer.getOffset());

        this.listener.onDataResponseMsgReceived(this.reusableReceivedResponse);
    }

    /** Process a message of type data request that has already been wrapped on the buffer serializer */
    private void processDataRequestMessage()
    {
        if (log.isTraceEnabled())
        {
            log.trace("Request message received");
        }

        // Deserialize the header to get the id of the publisher that sent the message
        this.reusableReqMsgHeader.fromBinary(this.bufferSerializer);

        // Set the fields of the reusable received request
        this.reusableReceivedRequest.setInstanceId(this.reusableReqMsgHeader.getInstanceId());
        this.reusableReceivedRequest.setTopicPublisherId(this.reusableReqMsgHeader.getTopicPublisherId());
        this.reusableReceivedRequest.setSequenceNumber(this.reusableReqMsgHeader.getSequenceNumber());
        this.reusableReceivedRequest.setRequestId(this.reusableReqMsgHeader.getRequestId());
        this.reusableReceivedRequest.setUnsafeBufferContent(this.bufferSerializer.getInternalBuffer());
        this.reusableReceivedRequest.setContentOffset(this.bufferSerializer.getOffset());
        this.reusableReceivedRequest.setContentLength(this.bufferSerializer.getMsgLength() - this.bufferSerializer.getOffset());

        this.listener.onDataRequestMsgReceived(this.reusableReceivedRequest);
    }

    /** Process a message of type heartbeat request */
    private void processHeartbeatRequestMessage()
    {
        if (log.isTraceEnabled())
        {
            log.trace("Heartbeat request message received");
        }

        // Deserialize the header to get the id of the publisher that sent the message and the request id
        this.reusableReqMsgHeader.fromBinary(this.bufferSerializer);

        // Give the heartbeat request to the listener
        this.listener.onHeartbeatRequestMsgReceived(this.reusableReqMsgHeader.getInstanceId(), this.reusableReqMsgHeader.getRequestId());
    }
}
