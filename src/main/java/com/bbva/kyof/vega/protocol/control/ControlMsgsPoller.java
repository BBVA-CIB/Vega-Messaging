package com.bbva.kyof.vega.protocol.control;

import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.msg.*;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.threads.RecurrentTask;
import io.aeron.FragmentAssembler;
import io.aeron.logbuffer.Header;
import lombok.extern.slf4j.Slf4j;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.util.UUID;

/**
 * Class that perform message polling for control messages on the instance
 *
 * This class is thread safe!!
 */
@Slf4j
class ControlMsgsPoller extends RecurrentTask
{
    /** Reusable base header for received messages */
    private final BaseHeader reusableBaseHeader = new BaseHeader();

    /** Reusable security request message */
    private final MsgSecurityReq reusableSecurityReq = new MsgSecurityReq();

    /** Reusable security response message */
    private final MsgSecurityResp reusableSecurityResp = new MsgSecurityResp();

    /** Reusable security error response message */
    private final MsgSecurityErrorResp reusableSecurityErrorResp = new MsgSecurityErrorResp();

    /** Reusable buffer serializer */
    private final UnsafeBufferSerializer bufferSerializer = new UnsafeBufferSerializer();

    /** Aeron subscriber that is going to be polled */
    private final ControlSubscriber subscriber;

    /** Fragment assembler that will handle the new messages */
    private final FragmentAssembler fragmentAssembler;

    /** Listener for received security requests */
    private final ISecurityRequestListener secReqListener;

    /** Listener for received security responses */
    private final ISecurityResponseListener secRespListener;

    /** Store the vega instance id the poller belongs to */
    private final UUID ownInstanceId;

    /**
     * Create a new poller for control messages
     *
     * @param subscriber subscriber that will receive the control messages
     * @param secReqListener listener that will process the security requests messages
     * @param secRespListener listener that will process the security response messages
     * @param ownInstanceId the instance id of the vega instance the poller belongs to
     */
    ControlMsgsPoller(final ControlSubscriber subscriber, final ISecurityRequestListener secReqListener, final ISecurityResponseListener secRespListener, final UUID ownInstanceId)
    {
        // 1 Millisecond idle strategy, the control messages are not in the critical path
        super(new SleepingMillisIdleStrategy(1));
        this.subscriber = subscriber;
        this.secReqListener = secReqListener;
        this.secRespListener = secRespListener;
        this.ownInstanceId = ownInstanceId;
        this.fragmentAssembler = new FragmentAssembler(this::processAeronMsg);
    }

    /**
     * Start the poller
     */
    void start()
    {
        log.info("Starting control messages poller for instance {}", this.ownInstanceId);
        this.start("ControlMsgsPoller_" + this.ownInstanceId);
    }

    @Override
    public int action()
    {
       return this.subscriber.poll(this.fragmentAssembler, 1);
    }

    @Override
    public void cleanUp()
    {
        // Nothing to do
    }

    /**
     * Process a polled message from the control messages subscriber
     *
     * @param buffer the buffer that contains the message
     * @param offset the offset where the message starts on the buffer
     * @param length the length of the message
     * @param header the Aeron header of the message
     */
    private void processAeronMsg(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        try
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
                case MsgType.CONTROL_SECURITY_REQ:
                    this.processSecurityReq();
                    break;
                case MsgType.CONTROL_SECURITY_RESP:
                    this.processSecurityResp();
                    break;
                case MsgType.CONTROL_SECURITY_ERROR_RESP:
                    this.processSecurityErrorResp();
                    break;
                default:
                    log.warn("Unexpected message type received [{}], control message expected", this.reusableBaseHeader.getMsgType());
                    break;
            }
        }
        catch (final RuntimeException e)
        {
            log.error("Unexpected exception processing received control message", e);
        }
    }

    /** Process a security request whose content is already in the reusable buffer and whose header has been processed */
    private void processSecurityReq()
    {
        // Extract the message
        this.reusableSecurityReq.fromBinary(this.bufferSerializer);

        if (log.isTraceEnabled())
        {
            log.trace("Security request message received. {}", this.reusableSecurityReq);
        }

        // Verify that the message is for the current instance, ignore in other case
        if (!this.reusableSecurityReq.getTargetVegaInstanceId().equals(this.ownInstanceId))
        {
            return;
        }

        // Notify the listener
        this.secReqListener.onSecurityRequestReceived(this.reusableSecurityReq);
    }

    /** Process a security response whose content is already in the reusable buffer and whose header has been processed */
    private void processSecurityResp()
    {
        // Extract the message
        this.reusableSecurityResp.fromBinary(this.bufferSerializer);

        if (log.isTraceEnabled())
        {
            log.trace("Security response message received. {}", this.reusableSecurityResp);
        }

        // Verify that the message is for the current instance, ignore in other case
        if (!this.reusableSecurityResp.getTargetVegaInstanceId().equals(this.ownInstanceId))
        {
            return;
        }

        // Notify the listener
        this.secRespListener.onSecuirtyResponseReceived(this.reusableSecurityResp);
    }

    /** Process a security error response whose content is already in the reusable buffer and whose header has been processed */
    private void processSecurityErrorResp()
    {
        // Extract the message
        this.reusableSecurityErrorResp.fromBinary(this.bufferSerializer);

        if (log.isTraceEnabled())
        {
            log.trace("Security error response message received. {}", this.reusableSecurityErrorResp);
        }

        // Verify that the message is for the current instance, ignore in other case
        if (!this.reusableSecurityErrorResp.getTargetVegaInstanceId().equals(this.ownInstanceId))
        {
            return;
        }

        // Notify the listener
        this.secRespListener.onSecurityErrorResponseReceived(this.reusableSecurityErrorResp);
    }
}
