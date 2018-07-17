package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.msg.*;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.agrona.DirectBuffer;

import java.io.Closeable;
import java.io.IOException;

/**
 * Simple receiver to get and process messages
 */
@Slf4j
public class SimpleReceiver implements Closeable
{
    /** Reusable base header for received messages */
    @Getter private BaseHeader reusableBaseHeader = new BaseHeader();

    /** Reusable header for received data messages */
    @Getter private MsgDataHeader reusableDataMsgHeader = new MsgDataHeader();

    /** Reusable header for received request */
    @Getter private MsgReqHeader reusableReqMsgHeader = new MsgReqHeader();

    /** Reusable header for received responses */
    @Getter private MsgRespHeader reusableRespMsgHeader = new MsgRespHeader();

    /** Reusable message object for received messages */
    @Getter private RcvMessage reusableReceivedMsg = new RcvMessage();

    /** Reusable message object for received requests */
    @Getter private RcvRequest reusableReceivedRequest = new RcvRequest();

    /** Reusable message object for received responses */
    @Getter private RcvResponse reusableReceivedResponse = new RcvResponse();

    /** Reusable buffer serializer */
    private final UnsafeBufferSerializer bufferSerializer = new UnsafeBufferSerializer();

    private final Subscription subscription;

    SimpleReceiver(final Aeron aeron, final TransportMediaType type, final String ip, final int port, final int stream, final SubnetAddress subnet)
    {
        switch (type)
        {
            case UNICAST:
                subscription = aeron.addSubscription(AeronChannelHelper.createUnicastChannelString(ip, port, subnet), stream);
                break;
            case MULTICAST:
                subscription = aeron.addSubscription(AeronChannelHelper.createMulticastChannelString(ip, port, subnet), stream);
                break;
            case IPC:
                subscription = aeron.addSubscription(AeronChannelHelper.createIpcChannelString(), stream);
                break;
            default:
                subscription = null;
        }

        log.info("Subscription created for channel {} and stream {}", subscription.channel(), subscription.streamId());
    }

    @Override
    public void close()
    {
        subscription.close();
    }

    int pollReceivedMessage()
    {
        return subscription.poll(new FragmentAssembler(this::processRcvMessage), Integer.MAX_VALUE);
    }

    private void processRcvMessage(final DirectBuffer buffer, final int offset, final int length, final Header aeronHeader)
    {
        // Wrap the buffer into the serializer
        this.bufferSerializer.wrap(buffer, offset, length);

        // Read the base header
        this.reusableBaseHeader.fromBinary(this.bufferSerializer);

        // Check the message type and process
        switch (this.reusableBaseHeader.getMsgType())
        {
            case MsgType.DATA:
                this.processDataMessage();
                break;
            case MsgType.ENCRYPTED_DATA:
                this.processDataMessage();
                break;
            case MsgType.DATA_REQ:
                this.processDataRequestMessage();
                break;
            case MsgType.RESP:
                this.processDataResponseMessage();
                break;
        }
    }

    /** Process a message of type data that has already been wrapped on the buffer serializer */
    private void processDataMessage()
    {
        // Deserialize the header to get the id of the publisher that sent the message
        this.reusableDataMsgHeader.fromBinary(this.bufferSerializer);

        // Set the fields of the reusable received msg
        this.reusableReceivedMsg.setInstanceId(this.reusableDataMsgHeader.getInstanceId());
        this.reusableReceivedMsg.setTopicPublisherId(this.reusableDataMsgHeader.getTopicPublisherId());
        this.reusableReceivedMsg.setSequenceNumber(this.reusableDataMsgHeader.getSequenceNumber());
        this.reusableReceivedMsg.setUnsafeBufferContent(this.bufferSerializer.getInternalBuffer());
        this.reusableReceivedMsg.setContentOffset(this.bufferSerializer.getOffset());
        this.reusableReceivedMsg.setContentLength(this.bufferSerializer.getMsgLength() - this.bufferSerializer.getOffset());
    }

    /** Process a message of type data response that has already been wrapped on the buffer serializer */
    private void processDataResponseMessage()
    {
        // Deserialize the header to get the id of the publisher that sent the message
        this.reusableRespMsgHeader.fromBinary(this.bufferSerializer);

        // Set the fields of the reusable received request
        this.reusableReceivedResponse.setInstanceId(this.reusableRespMsgHeader.getInstanceId());
        this.reusableReceivedResponse.setOriginalRequestId(this.reusableRespMsgHeader.getRequestId());
        this.reusableReceivedResponse.setUnsafeBufferContent(this.bufferSerializer.getInternalBuffer());
        this.reusableReceivedResponse.setContentOffset(this.bufferSerializer.getOffset());
        this.reusableReceivedResponse.setContentLength(this.bufferSerializer.getMsgLength() - this.bufferSerializer.getOffset());
    }

    /** Process a message of type data request that has already been wrapped on the buffer serializer */
    private void processDataRequestMessage()
    {
        // Deserialize the header to get the id of the publisher that sent the message
        this.reusableReqMsgHeader.fromBinary(this.bufferSerializer);

        // Set the fields of the reusable received request
        this.reusableReceivedRequest.setInstanceId(this.reusableReqMsgHeader.getInstanceId());
        this.reusableReceivedRequest.setTopicPublisherId(this.reusableReqMsgHeader.getTopicPublisherId());
        this.reusableReceivedRequest.setRequestId(this.reusableReqMsgHeader.getRequestId());
        this.reusableReceivedRequest.setSequenceNumber(this.reusableReqMsgHeader.getSequenceNumber());
        this.reusableReceivedRequest.setUnsafeBufferContent(this.bufferSerializer.getInternalBuffer());
        this.reusableReceivedRequest.setContentOffset(this.bufferSerializer.getOffset());
        this.reusableReceivedRequest.setContentLength(this.bufferSerializer.getMsgLength() - this.bufferSerializer.getOffset());
    }

    public void reset()
    {
        reusableBaseHeader = new BaseHeader();
        reusableDataMsgHeader = new MsgDataHeader();
        reusableReqMsgHeader = new MsgReqHeader();
        reusableRespMsgHeader = new MsgRespHeader();
        reusableReceivedMsg = new RcvMessage();
        reusableReceivedRequest = new RcvRequest();
        reusableReceivedResponse = new RcvResponse();
    }
}
