package com.bbva.kyof.vega.autodiscovery.daemon;

import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscDaemonClientInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.msg.BaseHeader;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import com.bbva.kyof.vega.serialization.IUnsafeSerializable;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import com.bbva.kyof.vega.util.threads.RecurrentTask;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.SleepingIdleStrategy;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Created by cnebrera on 08/08/16.
 */
@Slf4j
public class UnicastDaemonClientSimulator extends RecurrentTask
{
    /** Reusable buffer serializer for the incomming messages */
    private final UnsafeBufferSerializer rcvBufferSerializer = new UnsafeBufferSerializer();

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(1024);
    private final UnsafeBufferSerializer sendBufferSerializer = new UnsafeBufferSerializer();

    @Getter private final UUID uniqueId = UUID.randomUUID();
    @Getter private final AutoDiscDaemonClientInfo clientInfo;

    private List<AutoDiscTopicInfo> receivedTopicInfoMsgs = Collections.synchronizedList(new LinkedList<>());

    private final Publication publication;
    private final Subscription subscription;

    public UnicastDaemonClientSimulator(final Aeron aeron,
                                        final String ip,
                                        int subPort,
                                        int pubPort,
                                        final int streamId,
                                        final SubnetAddress subnetAddress)
    {
        super(new SleepingIdleStrategy(TimeUnit.MILLISECONDS.toNanos(1)));

        final String subChannel = AeronChannelHelper.createUnicastChannelString(ip, subPort, subnetAddress);
        final String pubChannel = AeronChannelHelper.createUnicastChannelString(ip, pubPort, subnetAddress);

        log.info("Creating subscription on channel {} and stream {}", subChannel, streamId);
        this.subscription = aeron.addSubscription(subChannel, streamId);
        log.info("Creating publications on channel {} and stream {}", pubChannel, streamId);
        this.publication = aeron.addPublication(pubChannel, streamId);

        this.clientInfo = new AutoDiscDaemonClientInfo(this.uniqueId, InetUtil.convertIpAddressToInt(ip), subPort, streamId);
    }

    @Override
    public int action()
    {
        return this.subscription.poll(this::processRcvMsg, 1);
    }

    @Override
    public void cleanUp()
    {
        this.subscription.close();
    }

    private void processRcvMsg(final DirectBuffer buffer, final int offset, final int length, final Header aeronHeader)
    {
        try
        {
            // Wrap the buffer into the serializer
            this.rcvBufferSerializer.wrap(buffer, offset, length);

            // Read the base header
            final BaseHeader baseHeader = new BaseHeader();
            baseHeader.fromBinary(this.rcvBufferSerializer);

            // Check the message type
            switch (baseHeader.getMsgType())
            {
                case MsgType.AUTO_DISC_INSTANCE:
                    break;
                case MsgType.AUTO_DISC_TOPIC_SOCKET:
                    break;
                case MsgType.AUTO_DISC_TOPIC:
                    this.onReceivedTopicInfoMsg();
                    break;
                default:
                    log.warn("Wrong message type [{}] received on autodiscovery", baseHeader.getMsgType());
                    break;
            }
        }
        catch (final Exception e)
        {
            log.error("Unexpected error processing received autodiscovery message", e);
        }
    }

    /**
     * Process a received message with information about a topic publisher or subscriber
     */
    private void onReceivedTopicInfoMsg()
    {
        // Deserialize the message
        final AutoDiscTopicInfo topicInfo = new AutoDiscTopicInfo();
        topicInfo.fromBinary(this.rcvBufferSerializer);

        receivedTopicInfoMsgs.add(topicInfo);
    }

    public int getNumRcvTopicInfoMsgs()
    {
        return receivedTopicInfoMsgs.size();
    }

    public AutoDiscTopicInfo getLastReceivedTopicInfoMsg()
    {
        return receivedTopicInfoMsgs.get(receivedTopicInfoMsgs.size() - 1);
    }

    public void sendClientInfo(final boolean wrongVersion)
    {
        this.sendMessage(MsgType.AUTO_DISC_DAEMON_CLIENT_INFO, this.clientInfo, wrongVersion);
    }

    public void sendMessage(final byte msgType, final IUnsafeSerializable serializable, boolean wrongVersion)
    {
        // Prepare the send buffer
        this.sendBuffer.clear();
        this.sendBufferSerializer.wrap(this.sendBuffer);

        // Set msg type and write the base header
        BaseHeader baseHeader;

        if (wrongVersion)
        {
            baseHeader = new BaseHeader(msgType, Version.toIntegerRepresentation((byte)55, (byte)3, (byte)1));
        }
        else
        {
            baseHeader = new BaseHeader(msgType, Version.LOCAL_VERSION);
        }

        // Write the base header
        baseHeader.toBinary(this.sendBufferSerializer);

        // Serialize the message
        serializable.toBinary(this.sendBufferSerializer);

        // Send the message
        long offerResult = publication.offer(this.sendBufferSerializer.getInternalBuffer(), 0, this.sendBufferSerializer.getOffset());

        log.info("Offer result {}", offerResult);
    }
}
