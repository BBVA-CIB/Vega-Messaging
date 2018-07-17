package com.bbva.kyof.vega.autodiscovery.model;

import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.net.InetUtil;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Represents a pair of topic publisher + aeron publisher or topic subscriber + aeron subscriber. <p>
 * This means is a pair of "logic transport" = topic + "real transport" = socket.
 *
 * It has it's own unique id to prevent collisions in the system and contains the required information to start the proper
 * transport on the form of aeron publisher or subscriber and relate it with the proper topic publisher or subscriber.
 */
@Builder
@NoArgsConstructor
public class AutoDiscTopicSocketInfo implements IAutoDiscTopicInfo
{
    /**Constant for security id when the topic is a non secured one */
    public static final int NO_SECURED_CONSTANT = 0;

    /** Number of internal fields of type Integer */
    private static final int NUM_INT_FIELDS = 4;

    /** Number of internal fields of type UUID */
    private static final int NUM_UUID_FIELDS = 3;

    /** Serialized size for the members that have a fixed size */
    private static final int FIX_MEMBERS_SERIALIZED_SIZE = UnsafeBufferSerializer.BYTE_SIZE +
            UnsafeBufferSerializer.UUID_SIZE * NUM_UUID_FIELDS +
            UnsafeBufferSerializer.INT_SIZE * NUM_INT_FIELDS;

    /** Instance id the topic belongs to */
    @Getter private UUID instanceId;

    /** The transport type */
    @Getter private AutoDiscTransportType transportType;

    /** Unique for the instance, it is used for fast lookup when a new advert arrives */
    @Getter private UUID uniqueId;

    /** Topic name */
    @Getter private String topicName;

    /** Unique Id of the topic publisher or topic subscriber */
    @Getter private UUID topicId;

    /** Transport ipAddress (0 for ipc transport) */
    @Getter private int ipAddress;

    /** Transport port (0 for ipc transport) */
    @Getter private int port;

    /** Transport stream id */
    @Getter private int streamId;

    /** Security ID of the topic if security is activated, 0 if security is not active */
    @Getter private int securityId;

    /**
     * Create a new autodiscovery topic socket info instance with no security
     *
     * @param instanceId the instance id the topic socket belongs to
     * @param transportType transport type for the topic-socket
     * @param uniqueId the unique id for the topic socket information
     * @param topicName the name of the topic
     * @param topicId the unique id of the topic
     * @param ipAddress the ip address in integer representation
     * @param port the port number
     * @param streamId the stream id for the aeron connection
     */
    public AutoDiscTopicSocketInfo(final UUID instanceId,
                                   final AutoDiscTransportType transportType,
                                   final UUID uniqueId,
                                   final String topicName,
                                   final UUID topicId,
                                   final int ipAddress,
                                   final int port,
                                   final int streamId)
    {
        this(instanceId, transportType, uniqueId, topicName, topicId, ipAddress, port, streamId, NO_SECURED_CONSTANT);
    }

    /**
     * Create a new autodiscovery topic socket info instance
     *
     * @param instanceId the instance id the topic socket belongs to
     * @param transportType transport type for the topic-socket
     * @param uniqueId the unique id for the topic socket information
     * @param topicName the name of the topic
     * @param topicId the unique id of the topic
     * @param ipAddress the ip address in integer representation
     * @param port the port number
     * @param streamId the stream id for the aeron connection
     * @param securityId the topic security id, 0 if no secured
     */
    public AutoDiscTopicSocketInfo(final UUID instanceId,
                                   final AutoDiscTransportType transportType,
                                   final UUID uniqueId,
                                   final String topicName,
                                   final UUID topicId,
                                   final int ipAddress,
                                   final int port,
                                   final int streamId,
                                   final int securityId)
    {
        this.instanceId = instanceId;
        this.transportType = transportType;
        this.uniqueId = uniqueId;
        this.topicName = topicName;
        this.topicId = topicId;
        this.ipAddress = ipAddress;
        this.port = port;
        this.streamId = streamId;
        this.securityId = securityId;
    }

    /**
     * Return true if the topic has been configured with security
     * @return true if security is configured for the topic
     */
    public boolean hasSecurity()
    {
        return this.securityId != NO_SECURED_CONSTANT;
    }

    @Override
    public void fromBinary(final UnsafeBufferSerializer buffer)
    {
        this.instanceId = buffer.readUUID();
        this.transportType = AutoDiscTransportType.fromByte(buffer.readByte());
        this.uniqueId = buffer.readUUID();
        this.topicName = buffer.readString();
        this.topicId = buffer.readUUID();
        this.ipAddress = buffer.readInt();
        this.port = buffer.readInt();
        this.streamId = buffer.readInt();
        this.securityId = buffer.readInt();
    }

    @Override
    public void toBinary(final UnsafeBufferSerializer buffer)
    {
        buffer.writeUUID(this.instanceId);
        buffer.writeByte(this.transportType.getByteValue());
        buffer.writeUUID(this.uniqueId);
        buffer.writeString(this.topicName);
        buffer.writeUUID(this.topicId);
        buffer.writeInt(this.ipAddress);
        buffer.writeInt(this.port);
        buffer.writeInt(this.streamId);
        buffer.writeInt(this.securityId);
    }

    @Override
    public int serializedSize()
    {
        return FIX_MEMBERS_SERIALIZED_SIZE + UnsafeBufferSerializer.serializedSize(this.topicName);
    }

    @Override
    public boolean equals(final Object target)
    {
        if (this == target)
        {
            return true;
        }
        if (target == null || getClass() != target.getClass())
        {
            return false;
        }

        AutoDiscTopicSocketInfo that = (AutoDiscTopicSocketInfo) target;

        return uniqueId.equals(that.uniqueId);
    }

    @Override
    public int hashCode()
    {
        return uniqueId.hashCode();
    }


    @Override
    public String toString()
    {
        return "AutoDiscTopicSocketInfo{" +
                "transportType=" + transportType +
                ", uniqueId=" + uniqueId +
                ", topicName='" + topicName + '\'' +
                ", topicId=" + topicId +
                ", ipAddress=" + InetUtil.convertIntToIpAddress(ipAddress) +
                ", port=" + port +
                ", streamId=" + streamId +
                ", securityId=" + securityId +
                ", instanceId=" + instanceId +
                '}';
    }
}
