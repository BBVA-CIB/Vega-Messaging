package com.bbva.kyof.vega.autodiscovery.model;

import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import lombok.*;

import java.util.UUID;

/**
 * Represent a topic publisher or topic subscriber that has been created in a vega library instance.
 */
@NoArgsConstructor
public class AutoDiscTopicInfo implements IAutoDiscTopicInfo
{
    /** Constant for security id when the topic is a non secured one */
    public static final int NO_SECURED_CONSTANT = 0;

    /** Serialized size for the members that have a fixed size */
    private static final int FIX_MEMBERS_SERIALIZED_SIZE = UnsafeBufferSerializer.BYTE_SIZE +
            (UnsafeBufferSerializer.UUID_SIZE * 2) +
            UnsafeBufferSerializer.INT_SIZE;

    /** Instance id the topic belongs to */
    @Getter private UUID instanceId;

    /** Transport type and direction for the topic */
    @Getter private AutoDiscTransportType transportType;

    /** Unique of the topic publisher or topic subscriber */
    @Getter private UUID uniqueId;

    /** Topic name */
    @Getter private String topicName;

    /** Security ID of the topic if security is activated, 0 if security is not active */
    @Getter private int securityId;


    /**
     * Create a new auto discovery topic info message with no security
     *
     * @param instanceId Instance id the topic belongs to
     * @param transportType the transport type of the topic
     * @param uniqueId the topic unique id
     * @param topicName the name of the topic
     */
    public AutoDiscTopicInfo(final UUID instanceId, final AutoDiscTransportType transportType, final UUID uniqueId, final String topicName)
    {
        this(instanceId, transportType, uniqueId, topicName, NO_SECURED_CONSTANT);
    }

    /**
     * Create a new auto discovery topic info message
     *
     * @param instanceId Instance id the topic belongs to
     * @param transportType the transport type of the topic
     * @param uniqueId the topic unique id
     * @param topicName the name of the topic
     * @param securityId the security if of the topic, 0 if not secured
     */
    public AutoDiscTopicInfo(final UUID instanceId, final AutoDiscTransportType transportType, final UUID uniqueId, final String topicName, final int securityId)
    {
        this.instanceId = instanceId;
        this.transportType = transportType;
        this.uniqueId = uniqueId;
        this.topicName = topicName;
        this.securityId = securityId;
    }

    /**
     * Return true if the topic has been configured with security
     *
     * @return true if the topic has security configured
     */
    public boolean hasSecurity()
    {
        return this.securityId != NO_SECURED_CONSTANT;
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

        AutoDiscTopicInfo that = (AutoDiscTopicInfo) target;

        return uniqueId.equals(that.uniqueId);
    }

    @Override
    public int hashCode()
    {
        return uniqueId.hashCode();
    }

    @Override
    public void fromBinary(final UnsafeBufferSerializer buffer)
    {
        // Read compulsory fields
        this.instanceId = buffer.readUUID();
        this.transportType = AutoDiscTransportType.fromByte(buffer.readByte());
        this.uniqueId = buffer.readUUID();
        this.topicName = buffer.readString();
        this.securityId = buffer.readInt();
    }

    @Override
    public void toBinary(final UnsafeBufferSerializer buffer)
    {
        buffer.writeUUID(this.instanceId);
        buffer.writeByte(this.transportType.getByteValue());
        buffer.writeUUID(this.uniqueId);
        buffer.writeString(this.topicName);
        buffer.writeInt(this.securityId);
    }

    @Override
    public int serializedSize()
    {
        // Size of fix members plus string size
        return FIX_MEMBERS_SERIALIZED_SIZE + UnsafeBufferSerializer.serializedSize(this.topicName);
    }

    @Override
    public String toString()
    {
        return "AutoDiscTopicInfo{" +
                "transportType=" + transportType +
                ", uniqueId=" + uniqueId +
                ", topicName='" + topicName + '\'' +
                ", securityId=" + securityId +
                '}';
    }
}
