package com.bbva.kyof.vega.autodiscovery.model;

import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.net.InetUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Represent the information of a vega instance. Containing the name and unique Id of the instance plus the information regarding
 * response reception socket for the instance.<p>
 *
 * Also contains the serialization methods to convert the objects to binary messages.
 */
@AllArgsConstructor
@NoArgsConstructor
public class AutoDiscInstanceInfo implements IAutoDiscInfo
{
    /** Number of internal fields of type Integer */
    private static final int NUM_INT_FIELDS = 6;

    /** Serialized size for the members that have a fixed size */
    private static final int FIX_MEMBERS_SERIALIZED_SIZE = UnsafeBufferSerializer.UUID_SIZE + UnsafeBufferSerializer.INT_SIZE * NUM_INT_FIELDS;

    /** Name of the vega library instance */
    @Getter private String instanceName;

    /** Unique id of the vega library instance */
    @Getter private UUID uniqueId;

    /** Transport ip for responses being sent to the vega instance represented by this object */
    @Getter private int responseTransportIp;

    /** Transport port  for responses being sent to the vega instance represented by this object */
    @Getter private int responseTransportPort;

    /** Transport stream id for responses being sent to the vega instance represented by this object */
    @Getter private int responseTransportStreamId;

    /** Transport ip for control messages receiver */
    @Getter private int controlRcvTransportIp;

    /** Transport port for control messages receiver */
    @Getter private int controlRcvTransportPort;

    /** Transport stream id for for control messages receiver */
    @Getter private int controlRcvTransportStreamId;

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

        final AutoDiscInstanceInfo that = (AutoDiscInstanceInfo) target;

        return uniqueId.equals(that.uniqueId);
    }

    @Override
    public int hashCode()
    {
        return this.uniqueId.hashCode();
    }

    @Override
    public void fromBinary(final UnsafeBufferSerializer buffer)
    {
        this.instanceName = buffer.readString();
        this.uniqueId = buffer.readUUID();
        this.responseTransportIp = buffer.readInt();
        this.responseTransportPort = buffer.readInt();
        this.responseTransportStreamId = buffer.readInt();
        this.controlRcvTransportIp = buffer.readInt();
        this.controlRcvTransportPort = buffer.readInt();
        this.controlRcvTransportStreamId = buffer.readInt();
    }

    @Override
    public void toBinary(final UnsafeBufferSerializer buffer)
    {
        buffer.writeString(this.instanceName);
        buffer.writeUUID(this.uniqueId);
        buffer.writeInt(this.responseTransportIp);
        buffer.writeInt(this.responseTransportPort);
        buffer.writeInt(this.responseTransportStreamId);
        buffer.writeInt(this.controlRcvTransportIp);
        buffer.writeInt(this.controlRcvTransportPort);
        buffer.writeInt(this.controlRcvTransportStreamId);
    }

    @Override
    public int serializedSize()
    {
        return FIX_MEMBERS_SERIALIZED_SIZE + UnsafeBufferSerializer.serializedSize(this.instanceName);
    }

    @Override
    public String toString()
    {
        return "AutoDiscInstanceInfo{" +
                "instanceName='" + instanceName + '\'' +
                ", uniqueId=" + uniqueId +
                ", responseTransportIp=" + InetUtil.convertIntToIpAddress(responseTransportIp) +
                ", responseTransportPort=" + responseTransportPort +
                ", responseTransportStreamId=" + responseTransportStreamId +
                ", controlRcvTransportIp=" + InetUtil.convertIntToIpAddress(controlRcvTransportIp) +
                ", controlRcvTransportPort=" + controlRcvTransportPort +
                ", controlRcvTransportStreamId=" + controlRcvTransportStreamId +
                '}';
    }
}
