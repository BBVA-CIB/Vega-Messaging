package com.bbva.kyof.vega.autodiscovery.model;

import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.net.InetUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Represent the information of a Unicast Daemon Client. When unicast autodiscovery is selected, every instance of the library becomes
 * a client for a unicast resolver daemon. In order fot he daemon to know how to route the autodiscovery messages to the client it needs
 * to know the information of the subscriber unicast socket the client uses to receive it's messages. <p>
 *
 * The object of this class contains that information. Also contains the serialization methods to convert the objects to binary messages.
 */
@NoArgsConstructor
@AllArgsConstructor
public class AutoDiscDaemonClientInfo implements IAutoDiscInfo
{
    /** Number of internal fields of type Integer */
    private static final int NUM_INT_FIELDS = 3;

    /** Serialized size for the members that have a fixed size */
    private static final int FIX_MEMBERS_SERIALIZED_SIZE = UnsafeBufferSerializer.UUID_SIZE + UnsafeBufferSerializer.INT_SIZE * NUM_INT_FIELDS;

    /** Unique id of the vega library instance of this client */
    @Getter private UUID uniqueId;

    /** Unicast resolver client ip where it received the resolver daemon messages  */
    @Getter private int unicastResolverClientIp;

    /** Unicast resolver client port where it received the resolver daemon messages */
    @Getter private int unicastResolverClientPort;

    /** Unicast resolver client stream id where it received the resolver daemon messages */
    @Getter private int unicastResolverClientStreamId;

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

        final AutoDiscDaemonClientInfo that = (AutoDiscDaemonClientInfo) target;

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
        this.uniqueId = buffer.readUUID();
        this.unicastResolverClientIp = buffer.readInt();
        this.unicastResolverClientPort = buffer.readInt();
        this.unicastResolverClientStreamId = buffer.readInt();
    }

    @Override
    public void toBinary(final UnsafeBufferSerializer buffer)
    {
        buffer.writeUUID(this.uniqueId);
        buffer.writeInt(this.unicastResolverClientIp);
        buffer.writeInt(this.unicastResolverClientPort);
        buffer.writeInt(this.unicastResolverClientStreamId);
    }

    @Override
    public int serializedSize()
    {
        return FIX_MEMBERS_SERIALIZED_SIZE;
    }

    @Override
    public String toString()
    {
        return "AutoDiscDaemonClientInfo{" +
                "uniqueId=" + uniqueId +
                ", unicastResolverClientIp=" + InetUtil.convertIntToIpAddress(unicastResolverClientIp) +
                ", unicastResolverClientPort=" + unicastResolverClientPort +
                ", unicastResolverClientStreamId=" + unicastResolverClientStreamId +
                '}';
    }
}
