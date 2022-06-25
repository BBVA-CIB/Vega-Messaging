package com.bbva.kyof.vega.autodiscovery.model;

import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.net.InetUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Represent the information of a Unicast Daemon Server. When unicast autodiscovery is selected, every instance of the library becomes
 * a client for a unicast resolver daemon.
 * With High Availability, there may be various Unicast Daemon Servers, and the clients need to know
 * wich servers are up.
 * When a server receives a AutoDiscDaemonClientInfo, this server will answer to the client with a
 * AutoDiscDaemonServerInfo. The client then knows wich servers are up.
 *
 * The client will disable the Unicast Daemon Servers that do not send AutoDiscDaemonServerInfo messages.
 *
 * The object of this class contains that information. Also contains the serialization methods to convert the objects to binary messages.
 */
@NoArgsConstructor
@AllArgsConstructor
public class AutoDiscDaemonServerInfo implements IAutoDiscInfo
{
	/** Number of internal fields of type Integer */
	private static final int NUM_INT_FIELDS = 2;

	/** Serialized size for the members that have a fixed size */
	private static final int FIX_MEMBERS_SERIALIZED_SIZE = UnsafeBufferSerializer.UUID_SIZE + UnsafeBufferSerializer.INT_SIZE * NUM_INT_FIELDS;

	/** Unique id of the vega library instance of this client */
	@Getter private UUID uniqueId;

	/** Unicast resolver client ip where it received the resolver daemon messages  */
	@Getter private int unicastResolverServerIp;

	/** Unicast resolver client port where it received the resolver daemon messages */
	@Getter private int unicastResolverServerPort;

	/** Unicast resolver client hostname where it received the resolver daemon messages */
	@Getter private String unicastResolverHostname;

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

		final AutoDiscDaemonServerInfo that = (AutoDiscDaemonServerInfo) target;

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
		this.unicastResolverServerIp = buffer.readInt();
		this.unicastResolverServerPort = buffer.readInt();
		this.unicastResolverHostname = buffer.readString();
	}

	@Override
	public void toBinary(final UnsafeBufferSerializer buffer)
	{
		buffer.writeUUID(this.uniqueId);
		buffer.writeInt(this.unicastResolverServerIp);
		buffer.writeInt(this.unicastResolverServerPort);
		buffer.writeString(this.unicastResolverHostname);
	}

	@Override
	public int serializedSize()
	{
		return FIX_MEMBERS_SERIALIZED_SIZE + UnsafeBufferSerializer.serializedSize(this.unicastResolverHostname);
	}

	@Override
	public String toString()
	{
		return "AutoDiscDaemonClientInfo{" +
				"uniqueId=" + uniqueId +
				", unicastResolverServerIp=" + InetUtil.convertIntToIpAddress(unicastResolverServerIp) +
				", unicastResolverServerPort=" + unicastResolverServerPort +
				", unicastResolverHostname='" + unicastResolverHostname + '\'' +
				'}';
	}
}
