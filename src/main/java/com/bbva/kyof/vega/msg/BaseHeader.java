package com.bbva.kyof.vega.msg;

import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.serialization.IUnsafeSerializable;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import lombok.*;

/**
 * Base header for every message in the framework independently of the message type
 *
 * This class is not thread safe!
 */
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class BaseHeader implements IUnsafeSerializable
{
    /** Binary size of the header once it has been serialized */
    private static final int BINARY_SIZE = UnsafeBufferSerializer.BYTE_SIZE + UnsafeBufferSerializer.INT_SIZE;

    /** Internal library type of the message */
    @Getter @Setter private byte msgType;

    /** Version of the library that sent the message */
    @Getter private int version;

    /** @return true if the version of the header is compatible with the library local version */
    public boolean isVersionCompatible()
    {
        return Version.isCompatibleWithLocal(version);
    }

    @Override
    public void toBinary(final UnsafeBufferSerializer buffer)
    {
        // Version
        buffer.writeInt(this.version);

        // Message type
        buffer.writeByte(this.msgType);
    }

    @Override
    public void fromBinary(final UnsafeBufferSerializer buffer)
    {
        this.version = buffer.readInt();
        this.msgType = buffer.readByte();
    }

    @Override
    public int serializedSize()
    {
        return BINARY_SIZE;
    }
}
