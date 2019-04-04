package com.bbva.kyof.vega.autodiscovery.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Represent all the possible transport types for autodiscovery information and messages.
 *
 * Each type contains the direction (pub or sub) and the transport (unicast, multicast, ipc)
 */
@AllArgsConstructor
public enum AutoDiscTransportType
{
    /** Unicast publisher type*/
    PUB_UNI((byte) 0),
    /** Multicast publisher type */
    PUB_MUL((byte) 1),
    /** Unicast subscriber type */
    SUB_UNI((byte) 2),
    /** Multicast subscriber type */
    SUB_MUL((byte) 3),
    /** IPC publisher type */
    PUB_IPC((byte) 4),
    /** IPC subscriber type */
    SUB_IPC((byte) 5);

    /** The value of the enum in byte representation */
    @Getter private byte byteValue;

    /**
     * Convert the given byte value to the represented transport type
     * @param value the transport type in byte value
     * @return the enum representation
     */
    public static AutoDiscTransportType fromByte(final byte value)
    {
        switch (value)
        {
            case 0: return PUB_UNI;
            case 1: return PUB_MUL;
            case 2: return SUB_UNI;
            case 3: return SUB_MUL;
            case 4: return PUB_IPC;
            case 5: return SUB_IPC;
            default: return null;
        }
    }

    /** @return true if is a publisher */
    public boolean isPublisher()
    {
        return this == PUB_UNI || this == PUB_MUL || this == PUB_IPC;
    }

    /** @return true if is a subscriber */
    public boolean isSubscriber()
    {
        return this == SUB_UNI || this == SUB_MUL || this == SUB_IPC;
    }
}
