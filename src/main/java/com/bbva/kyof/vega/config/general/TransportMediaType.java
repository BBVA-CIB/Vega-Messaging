package com.bbva.kyof.vega.config.general;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;

/**
 * Represents all the valid transport types available
 */
@XmlType(name = "TransportMediaType")
@XmlEnum
public enum TransportMediaType 
{
    /** Reliable unicast transport */
    UNICAST,
    /** Reliable multicast transport */
    MULTICAST,
    /** IPC transport */
    IPC;

    /** @return the value of the transport media type */
    public String value() 
    {
        return name();
    }

    
    /**
     * Get a transport media type from a given value
     * 
     * @param value of the transport media type
     * @return the transport media type
     */
    public static TransportMediaType fromValue(final String value)
    {
        return valueOf(value);
    }
}
