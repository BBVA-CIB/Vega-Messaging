package com.bbva.kyof.vega.config.general;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;

/**
 * Represents the type of autodiscovery mechanism to use
 */
@XmlType(name = "AutoDiscoType")
@XmlEnum
public enum AutoDiscoType
{
    /** Unicast centralized daemon auto discovery */
    UNICAST_DAEMON,
    /** Multicast distributed auto discovery */
    MULTICAST;

    /** @return the name of the auto-disco type */
    public String value() 
    {
        return name();
    }

    /**
     * Get an auto-disco type from a given value
     * 
     * @param value of the auto-disco type
     * @return the auto-disco type
     */
    public static AutoDiscoType fromValue(final String value) 
    {
        return valueOf(value);
    }

}
