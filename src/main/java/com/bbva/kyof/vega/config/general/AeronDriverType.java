package com.bbva.kyof.vega.config.general;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;

/**
 * Represents the type of Aeron driver to use
 */
@XmlType(name = "AeronDriverType")
@XmlEnum
public enum AeronDriverType
{
    /** External driver */
    EXTERNAL,
    /** Driver embedded in the application */
    EMBEDDED,
    /** Low latency embedded driver in the instance */
    LOWLATENCY_EMBEDDED,
    /** Driver embedded using back off idle strategy */
    BACK_OFF_EMBEDDED;

    /** @return the name of the auto-disco type */
    public String value() 
    {
        return name();
    }

    /**
     * Get an aeron driver type type from a given value
     * 
     * @param value of the aeron driver type
     * @return the aeron driver type
     */
    public static AeronDriverType fromValue(final String value)
    {
        return valueOf(value);
    }

}
