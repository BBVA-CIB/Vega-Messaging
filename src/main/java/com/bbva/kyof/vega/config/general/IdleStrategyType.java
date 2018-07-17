package com.bbva.kyof.vega.config.general;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;

/** Represents an idle strategy type */
@XmlType(name = "IdleStrategyType")
@XmlEnum
public enum IdleStrategyType 
{
    /** Busy spin for lowest latency */
    BUSY_SPIN,
    /** Park the thread for some time */
    BACK_OFF,
    /** Sleep the given number of nanoseconds */
    SLEEP_NANOS;

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
    public static IdleStrategyType fromValue(final String value)
    {
        return valueOf(value);
    }
}
