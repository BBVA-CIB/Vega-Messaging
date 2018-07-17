package com.bbva.kyof.vega.autodiscovery.sniffer;

/**
 * Sniffer exception type
 */

public class SnifferException extends Exception
{
    /**
     * Constructor with an exception code and a message
     *
     * @param customMsg the message for the exception
     */
    public SnifferException(final String customMsg)
    {
        super(customMsg);
    }

    /**
     * Constructor with an exception code, and cause of the exception
     *
     * @param cause the cause of the exception
     */
    public SnifferException(final Throwable cause)
    {
        super(cause);
    }

    /**
     * Constructor with an exception code, message and cause of the exception
     *
     * @param cause         the cause of the exception
     * @param customMessage the message for the exception
     */
    public SnifferException(final String customMessage, final Throwable cause)
    {
        super(customMessage, cause);
    }
}
