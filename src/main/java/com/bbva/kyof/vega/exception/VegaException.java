package com.bbva.kyof.vega.exception;


/**
 * Vega Messaging library exception type
 */
@SuppressWarnings("serial")
public class VegaException extends java.lang.Exception
{
    /**
     * Constructor with an exception code and a message
     *
     * @param customMsg the message for the exception
     */
    public VegaException(final String customMsg)
    {
        super(customMsg);
    }

    /**
     * Constructor with an exception code, and cause of the exception
     *
     * @param cause the cause of the exception
     */
    public VegaException(final Throwable cause)
    {
        super(cause);
    }

    /**
     * Constructor with an exception code, message and cause of the exception
     *
     * @param cause the cause of the exception
     * @param customMessage the message for the exception
     */
    public VegaException(final String customMessage, final Throwable cause)
    {
        super(customMessage, cause);
    }
}
