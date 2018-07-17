package com.bbva.kyof.vega.msg;

import io.aeron.Publication;

/** Enum with the possible results after a publication */
public enum PublishResult
{
    /** Message sent
     *
     *  It doesn't actually means it has been sent, if the publisher is closed, there are no connected clients
     *  or there is an internal aeron error an OK will be returned as well.
     */
    OK,

    /** Failed due to back pressure in one or all the underlying sockets */
    BACK_PRESSURED,

    /** Unexpected internal unexpected library error in one of the underlying sockets */
    UNEXPECTED_ERROR;

    /**
     * Converts the aeron publication result into our internal publish result enum
     *
     * @param aeronResult to convert
     * @return Enum with the possible results after a publication
     */
    public static PublishResult fromAeronResult(final long aeronResult)
    {
        if (aeronResult == Publication.BACK_PRESSURED)
        {
            return PublishResult.BACK_PRESSURED;
        }
        else
        {
            return PublishResult.OK;
        }
    }
}
