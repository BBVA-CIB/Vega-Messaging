package com.bbva.kyof.vega.msg;

import java.util.UUID;

/**
 * Interface that represents received response.<br><br>
 *
 * The request ByteBuffer contents may be reused by the library to avoid memory allocation, always promote the message
 * if the message contents are going to be accessed on separate user thread!
 */
public interface IRcvResponse extends IRcvMessage
{
    /** @return the unique request identifier of the original sent request */
    UUID getOriginalRequestId();

    /**
     * Promote the response by cloning all internal fields into a new message, including the content
     *
     * Since the library will try to reuse internal buffers you have to promote the message if it's contents are going
     * to be accessed from a separate thread.
     *
     * @return the new created promoted message
     */
    @Override
    IRcvResponse promote();
}
