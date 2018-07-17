package com.bbva.kyof.vega.msg;

import org.agrona.DirectBuffer;

import java.util.UUID;

/**
 * Interface that represents received request.<p>
 *
 * The request ByteBuffer contents may be reused by the framework to avoid memory allocation, always promote the message
 * if the message contents are going to be accessed on separate user thread!
 */
public interface IRcvRequest extends IRcvMessage
{
    /** @return  the unique request identifier of the received request */
    UUID getRequestId();

    /**
     * Promote the request by cloning all internal fields into a new message, including the content
     *
     * Since the library will try to reuse internal buffers you have to promote the message if it's contents are going
     * to be accessed from a separate thread.
     *
     * @return the new created promoted received request
     */
    @Override
    IRcvRequest promote();

    /**
     * Send a new response. There is no limit of responses that can be sent for the same received request.
     *
     * @param responseContent the response contents to sendMsg. Support direct and non direct byte buffers.
     * @param offset Offset for the message start in the buffer
     * @param length Length of the message starting in the given offset
     *
     * @return the enum with the result of the publication
     */
    PublishResult sendResponse(DirectBuffer responseContent, int offset, int length);
}
