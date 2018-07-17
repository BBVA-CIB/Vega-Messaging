package com.bbva.kyof.vega.msg;

import org.agrona.DirectBuffer;

import java.util.UUID;

/**
 * Interface to response received requests
 */
public interface IRequestResponder
{
    /**
     * Send a response
     *
     * @param requestId the original request id that trigger the response
     * @param message the response message
     * @param offset Offset for the message start in the buffer
     * @param length Length of the message starting in the given offset
     *
     * @return the result of the response sent
     */
    PublishResult sendResponse(UUID requestId, DirectBuffer message, int offset, int length);
}
