package com.bbva.kyof.vega.msg;

import java.util.UUID;

/**
 * Interface that represent a request that has been sent.<p>
 *
 * The request will be automatically closed when it expires however is a good practice to perform a manual close if no
 * more responses are expected.<p>
 *
 * No more responses for the request will be processed once the request has been closed.
 */
public interface ISentRequest
{
    /** @return the result of the request send */
    PublishResult getSentResult();

    /** @return the unique ID of the sent request */
    UUID getRequestId();

    /** @return  true if the request has already expired */
    boolean hasExpired();

    /**
     * Reset expiration time of the request
     *
     * @param timeout new expiration time in milliseconds. The expiration time would be now + the given timeout
     */
    void resetExpiration(long timeout);

    /** Close the request, freeing the memory and preventing new responses from being processed */
    void closeRequest();

    /** @return True if the sent request has been already closed either by the user or due to expiration */
    boolean isClosed();

    /** @return the number of responses received for this request */
    int getNumberOfResponses();
}
