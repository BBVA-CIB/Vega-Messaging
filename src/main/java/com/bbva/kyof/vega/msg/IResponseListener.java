package com.bbva.kyof.vega.msg;

/**
 * Implement in order to receive responses to previously sent requests
 */
public interface IResponseListener
{
    /**
     * Method called when a response is received for a sent request.
     *
     * IMPORTANT: If the response message contents are going to be accessed from a separate thread the message should be promoted!!
     *
     * @param originalSentRequest original sent request related to the response
     * @param response the received response
     */
    void onResponseReceived(final ISentRequest originalSentRequest, final IRcvResponse response);

    /**
     * Called when a set request has timed out before being manually closed
     *
     * @param originalSentRequest the original request that has timed out
     */
    void onRequestTimeout(final ISentRequest originalSentRequest);
}