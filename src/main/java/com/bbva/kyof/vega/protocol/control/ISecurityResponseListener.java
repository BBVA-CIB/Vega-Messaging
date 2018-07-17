package com.bbva.kyof.vega.protocol.control;

import com.bbva.kyof.vega.msg.MsgSecurityErrorResp;
import com.bbva.kyof.vega.msg.MsgSecurityResp;

/**
 * Listener to implement in order to listen to security response messages
 */
interface ISecurityResponseListener
{
    /**
     * Called when a new security response is received
     *
     * @param responseMsg the security response message
     */
    void onSecuirtyResponseReceived(MsgSecurityResp responseMsg);

    /**
     * Called when a new security error response is received
     *
     * @param errorResponseMsg the security error response message
     */
    void onSecurityErrorResponseReceived(MsgSecurityErrorResp errorResponseMsg);
}
