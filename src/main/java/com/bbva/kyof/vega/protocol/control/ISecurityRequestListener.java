package com.bbva.kyof.vega.protocol.control;

import com.bbva.kyof.vega.msg.MsgSecurityReq;

/**
 * Listener to implement in order to listen to security request messages
 */
interface ISecurityRequestListener
{
    /**
     * Called when a new security request is received
     *
     * @param securityReq the security request message
     */
    void onSecurityRequestReceived(MsgSecurityReq securityReq);
}
