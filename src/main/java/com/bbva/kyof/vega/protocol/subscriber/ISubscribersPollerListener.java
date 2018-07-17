package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.msg.RcvMessage;
import com.bbva.kyof.vega.msg.RcvRequest;
import com.bbva.kyof.vega.msg.RcvResponse;

import java.util.UUID;

/**
 * Listener to implement in order to listen to messages from a subscribers poller
 */
interface ISubscribersPollerListener
{
    /**
     * Callback when a new data message is received
     * @param msg the data message
     */
    void onDataMsgReceived(RcvMessage msg);

    /**
     * Callback when a new encrypted data message is received
     * @param msg the encrypted data message
     */
    void onEncryptedDataMsgReceived(RcvMessage msg);

    /**
     * Callback when a new data request message is received
     * @param request the received request
     */
    void onDataRequestMsgReceived(RcvRequest request);

    /**
     * Callback when a new response message is received
     * @param response the received response
     */
    void onDataResponseMsgReceived(RcvResponse response);

    /**
     * Callback when a new heartbeat request message is received
     * @param senderInstanceId request sender instance id
     * @param requestId request unique id
     */
    void onHeartbeatRequestMsgReceived(final UUID senderInstanceId, final UUID requestId);
}
