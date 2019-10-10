package com.bbva.kyof.vega.autodiscovery.daemon;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscDaemonClientInfo;
import org.agrona.DirectBuffer;

/**
 * Listener interface to implement in order to get notifications from incomming messages in the Unicast Daemon
 */
interface IDaemonReceiverListener
{
    /**
     * Called when a client is considered disconnected due to timeout
     * @param clientInfo the disconnected client information
     */
    void onRemovedAutoDiscDaemonClientInfo(AutoDiscDaemonClientInfo clientInfo);

    /**
     * Called when a new auto-discovery message that should be forwarded arrives to the daemon.
     *
     * Any message that is not a client info message will be forwarded to all clients connected to the daemon.
     *
     * @param buffer the buffer containing the original message
     * @param offset offset of the original message in the buffer
     * @param length length of the message
     */
    void onNewMessageToFordward(DirectBuffer buffer, int offset, int length);

    /**
     * Called when a new client connects to the daemon
     *
     * @param info information about the connected client
     */
    void onNewAutoDiscDaemonClientInfo(AutoDiscDaemonClientInfo info);

    /**
     * Called when a daemon receives a client info msg
     *
     * @param info information about the connected client
     */
    void onReceiveAutoDiscDaemonClientInfo(AutoDiscDaemonClientInfo info);
}
