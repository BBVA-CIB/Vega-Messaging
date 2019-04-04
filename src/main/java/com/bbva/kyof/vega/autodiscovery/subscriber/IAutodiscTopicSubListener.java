package com.bbva.kyof.vega.autodiscovery.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;

/**
 * Listener to receive the events triggered by the auto-discovery mechanism
 */
public interface IAutodiscTopicSubListener
{
    /**
     * Called when a new autodiscovery topic info is added
     *
     * @param info the added info
     */
    void onNewAutoDiscTopicInfo(final AutoDiscTopicInfo info);

    /**
     * Called when a autodiscovery topic info is removed due to expiration
     *
     * @param info the removed info
     */
    void onTimedOutAutoDiscTopicInfo(final AutoDiscTopicInfo info);

    /**
     * Called when a new autodiscovery topic socket pair info is added
     *
     * @param info the added info
     */
    void onNewAutoDiscTopicSocketInfo(final AutoDiscTopicSocketInfo info);

    /**
     * Called when a new autodiscovery topic socket pair info is removed due to expiration
     *
     * @param info the removed info
     */
    void onTimedOutAutoDiscTopicSocketInfo(final AutoDiscTopicSocketInfo info);
}
