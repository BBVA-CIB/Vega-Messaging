package com.bbva.kyof.vega.autodiscovery.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;

/**
 * Listener to receive the events triggered by the auto-discovery mechanism
 */
public interface IAutodiscInstanceListener
{
    /**
     * Called when a new autodiscovery instance info is added
     *
     * @param info the added info
     */
    void onNewAutoDiscInstanceInfo(AutoDiscInstanceInfo info);

    /**
     * Called when a new autodiscovery instance info times out
     *
     * @param info the removed info
     */
    void onTimedOutAutoDiscInstanceInfo(AutoDiscInstanceInfo info);
}
