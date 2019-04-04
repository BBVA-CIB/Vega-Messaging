package com.bbva.kyof.vega.autodiscovery.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;

/** Interface to implement in order to listen to events triggered in the auto-discovery receiver */
public interface IAutodiscGlobalEventListener
{
    /**
     * Called when a instance info event that was not registered is received for the first time
     * @param info the event information
     */
    void onNewInstanceInfo(AutoDiscInstanceInfo info);

    /**
     * Called when a topic info event that was not registered is received for the first time
     * @param info the event information
     */
    void onNewTopicInfo(AutoDiscTopicInfo info);
}
