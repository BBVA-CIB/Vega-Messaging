package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.autodiscovery.model.IAutoDiscTopicInfo;

/**
 * Extension of a registered info specifically for information regarding a topic
 */
class RegisteredTopicInfo<T extends IAutoDiscTopicInfo> extends RegisteredInfo<T>
{
    /**
     * Create a new registered info with topic information
     *
     * @param info the information to registerTopicInfo
     * @param sendIntervalMillis the send interval in milliseconds
     */
    RegisteredTopicInfo(final T info, final long sendIntervalMillis)
    {
        super(info, sendIntervalMillis);
    }
}
