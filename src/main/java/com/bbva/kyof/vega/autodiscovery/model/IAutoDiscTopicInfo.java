package com.bbva.kyof.vega.autodiscovery.model;

/**
 * Interface to implement by any auto-discovery information message that contains information related to a topic
 */
public interface IAutoDiscTopicInfo extends IAutoDiscInfo
{
    /** @return the Name of the topic this information is related to */
    String getTopicName();

    /** @return the transport type this information is related to */
    AutoDiscTransportType getTransportType();
}
