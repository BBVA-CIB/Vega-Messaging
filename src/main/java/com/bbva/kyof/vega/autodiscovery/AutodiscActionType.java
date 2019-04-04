package com.bbva.kyof.vega.autodiscovery;

/** Enum with all the actions that can be performed over the autodiscovery */
enum AutodiscActionType
{
    /** Register instance information */
    REGISTER_INSTANCE,
    /** Unregister instance information */
    UNREGISTER_INSTANCE,
    /** Register topic publisher or subscriber information */
    REGISTER_TOPIC,
    /** Unregister topic publisher or subscriber information */
    UNREGISTER_TOPIC,
    /** Register information on a topic socket pair */
    REGISTER_TOPIC_SOCKET,
    /** Unregister information on a topic socket pair */
    UNREGISTER_TOPIC_SOCKET,
    /** Subscribe to a topic */
    SUBSCRIBE_TO_TOPIC,
    /** Unsubscribe from a topic */
    UNSUBSCRIBE_FROM_TOPIC,
    /** Subscribe to a instances adverts */
    SUBSCRIBE_TO_INSTANCES,
    /** Unsubscribe from instances adverts */
    UNSUBSCRIBE_FROM_INSTANCES,
    /** Subscribe to topic that matches pattern */
    SUBSCRIBE_TO_PUB_PATTERN,
    /** Unsubscribe from topic that matches pattern */
    UNSUBSCRIBE_FROM_PUB_PATTERN
}
