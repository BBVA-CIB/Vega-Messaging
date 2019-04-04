package com.bbva.kyof.vega.protocol.control;

import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;

import java.util.UUID;

/**
 * Listener that receive events related to own secure topic publishers added or removed on the instance
 */
public interface IOwnSecPubTopicsChangesListener
{
    /**
     * Called when a new secure topic publisher is added to the vega instance
     *
     * @param topicPubId the unique id of the topic publisher
     * @param sessionKey session key for the secure connection with AES
     * @param securityConfig the secure topic configuration
     */
    void onOwnSecureTopicPublisherAdded(UUID topicPubId, byte[] sessionKey, TopicSecurityTemplateConfig securityConfig);

    /**
     * Called when a secure topic publisher is removed from the vega instance
     * @param topicPubId the unique id of the topic publisher
     */
    void onOwnSecuredTopicPublisherRemoved(UUID topicPubId);
}
