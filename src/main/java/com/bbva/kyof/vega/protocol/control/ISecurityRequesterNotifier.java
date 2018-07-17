package com.bbva.kyof.vega.protocol.control;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;

import java.util.UUID;

/**
 * Interface implemented by the security requester. It is used to add or remove the need of security information for a publisher topic.
 *
 * When there is a change on a remote secure topic publisher it will try to obtain the security credentials in order to decode the secure messages
 */
public interface ISecurityRequesterNotifier
{
    /**
     * Called when a remote secure topic subscriber is removed. It will remove any publisher information for that topic.
     * @param subTopicId the secure topic subscriber unique id
     */
    void removedSecureSubTopic(UUID subTopicId);

    /**
     * Called when a new secure topic publisher has been found for an existing subscribed secure topic subscribed
     *
     * @param pubTopicInfo information of the added publisher topic
     * @param subTopicId the unique id of the owned subscriber topic that is going to connect to the publisher
     * @param subSecurityConfig the security configuration of the topic subscriber
     */
    void addedPubForSubTopic(AutoDiscTopicInfo pubTopicInfo, UUID subTopicId, final TopicSecurityTemplateConfig subSecurityConfig);

    /**
     * Called when a secure topic publisher is removed from an existing owned secure topic subscriber
     * @param pubTopicInfo the information of the removed topic publisher
     * @param subTopicId the unique id of the topic subscriber
     */
    void removedPubForSubTopic(AutoDiscTopicInfo pubTopicInfo, UUID subTopicId);
}
