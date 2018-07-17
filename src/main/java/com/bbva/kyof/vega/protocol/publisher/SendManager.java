package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.control.IOwnSecPubTopicsChangesListener;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;

/**
 * Main class to handle the sending functionality on the framework. It contains separate managers for unicast messaging and multicast/ipc messaging.
 * The reason fo this separation is that in unicast the subscriber is the end point and in multicast/ipc is the opposite. This creates a different required
 * behaviour for both cases.<p>
 *
 * It also keep track of the created publishers and configurations.<p>
 *
 * This class is not thread safe!!
 */
@Slf4j
public class SendManager implements Closeable
{
    /** The manager to handle all publication logic for multicast and ipc */
    private final PublishersManagerIpcMcast ipcMulticastPublishersManager;

    /** The manager to handle all publication logic for unicast */
    private final PublishersManagerUnicast unicastPublishersManager;

    /** Context of the instance */
    private final VegaContext vegaContext;

    /**
     * Creates a new instance
     * @param vegaContext the context of the library instance
     * @param ownSecureChangesNotifier notifier that will be called when there are changes on secure topics
     */
    public SendManager(final VegaContext vegaContext, final IOwnSecPubTopicsChangesListener ownSecureChangesNotifier)
    {
        // Initialize the managers for publications
        this.ipcMulticastPublishersManager = new PublishersManagerIpcMcast(vegaContext, ownSecureChangesNotifier);
        this.unicastPublishersManager = new PublishersManagerUnicast(vegaContext, ownSecureChangesNotifier);

        this.vegaContext = vegaContext;
    }

    /**
     * Create a new topic publisher for the given topic
     *
     * @param topicName the name of the topic
     * @return the created topic publisher
     * @throws VegaException exception thrown if there is a problem creating the publisher or it already exists
     */
    public ITopicPublisher createTopicPublisher(final String topicName) throws VegaException
    {
        log.info("Creating publisher for topic [{}]", topicName);

        final TopicTemplateConfig templateCfg = this.findTopicConfig(topicName);

        // Find the security template configuration, it may be null if not configured
        final TopicSecurityTemplateConfig securityTemplateConfig = this.findTopicSecurityConfig(topicName);

        // Verify the topic subscriber security
        this.verifyTopicPublisherSecurityOrFail(securityTemplateConfig);

        // Call the right manager to do the rest of the job depending on the transport type
        switch (templateCfg.getTransportType())
        {
            case UNICAST:
                return this.unicastPublishersManager.createTopicPublisher(topicName, templateCfg, securityTemplateConfig);
            case MULTICAST:
            case IPC:
                return this.ipcMulticastPublishersManager.createTopicPublisher(topicName, templateCfg, securityTemplateConfig);
            default:
                return null;
        }
    }

    /**
     * Destroy an existing topic publisher given the name of the topic
     * @param topicName the name of hte topic the publisher belongs to
     *
     * @throws VegaException exception thrown if there is a problem or if the topic publisher don't exists
     */
    public void destroyTopicPublisher(final String topicName) throws VegaException
    {
        log.info("Destroying topic publisher for topic [{}] called by user", topicName);

        final TopicTemplateConfig templateCfg = this.findTopicConfig(topicName);

        // Call the right manager to do the rest of the job depending on the transport type
        switch (templateCfg.getTransportType())
        {
            case UNICAST:
                this.unicastPublishersManager.destroyTopicPublisher(topicName);
                break;
            case MULTICAST:
            case IPC:
                this.ipcMulticastPublishersManager.destroyTopicPublisher(topicName);
                break;
            default:
                break;
        }
    }

    /**
     * Find the template configuration for the given topic name
     *
     * @param topicName the name of the topic
     * @return the configuration
     * @throws VegaException exception thrown if no configuration found for the topic
     */
    private TopicTemplateConfig findTopicConfig(final String topicName) throws VegaException
    {
        // Find the publisher configuration given the topic
        final TopicTemplateConfig templateCfg = this.vegaContext.getInstanceConfig().getTopicTemplateForTopic(topicName);
        if (templateCfg == null)
        {
            log.error("No configuration found for topic [{}]", topicName);
            throw new VegaException("Cannot find any valid configuration for topic " + topicName);
        }

        return templateCfg;
    }

    /**
     * Find the topic security template configuration for the given topic name
     *
     * @param topicName the topic name to find the configuration for
     * @return an optional with the found configuration
     */
    private TopicSecurityTemplateConfig findTopicSecurityConfig(final String topicName)
    {
        // Find the subscriber configuration given the topic
        return this.vegaContext.getInstanceConfig().getTopicSecurityTemplateForTopic(topicName);
    }

    /**
     * Verify the topic subscriber security. If security is enabled, it check if the application secure id match a valid secure id for the topic
     * @param securityTemplateConfig security configuration for the topic
     * @throws VegaException exception thrown if the security is not valid
     */
    private void verifyTopicPublisherSecurityOrFail(final TopicSecurityTemplateConfig securityTemplateConfig) throws VegaException
    {
        if (securityTemplateConfig != null && !securityTemplateConfig.getPubSecIds().contains(this.vegaContext.getSecurityContext().getSecurityId()))
        {
            log.error("Trying to create secure publisher, but the application secure id is not in the configuration list of valid publishers");
            throw new VegaException("The current security id is not configured in the list of pub secure ids for the topic");
        }
    }

    @Override
    public void close()
    {
        log.info("Closing send manager for instance ID [{}]", this.vegaContext.getInstanceUniqueId());

        // Close the internal managers
        this.ipcMulticastPublishersManager.close();
        this.unicastPublishersManager.close();

        log.info("Send manager closed");
    }
}
