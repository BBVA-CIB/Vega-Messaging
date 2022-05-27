package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.config.general.ResponsesConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.control.ISecurityRequesterNotifier;
import com.bbva.kyof.vega.util.collection.HashMapOfHashSet;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import com.bbva.kyof.vega.util.net.InetUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Receive manager to handle unicast subscriptions. It will handle all the sockets and relations between topic subscribers.
 *
 * This class is thread safe!
 */
@Slf4j
final class SubscribersManagerUnicast extends AbstractSubscribersManager
{
    /** Stores all the subscribers in the pool, given the parameters used to create them */
    private final Map<AeronSubscriberParams, AeronSubscriber> subscribersByParams = new HashMap<>();

    /** Stores all the registered auto-discovery topic socket infos for unregisterInstanceInfo lookup
     *  Since there is only one aeron subscriber per topic subscriber we can use the topic subscriber as key */
    private final Map<UUID, AutoDiscTopicSocketInfo> registeredTopicSocketInfosByTopicId = new HashMap<>();

    /** Aeron subscriber by topic subscriber, in unicast each topic subscriber has a single aeron subscriber */
    private final Map<TopicSubscriber, AeronSubscriber> aeronSubByTopicSub = new HashMap<>();

    /** Store all the topic subscribers related to the same aeron subscriber */
    private final HashMapOfHashSet<AeronSubscriber, TopicSubscriber> topicSubscribersByAeronSub = new HashMapOfHashSet<>();

    /** Socket wrapper that can receive responses from sent requests */
    private final AeronSubscriber responsesSubscriber;

    /**
     * Constructor
     *
     * @param vegaContext the context of the manager instance
     * @param pollersManager manager that handle the pollers
     * @param topicSubAndTopicPubIdRelations relationships between topic subscribers and topic publishers
     * @param subSecurityNotifier notifier for security changes
     */
    SubscribersManagerUnicast(final VegaContext vegaContext,
                              final SubscribersPollersManager pollersManager,
                              final TopicSubAndTopicPubIdRelations topicSubAndTopicPubIdRelations,
                              final ISecurityRequesterNotifier subSecurityNotifier)
    {
        super(vegaContext, pollersManager, topicSubAndTopicPubIdRelations, subSecurityNotifier);

        // Create the receiver for unicast responses
        this.responsesSubscriber = this.createResponsesSubscriber();
    }

    @Override
    protected void processCreatedTopicSubscriber(final TopicSubscriber topicSubscriber)
    {
        // Call the parent
        super.processCreatedTopicSubscriber(topicSubscriber);

        // Create the Aeron publisher parameters for the topic publisher
        final AeronSubscriberParams aeronSubscriberParams = this.createAeronSubscriberParams(topicSubscriber);

        // Look for an existing subscriber that matches the given parameters
        AeronSubscriber aeronSubscriber = this.subscribersByParams.get(aeronSubscriberParams);

        // If it doesn't exists already, create a new one
        if (aeronSubscriber == null)
        {
            aeronSubscriber = new AeronSubscriber(this.getVegaContext(), aeronSubscriberParams);
            this.subscribersByParams.put(aeronSubscriberParams, aeronSubscriber);

            // Add to the poller
            this.getPollersManager().getPoller(topicSubscriber.getTopicConfig().getRcvPoller()).addSubscription(aeronSubscriber);
        }

        // Add the related topic to the aeron publisher, there can be more than one since we are reusing
        this.topicSubscribersByAeronSub.put(aeronSubscriber, topicSubscriber);

        // Add the topic subscriber and aeron subscriber relation
        this.aeronSubByTopicSub.put(topicSubscriber, aeronSubscriber);

        // Register the new topic socket info in auto-discovery
        this.registerTopicSocketInfoInAutodiscovery(topicSubscriber, aeronSubscriberParams);
    }

    @Override
    protected void processTopicSubscriberBeforeDestroy(final TopicSubscriber topicSubscriber)
    {
        // The topic subscriber is going to be deleted, remove the aeron subscriber relation and get the aeron subscriber
        final AeronSubscriber aeronSubscriber = aeronSubByTopicSub.remove(topicSubscriber);

        // Remove the topic subscriber from the lsit of topic subscribers for the aeron sub
        this.topicSubscribersByAeronSub.remove(aeronSubscriber, topicSubscriber);

        // If there are no more topic subscribers related, we should remove the aeron sub as well
        if (!this.topicSubscribersByAeronSub.containsKey(aeronSubscriber))
        {
            // Remove from the poller
            this.getPollersManager().getPoller(topicSubscriber.getTopicConfig().getRcvPoller()).removeSubscription(aeronSubscriber);

            // Close and remove from parameters
            aeronSubscriber.close();
            this.subscribersByParams.remove(aeronSubscriber.getParams());
        }

        // Un-registerTopicInfo topic socket info from auto-discovery
        this.unRegisterTopicSocketInfoFromAutodiscovery(topicSubscriber);
    }

    @Override
    public void cleanAfterClose()
    {
        // Destroy the subscriber to receive responses
        this.destroyResponsesSubscriber();

        // Clean internal maps
        this.subscribersByParams.clear();
        this.registeredTopicSocketInfosByTopicId.clear();
        this.aeronSubByTopicSub.clear();
        this.topicSubscribersByAeronSub.clear();

    }

    /**
     * Given the topic subscriber it creates the aeron subscriber parameters that correspond to the topic subscriber
     * @param topicSubscriber topic subscriber to create the parameters from
     * @return the created parameters for the subscriber
     */
    private AeronSubscriberParams createAeronSubscriberParams(final TopicSubscriber topicSubscriber)
    {
        // Get the name of the topic
        final String topicName = topicSubscriber.getTopicName();

        // Get the configuration
        final TopicTemplateConfig templateCfg = topicSubscriber.getTopicConfig();

        // Select the Stream ID
        final int streamId = AeronChannelHelper.selectStreamFromRange(topicName, templateCfg.getNumStreamsPerPort());

        // Select the ip address using the subnet address, since we are using 32 bit mask subnets we can use that address directly
        final String ipAddress = templateCfg.getSubnetAddress().getIpAddres().getHostAddress();

        // Select the port
        final int portNumber = AeronChannelHelper.selectPortFromRange(topicName, templateCfg.getMinPort(), templateCfg.getMaxPort());

        // Create the parameters
        return new AeronSubscriberParams(TransportMediaType.UNICAST, InetUtil.convertIpAddressToInt(ipAddress), portNumber, streamId, templateCfg.getSubnetAddress());
    }

    /**
     * Register the information of the pair topic / socket created in auto-discovery
     * @param topicSubscriber the topic subscriber that represent the topic
     * @param aeronSubscriberParams the parameters of the AeronSubscriber that represent the socket
     */
    private void registerTopicSocketInfoInAutodiscovery(final TopicSubscriber topicSubscriber, final AeronSubscriberParams aeronSubscriberParams)
    {
        final AutoDiscTopicSocketInfo autoDiscTopicSocketInfo = new AutoDiscTopicSocketInfo(
                this.getVegaContext().getInstanceUniqueId(),
                AutoDiscTransportType.SUB_UNI,
                UUID.randomUUID(),
                topicSubscriber.getTopicName(),
                topicSubscriber.getUniqueId(),
                aeronSubscriberParams.getIpAddress(),
                aeronSubscriberParams.getPort(),
                aeronSubscriberParams.getStreamId(),
                topicSubscriber.getTopicConfig().getHostname() == null ? AutoDiscTopicSocketInfo.NO_HOSTNAME : topicSubscriber.getTopicConfig().getHostname(),
                topicSubscriber.hasSecurity() ? this.getVegaContext().getSecurityContext().getSecurityId() : AutoDiscTopicInfo.NO_SECURED_CONSTANT);

        this.registeredTopicSocketInfosByTopicId.put(topicSubscriber.getUniqueId(), autoDiscTopicSocketInfo);

        this.getVegaContext().getAutodiscoveryManager().registerTopicSocketInfo(autoDiscTopicSocketInfo);
    }

    /**
     * Unregister the information of the pair topic / socket created from auto-discovery
     * @param topicSubscriber the topic subscriber that represent the topic
     */
    private void unRegisterTopicSocketInfoFromAutodiscovery(final TopicSubscriber topicSubscriber)
    {
        // Find the registered info, since there is only 1 per topic we can use the topic as the key
        final AutoDiscTopicSocketInfo autoDiscTopicSocketInfo = this.registeredTopicSocketInfosByTopicId.remove(topicSubscriber.getUniqueId());

        if (autoDiscTopicSocketInfo != null)
        {
            this.getVegaContext().getAutodiscoveryManager().unregisterTopicSocketInfo(autoDiscTopicSocketInfo);
        }
    }

    /**
     * Create the aeron subscriber that will listen for responses to sent requests
     * @return the created subscriber
     */
    private AeronSubscriber createResponsesSubscriber()
    {
        // Create a hash for the instance id. We will use it to select a random port and stream
        final int instanceIdHash = getVegaContext().getInstanceUniqueId().hashCode();

        // Get the configuration for responses
        final ResponsesConfig responsesConfig = getVegaContext().getInstanceConfig().getResponsesConfig();

        // Select the Stream ID
        final int streamId = AeronChannelHelper.selectStreamFromRange(instanceIdHash, responsesConfig.getNumStreams());

        // Select the ip address using the subnet address, since we are using 32 bit mask subnets we can use that address directly
        final String ipAddress = responsesConfig.getSubnetAddress().getIpAddres().getHostAddress();

        // Select the port
        final int portNumber = AeronChannelHelper.selectPortFromRange(instanceIdHash, responsesConfig.getMinPort(), responsesConfig.getMaxPort());

        // Create the parameters
        final AeronSubscriberParams params = new AeronSubscriberParams(TransportMediaType.UNICAST, InetUtil.convertIpAddressToInt(ipAddress), portNumber, streamId,
                responsesConfig.getSubnetAddress());

        // Create the subscriber
        final AeronSubscriber subscriber = new AeronSubscriber(getVegaContext(), params);

        // Add to the poller
        this.getPollersManager().getPoller(responsesConfig.getRcvPoller()).addSubscription(subscriber);

        // Return it
        return subscriber;
    }

    /**
     * Destroy the subscriber that handle responses to sent requests
     */
    private void destroyResponsesSubscriber()
    {
        // Remove subscription from the poller
        this.getPollersManager().getPoller(this.getVegaContext().getInstanceConfig().getResponsesConfig().getRcvPoller()).removeSubscription(this.responsesSubscriber);

        // Close the subscription
        this.responsesSubscriber.close();
    }

    /**
     * Return the parameters used to create the responses subsriber
     */
    AeronSubscriberParams getResponsesSubscriberParams()
    {
        return this.responsesSubscriber.getParams();
    }

    @Override
    public void onNewAutoDiscTopicSocketInfo(final AutoDiscTopicSocketInfo info)
    {
        log.debug("New topic socket info event received from auto-discovery {}", info);
    }

    @Override
    public void onTimedOutAutoDiscTopicSocketInfo(final AutoDiscTopicSocketInfo info)
    {
        log.debug("Topic socket info event timed out in auto-discovery {}", info);
    }
}