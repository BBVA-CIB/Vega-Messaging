package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.protocol.control.IOwnSecPubTopicsChangesListener;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.util.collection.HashMapOfHashSet;
import com.bbva.kyof.vega.util.net.InetUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;



/**
 * Send manager for ipc and multicast sending. It will handle all the sockets and relations between topic publisher.
 *
 * This class is thread safe!
 */
@Slf4j
class PublishersManagerIpcMcast extends AbstractPublishersManager<TopicPublisherIpcMcast>
{
    /** Stores all the publishers handled by the manager, given the parameters used to create them */
    private final Map<AeronPublisherParams, AeronPublisher> publisherByParams = new HashMap<>();

    /**
     * Stores all the registered auto-discovery topic socket infos that represent a pair of topic publisher / socket.
     * Since there is only one aeron publisher per topic publisher we can use the topic publisher id as key
     */
    private final Map<UUID, AutoDiscTopicSocketInfo> registeredTopicSocketInfosByTopicId = new HashMap<>();

    /** Store all the topic publishers related to the same aeron publisher */
    private final HashMapOfHashSet<AeronPublisher, TopicPublisherIpcMcast> topicPublishersByAeronPub = new HashMapOfHashSet<>();

    /**
     * Constructor
     *
     * @param vegaContext the context of the library instance
     * @param secureChangesNotifier to call when there is a change on a secure topic
     */
    PublishersManagerIpcMcast(final VegaContext vegaContext, final IOwnSecPubTopicsChangesListener secureChangesNotifier)
    {
        super(vegaContext, secureChangesNotifier);
    }

    @Override
    protected void cleanAfterClose()
    {
        this.publisherByParams.clear();
        this.registeredTopicSocketInfosByTopicId.clear();
        this.topicPublishersByAeronPub.clear();
    }

    @Override
    protected void processCreatedTopicPublisher(final TopicPublisherIpcMcast topicPublisher)
    {
        // Create the aeron publisher parameters for the topic publisher
        final AeronPublisherParams aeronPublisherParams = this.createAeronPublisherParams(topicPublisher);

        // Look for an existing publisher that matches the given parameters
        AeronPublisher aeronPublisher = this.publisherByParams.get(aeronPublisherParams);

        // If it doesn't exists already, create a new one
        if (aeronPublisher == null)
        {
            log.debug("Creating new AeronPublisher for topic publisher on topic [{}]", topicPublisher.getTopicName());

            aeronPublisher = new AeronPublisher(this.getVegaContext(), aeronPublisherParams);
            this.publisherByParams.put(aeronPublisherParams, aeronPublisher);
        }
        else
        {
            log.debug("Reusing existing AeronPublisher for topic publisher on topic [{}]", topicPublisher.getTopicName());
        }

        // Add the related topic to the aeron publisher, there can be more than one since we are reusing
        this.topicPublishersByAeronPub.put(aeronPublisher, topicPublisher);

        // Set the Aeron Publisher for the topic publisher, there is only one
        topicPublisher.setAeronPublisher(aeronPublisher);

        // Register the new topic socket info in auto-discovery
        this.registerTopicSocketInfoInAutodiscovery(topicPublisher, aeronPublisherParams);
    }

    @Override
    protected TopicPublisherIpcMcast instantiateTopicPublisher(final String topicName, final TopicTemplateConfig templateCfg)
    {
        return new TopicPublisherIpcMcast(topicName, templateCfg, this.getVegaContext());
    }

    @Override
    protected SecureTopicPublisherIpcMcast instantiateSecureTopicPublisher(final String topicName, final TopicTemplateConfig templateCfg, final TopicSecurityTemplateConfig securityConfig) throws VegaException
    {
        // Create the secure topic publisher
        final SecureTopicPublisherIpcMcast result = new SecureTopicPublisherIpcMcast(topicName, templateCfg, this.getVegaContext(), securityConfig);

        // Notify that a new secure topic publisher has been added
        this.secureChangesNotifier.onOwnSecureTopicPublisherAdded(result.getUniqueId(), result.getSessionKey(), securityConfig);

        return result;
    }

    @Override
    protected void processTopicPublisherBeforeDestroy(final TopicPublisherIpcMcast topicPublisher)
    {
        // Check for the internal publisher
        topicPublisher.runForAeronPublisher(aeronPublisher ->
        {
            // Remove the topic publisher from the list of topic publishers for the aeron publisher
            this.topicPublishersByAeronPub.remove(aeronPublisher, topicPublisher);

            if (!this.topicPublishersByAeronPub.containsKey(aeronPublisher))
            {
                aeronPublisher.close();
                this.publisherByParams.remove(aeronPublisher.getParams());
            }

            // Un-registerTopicInfo topic socket info from auto-discovery
            this.unRegisterTopicSocketInfoFromAutodiscovery(topicPublisher);
        });

        // If secure topic, notify
        if (topicPublisher.hasSecurity())
        {
            // Tell the secure changes notifier about the removal if required
            this.secureChangesNotifier.onOwnSecuredTopicPublisherRemoved(topicPublisher.getUniqueId());
        }

        // Close the topic publisher
        topicPublisher.close();
    }

    /**
     * Given the topic publisher it creates the aeron publisher parameters that correspond to the topic publisher
     * @param topicPublisher topic publisher to create the parameters from
     * @return the created parameters for the publisher
     */
    private AeronPublisherParams createAeronPublisherParams(final TopicPublisherIpcMcast topicPublisher)
    {
        // Get the name of the topic
        final String topicName = topicPublisher.getTopicName();

        // Get the configuration
        final TopicTemplateConfig templateCfg = topicPublisher.getTopicConfig();

        // Select the Stream ID
        final int streamId = AeronChannelHelper.selectStreamFromRange(topicName, templateCfg.getNumStreamsPerPort());

        // Create the publisher parameters
        if (templateCfg.getTransportType() == TransportMediaType.MULTICAST)
        {
            // Select the ip address
            final String ipAddress = AeronChannelHelper.selectMcastIpFromRange(topicName, templateCfg.getMulticastAddressLow(), templateCfg.getMulticastAddressHigh());

            // Select the port
            final int portNumber = AeronChannelHelper.selectPortFromRange(topicName, templateCfg.getMinPort(), templateCfg.getMaxPort());

            // Create the parameters
            return new AeronPublisherParams(TransportMediaType.MULTICAST, InetUtil.convertIpAddressToInt(ipAddress), portNumber, streamId, templateCfg.getSubnetAddress());
        }
        else
        {
            return new AeronPublisherParams(TransportMediaType.IPC, 0, 0, streamId, null);
        }
    }

    /**
     * Register the information about a topic / socket pair in autodiscovery by providing the topic and the parameters of the socket (aeron publisher params)
     *
     * @param topicPublisher the topic publisher of the topic / socket pair
     * @param aeronPublisherParams the publisher parameters of the socket
     */
    private void registerTopicSocketInfoInAutodiscovery(final TopicPublisherIpcMcast topicPublisher, final AeronPublisherParams aeronPublisherParams)
    {
        final AutoDiscTopicSocketInfo autoDiscTopicSocketInfo = new AutoDiscTopicSocketInfo(
                this.getVegaContext().getInstanceUniqueId(),
                this.convertToPubAutodiscTransportType(aeronPublisherParams.getTransportType()),
                UUID.randomUUID(),
                topicPublisher.getTopicName(),
                topicPublisher.getUniqueId(),
                aeronPublisherParams.getIpAddress(),
                aeronPublisherParams.getPort(),
                aeronPublisherParams.getStreamId(),
                AutoDiscTopicSocketInfo.NO_HOSTNAME,
                topicPublisher.hasSecurity() ? this.getVegaContext().getSecurityContext().getSecurityId() : AutoDiscTopicSocketInfo.NO_SECURED_CONSTANT);

        this.registeredTopicSocketInfosByTopicId.put(topicPublisher.getUniqueId(), autoDiscTopicSocketInfo);

        this.getVegaContext().getAutodiscoveryManager().registerTopicSocketInfo(autoDiscTopicSocketInfo);
    }

    /**
     * Unregister the information about a topic / socket pair from autodiscovery. Since in IPC and multicast there is a single aeron publisher per topic
     * publisher we just need the topic information.
     *
     * @param topicPublisher the topic publisher of the topic / socket pair
     */
    private void unRegisterTopicSocketInfoFromAutodiscovery(final TopicPublisherIpcMcast topicPublisher)
    {
        final AutoDiscTopicSocketInfo autoDiscTopicSocketInfo = this.registeredTopicSocketInfosByTopicId.remove(topicPublisher.getUniqueId());

        if (autoDiscTopicSocketInfo != null)
        {
            this.getVegaContext().getAutodiscoveryManager().unregisterTopicSocketInfo(autoDiscTopicSocketInfo);
        }
    }

    @Override
    public void onNewAutoDiscTopicInfo(final AutoDiscTopicInfo info)
    {
        log.debug("New topic info event received from auto-discovery {}", info);
    }

    @Override
    public void onTimedOutAutoDiscTopicInfo(final AutoDiscTopicInfo info)
    {
        log.debug("Topic info event timed out in auto-discovery {}", info);
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
