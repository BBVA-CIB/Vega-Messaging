package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.control.IOwnSecPubTopicsChangesListener;
import com.bbva.kyof.vega.util.collection.HashMapOfHashSet;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


/**
 * Send manager for unicast sending. It will handle all the sockets and relations between topic publisher.
 *
 * This class is thread safe!
 */
@Slf4j
class PublishersManagerUnicast extends AbstractPublishersManager<TopicPublisherUnicast>
{
    /** Stores all the publishers in the pool, given the parameters used to create them */
    private final Map<AeronPublisherParams, AeronPublisher> publisherByParams = new HashMap<>();

    /** Store all the sub topic sockets info of subscribers that match a topic name */
    private final HashMapOfHashSet<String, AutoDiscTopicSocketInfo> subTopicSocketInfosByTopicName = new HashMapOfHashSet<>();

    /** Store all the sub topic sockets info related to an AeronPublisher */
    private final HashMapOfHashSet<AeronPublisher, AutoDiscTopicSocketInfo> subTopicSocketInfosByAeronPub = new HashMapOfHashSet<>();

    /** Store the relation between sub topic socket infos and aeron publishers */
    private final Map<UUID, AeronPublisher> aeronPubBySubTopicSocketId = new HashMap<>();

    /**
     * Constructor
     *
     * @param vegaContext the context of the manager instance
     * @param secureChangesNotifier to call when there is a change on a secure topic
     */
    PublishersManagerUnicast(final VegaContext vegaContext, final IOwnSecPubTopicsChangesListener secureChangesNotifier)
    {
        super(vegaContext, secureChangesNotifier);
    }

    @Override
    protected void cleanAfterClose()
    {
        this.publisherByParams.clear();
        this.subTopicSocketInfosByTopicName.clear();
        this.subTopicSocketInfosByAeronPub.clear();
        this.aeronPubBySubTopicSocketId.clear();
    }

    @Override
    protected void processCreatedTopicPublisher(final TopicPublisherUnicast topicPublisher)
    {
        // Nothing to do
    }

    @Override
    protected TopicPublisherUnicast instantiateTopicPublisher(final String topicName, final TopicTemplateConfig templateCfg)
    {
        return new TopicPublisherUnicast(topicName, templateCfg, this.getVegaContext());
    }

    @Override
    protected SecureTopicPublisherUnicast instantiateSecureTopicPublisher(final String topicName,
                                                                          final TopicTemplateConfig templateCfg,
                                                                          final TopicSecurityTemplateConfig securityConfig) throws VegaException
    {
        // Create the secure topic publisher
        final SecureTopicPublisherUnicast result = new SecureTopicPublisherUnicast(topicName, templateCfg, this.getVegaContext(), securityConfig);

        // Notify that a new secure topic publisher has been added
        this.secureChangesNotifier.onOwnSecureTopicPublisherAdded(result.getUniqueId(), result.getSessionKey(), securityConfig);

        return result;
    }

    @Override
    protected void processTopicPublisherBeforeDestroy(final TopicPublisherUnicast topicPublisher)
    {
        // Unsubscribe from topic adverts in auto-discovery
        this.getVegaContext().getAutodiscoveryManager().unsubscribeFromTopic(topicPublisher.getTopicName(), AutoDiscTransportType.SUB_UNI, this);

        // Remove and consume all the topic socket Id's related to that topic subscriber
        this.subTopicSocketInfosByTopicName.removeAndConsumeIfKeyEquals(topicPublisher.getTopicName(), topicSocketId ->
                removeTopicSocketInfo(topicPublisher, topicSocketId));

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
     * Given the topic publisher it creates the aeron publisher parameters that correspond to the topic publisher and the subscriber to connect to
     * @param topicPublisher topic publisher to create the parameters from
     * @param subcriberInfo the topic socket info of the subscriber to connect to
     *
     * @return the created parameters for the publisher
     */
    private AeronPublisherParams createAeronPublisherParams(final TopicPublisherUnicast topicPublisher, final AutoDiscTopicSocketInfo subcriberInfo)
    {
        // Create the aeron publisher parameters for the topic publisher
        return new AeronPublisherParams(
                TransportMediaType.UNICAST,
                subcriberInfo.getIpAddress(),
                subcriberInfo.getPort(),
                subcriberInfo.getStreamId(),
                topicPublisher.getTopicConfig().getSubnetAddress());
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
    public void onNewAutoDiscTopicSocketInfo(final AutoDiscTopicSocketInfo subTopicSocketInfo)
    {
        log.debug("New topic socket info event received from auto-discovery {}", subTopicSocketInfo);

        synchronized (this.lock)
        {
            // Check if closed
            if (this.isClosed())
            {
                return;
            }

            // Get the topic publisher for the subscriber topic name
            final TopicPublisherUnicast topicPublisher = this.getTopicPublisherForTopicName(subTopicSocketInfo.getTopicName());
            if (topicPublisher == null)
            {
                return;
            }

            // Check if we already have that topic socket info registered, if registered is a duplicated event
            if (this.aeronPubBySubTopicSocketId.containsKey(subTopicSocketInfo.getUniqueId()))
            {
                return;
            }

            // Make sure it match the security filters, in other case ignore the event
            if (!this.performSecurityFilter(topicPublisher, subTopicSocketInfo))
            {
                return;
            }

            // Create the aeron publisher parameters for the topic publisher
            final AeronPublisherParams aeronPublisherParams = this.createAeronPublisherParams(topicPublisher, subTopicSocketInfo);

            // Check if we already have the socket created with that parameters, if not create a new one
            AeronPublisher aeronPublisher = this.publisherByParams.get(aeronPublisherParams);

            if (aeronPublisher == null)
            {
                aeronPublisher = new AeronPublisher(this.getVegaContext(), aeronPublisherParams);
                this.publisherByParams.put(aeronPublisherParams, aeronPublisher);
            }

            // Add all the relations between the topic socket id and the topic publisher and aeron publisher
            this.aeronPubBySubTopicSocketId.put(subTopicSocketInfo.getUniqueId(), aeronPublisher);
            this.subTopicSocketInfosByAeronPub.put(aeronPublisher, subTopicSocketInfo);
            this.subTopicSocketInfosByTopicName.put(topicPublisher.getTopicName(), subTopicSocketInfo);

            // Finally add the reused or created Aeron publisher to the topic publisher. If already exists it will just
            // be ignored.
            topicPublisher.addAeronPublisher(aeronPublisher);
        }
    }

    @Override
    public void onTimedOutAutoDiscTopicSocketInfo(final AutoDiscTopicSocketInfo subscriberTopicSocketInfo)
    {
        log.debug("Topic socket info event timed out in auto-discovery {}", subscriberTopicSocketInfo);

        synchronized (this.lock)
        {
            // Check if closed
            if (this.isClosed())
            {
                return;
            }

            // Get the topic publisher for the subscriber topic name
            final TopicPublisherUnicast topicPublisher = this.getTopicPublisherForTopicName(subscriberTopicSocketInfo.getTopicName());
            if (topicPublisher == null)
            {
                return;
            }

            // Remove the relationship between the topic and the topic socket
            this.subTopicSocketInfosByTopicName.remove(topicPublisher.getTopicName(), subscriberTopicSocketInfo);

            // Remove the the topic socket, it will delete the required relations and close the aeron publisher if necessary
            this.removeTopicSocketInfo(topicPublisher, subscriberTopicSocketInfo);
        }
    }

    /**
     * Triggered to remove a topic socket info that was stored.
     *
     * It will remove all stored relations for that topic socket id and if the AeronPublisher that contains it
     * don't have any more topic sockets related it will close and delete it as well
     *
     * @param topicPublisher the topic publisher for the topic name that match the topic socket information
     * @param subTopicSocketInfo topic socket info of the topic socket to remove
     */
    private void removeTopicSocketInfo(final TopicPublisherUnicast topicPublisher, final AutoDiscTopicSocketInfo subTopicSocketInfo)
    {
        // For each topic socket id that is going to be deleted, find the Aeron Subscribers that have them and remove it from there as well
        final AeronPublisher aeronPublisher = this.aeronPubBySubTopicSocketId.remove(subTopicSocketInfo.getUniqueId());

        // It may not be there if it is triggered by a duplicated event
        if (aeronPublisher == null)
        {
            return;
        }

        // Remove it also from the topic socket id's related to the aeron publisher
        this.subTopicSocketInfosByAeronPub.remove(aeronPublisher, subTopicSocketInfo);

        // If there are no more topic socket ids for the Aeron Subscriber, we should close it
        if (!this.subTopicSocketInfosByAeronPub.containsKey(aeronPublisher))
        {
            // If there are no topic sockets attached to the AeronPublisher we should remove it as well
            aeronPublisher.close();
            this.publisherByParams.remove(aeronPublisher.getParams());
        }

        // We have to decide if the aeron publisher should be removed form the topic publisher
        // The criteria is to remove if there are no more topic tocket infos related to the aeron publisher that have the same
        // topic name than the topic publisher
        if (!this.subTopicSocketInfosByAeronPub.anyValueForKeyMatchFilter(aeronPublisher,
            topicSocket -> topicSocket.getTopicName().equals(topicPublisher.getTopicName())))
        {
            // Remove the socket from the topic publisher
            topicPublisher.removeAeronPublisher(aeronPublisher);
        }
    }

    /**
     *  Check the security to decide if the topic should be filtered. Both topics should have or not have security, if secured the security id of
     *  the subscriber should be in the list of valid id's for the publisher.
     *
     * @param topicPublisher the existing topic publisher
     * @param subTopicSocketInfo the new topic socket subscriber information
     * @return true if it pass the security filger
     */
    private boolean performSecurityFilter(final TopicPublisherUnicast topicPublisher, final AutoDiscTopicSocketInfo subTopicSocketInfo)
    {
        // If the topic publisher has security configured
        if (topicPublisher.hasSecurity())
        {
            // The topic subscriber has security configured, the sub topic socket should have as well
            if (!subTopicSocketInfo.hasSecurity())
            {
                log.warn("Non-secured new SubTopicSocketInfo event received but the publisher has security configured. {}", subTopicSocketInfo);
                return false;
            }

            // Both have security, make sure the security id is in the list of valid id's
            if (!topicPublisher.getTopicSecurityConfig().getSubSecIds().contains(subTopicSocketInfo.getSecurityId()))
            {
                log.warn("Secured new SubTopicSocketInfo event received but the secure id is not configured for the publisher. {}", subTopicSocketInfo);
                return false;
            }
        }
        else if(subTopicSocketInfo.hasSecurity())
        {
            // No security configured in topic publisher, but topic socket has security, is an error
            log.warn("Secured new SubTopicSocketInfo event received but the publisher has no security configured. {}", subTopicSocketInfo);
            return false;
        }

        return true;
    }
}
