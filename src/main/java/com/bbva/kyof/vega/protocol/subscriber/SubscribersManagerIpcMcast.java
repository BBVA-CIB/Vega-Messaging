package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.control.ISecurityRequesterNotifier;
import com.bbva.kyof.vega.util.collection.HashMapOfHashSet;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Receive manager to hanle IPC and Multicast subscriptions. It will handle all the sockets and relations between topic subscribers.
 *
 * This class is not thread safe!
 */
@Slf4j
class SubscribersManagerIpcMcast extends AbstractSubscribersManager
{
    /** Stores all the subscribers handled by this manager by the parameters used to create them */
    private final Map<AeronSubscriberParams, AeronSubscriber> subscriberByParams = new HashMap<>();

    /** Store all the pub topic sockets info of publishers that match a topic name */
    private final HashMapOfHashSet<String, UUID> pubTopicSocketInfosByTopicName = new HashMapOfHashSet<>();

    /** Store all the pub topic sockets info related to an AeronSubscriber */
    private final HashMapOfHashSet<AeronSubscriber, UUID> pubTopicSocketIdByAeronSub = new HashMapOfHashSet<>();

    /** Store the relation between pub topic socket infos and aeron subscribers */
    private final Map<UUID, AeronSubscriber> aeronSubByPubTopicSocketId = new HashMap<>();

    /**
     * Constructor
     *
     * @param vegaContext the context of the manager instance
     * @param pollersManager manager that handle the pollers
     * @param topicSubAndTopicPubIdRelations relationships between topic subscribers and topic publishers
     * @param subSecurityNotifier notifier for security changes
     */
    SubscribersManagerIpcMcast(final VegaContext vegaContext,
                               final SubscribersPollersManager pollersManager,
                               final TopicSubAndTopicPubIdRelations topicSubAndTopicPubIdRelations,
                               final ISecurityRequesterNotifier subSecurityNotifier)
    {
        super(vegaContext, pollersManager, topicSubAndTopicPubIdRelations, subSecurityNotifier);
    }

    @Override
    protected void processTopicSubscriberBeforeDestroy(final TopicSubscriber topicSubscriber)
    {
        // Remove and consume all the topic socket Id's related to that topic subscriber
        this.pubTopicSocketInfosByTopicName.removeAndConsumeIfKeyEquals(topicSubscriber.getTopicName(), topicSocketId ->
                removeTopicSocketInfo(topicSubscriber, topicSocketId));
    }

    @Override
    public void cleanAfterClose()
    {
        this.subscriberByParams.clear();
        this.aeronSubByPubTopicSocketId.clear();
        this.pubTopicSocketIdByAeronSub.clear();
        this.pubTopicSocketInfosByTopicName.clear();
    }

    @Override
    public void onNewAutoDiscTopicSocketInfo(final AutoDiscTopicSocketInfo publisherTopicSocketInfo)
    {
        log.debug("New topic socket info event received from auto-discovery {}", publisherTopicSocketInfo);

        synchronized (this.lock)
        {
            if (this.isClosed())
            {
                return;
            }

            // Get the topic subscriber for the publisher topic name, if none we are not subscribed, ignore the event
            final TopicSubscriber topicSubscriber = this.getTopicSubscriberForTopicName(publisherTopicSocketInfo.getTopicName());
            if (topicSubscriber == null)
            {
                return;
            }

            // Check if we already have that topic socket info registered, if registered is a duplicated event
            if (this.aeronSubByPubTopicSocketId.containsKey(publisherTopicSocketInfo.getUniqueId()))
            {
                return;
            }

            // Check the security to decide if the topic socket should be filtered. Both topics should have or not have security, if secured the security id of
            // the publisher should be in the list of valid id's for the subscriber
            if (!performSecurityFilter(topicSubscriber, publisherTopicSocketInfo))
            {
                return;
            }

            // Create the aeron subscriber parameters for the topic subscriber and topic socket info of the publisher that generates the event
            final AeronSubscriberParams aeronSubscriberParams = this.createAeronSubscriberParams(topicSubscriber, publisherTopicSocketInfo);

            // Check if we already have the socket created with that parameters, if not create a new one
            AeronSubscriber aeronSubscriber = this.subscriberByParams.get(aeronSubscriberParams);

            if (aeronSubscriber == null)
            {
                // Create the new subscriber and store
                aeronSubscriber = new AeronSubscriber(this.getVegaContext(), aeronSubscriberParams);
                this.subscriberByParams.put(aeronSubscriberParams, aeronSubscriber);

                // Add to the poller
                this.getPollersManager().getPoller(topicSubscriber.getTopicConfig().getRcvPoller()).addSubscription(aeronSubscriber);
            }

            // Add all the relations between the topic socket id and the topic subscriber and aeron subscriber
            this.aeronSubByPubTopicSocketId.put(publisherTopicSocketInfo.getUniqueId(), aeronSubscriber);
            this.pubTopicSocketIdByAeronSub.put(aeronSubscriber, publisherTopicSocketInfo.getUniqueId());
            this.pubTopicSocketInfosByTopicName.put(topicSubscriber.getTopicName(), publisherTopicSocketInfo.getUniqueId());
        }
    }

    @Override
    public void onTimedOutAutoDiscTopicSocketInfo(final AutoDiscTopicSocketInfo publisherTopicSocketInfo)
    {
        log.debug("Topic socket info event timed out in auto-discovery {}", publisherTopicSocketInfo);

        synchronized (this.lock)
        {
            if (this.isClosed())
            {
                return;
            }

            // Get the topic subscriber for the publisher topic name, if not there we are not subscribed, it may be a duplicated
            final TopicSubscriber topicSubscriber = this.getTopicSubscriberForTopicName(publisherTopicSocketInfo.getTopicName());
            if (topicSubscriber == null)
            {
                return;
            }

            // Remove the relationship between the topic and the topic socket
            this.pubTopicSocketInfosByTopicName.remove(topicSubscriber.getTopicName(), publisherTopicSocketInfo.getUniqueId());

            // Remove the the topic socket, it will delete the required relations and close the aeron subscriber if necessary
            this.removeTopicSocketInfo(topicSubscriber, publisherTopicSocketInfo.getUniqueId());
        }
    }

    /**
     * Triggered to remove a topic socket info that was stored.
     *
     * It will remove all stored relations for that topic socket id and if the AeronSubscriber that contains it
     * don't have any more topic sockets related it will close and delete it as well
     *
     * @param topicSubscriber the topic subscriber for the topic name that match the topic socket information
     * @param pubTopicSocketId the unique id of the topic socket to remove
     */
    private void removeTopicSocketInfo(final TopicSubscriber topicSubscriber, final UUID pubTopicSocketId)
    {
        // For each topic socket id that is going to be deleted, find the Aeron Subscribers that have them and remove it from there as well
        final AeronSubscriber aeronSubscriber = this.aeronSubByPubTopicSocketId.remove(pubTopicSocketId);

        // It may not be there if it is triggered by a duplicated event
        if (aeronSubscriber == null)
        {
            return;
        }

        // Remove it also from the topic socket id's related to the aeron subscriber
        this.pubTopicSocketIdByAeronSub.remove(aeronSubscriber, pubTopicSocketId);

        // If there are no more topic socket ids for the Aeron Subscriber, we should close it
        if (!this.pubTopicSocketIdByAeronSub.containsKey(aeronSubscriber))
        {
            // If there are no topic sockets attached to the AeronSubscriber we should remove it as well
            this.getPollersManager().getPoller(topicSubscriber.getTopicConfig().getRcvPoller()).removeSubscription(aeronSubscriber);
            aeronSubscriber.close();
            this.subscriberByParams.remove(aeronSubscriber.getParams());
        }
    }

    /**
     * Given the topic subscriber and topic socket info it creates the aeron subscriber parameters that correspond to the topic socket
     *
     * @param topicSubscriber topic subscriber to create the parameters from
     * @param topicSocketInfo topic socket information from autodiscovery
     *
     * @return the created parameters
     */
    private AeronSubscriberParams createAeronSubscriberParams(final TopicSubscriber topicSubscriber, final AutoDiscTopicSocketInfo topicSocketInfo)
    {
        return new AeronSubscriberParams(
                topicSubscriber.getTopicConfig().getTransportType(),
                topicSocketInfo.getIpAddress(),
                topicSocketInfo.getPort(),
                topicSocketInfo.getStreamId(),
                topicSubscriber.getTopicConfig().getSubnetAddress());
    }

    /**
     *  Check the security to decide if the topic should be filtered. Both topics should have or not have security, if secured the security id of
     *  the publisher should be in the list of valid id's for the subscriber.
     *
     * @param topicSubscriber the existing topic subscriber
     * @param pubTopicSocketInfo the new topic publisher information
     * @return true if it pass the security filger
     */
    private boolean performSecurityFilter(final TopicSubscriber topicSubscriber, final AutoDiscTopicSocketInfo pubTopicSocketInfo)
    {
        // If the topic subscriber has security configured
        if (topicSubscriber.hasSecurity())
        {
            final SecureTopicSubscriber secureTopicSubscriber = (SecureTopicSubscriber)topicSubscriber;

            // The topic subscriber has security configured, the sub topic socket should have as well
            if (!pubTopicSocketInfo.hasSecurity())
            {
                log.warn("Non-secured new PubTopicSocketInfo event received but the subscriber has security configured. {}", pubTopicSocketInfo);
                return false;
            }

            // Both have security, make sure the security id is in the list of valid id's
            if (!secureTopicSubscriber.isTopicPubSecureIdAllowed(pubTopicSocketInfo.getSecurityId()))
            {
                log.warn("Secured new PubTopicSocketInfo event received but the secure id is not configured for the subscriber. {}", pubTopicSocketInfo);
                return false;
            }

            // Finally we should have the public key of the publisher
            if (!this.getVegaContext().getSecurityContext().getRsaCrypto().isSecurityIdRegistered(pubTopicSocketInfo.getSecurityId()))
            {
                // If is in the list, the public key should have been loaded as well
                log.warn("New secure publisher AutoDiscTopicSocketInfo received but the public key cannot be found in any of the pub keys files. {}", pubTopicSocketInfo);
                return false;
            }
        }
        else if(pubTopicSocketInfo.hasSecurity())
        {
            // No security configured in topic subscriber, but topic socket has security, is an error
            log.warn("Secured new PubTopicSocketInfo event received but the subscriber has no security configured. {}", pubTopicSocketInfo);
            return false;
        }

        return true;
    }
}