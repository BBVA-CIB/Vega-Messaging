package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.autodiscovery.subscriber.IAutodiscTopicSubListener;
import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.control.ISecurityRequesterNotifier;
import com.bbva.kyof.vega.util.collection.HashMapOfHashSet;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Base class for subscribers manager. The subscribers manager handles the topic subscribers that are active in the system.
 *
 * Specific implementations are created for multicast, unicast and ipc.
 *
 * This class is thread safe!
 */
@Slf4j
abstract class AbstractSubscribersManager implements Closeable, IAutodiscTopicSubListener
{
    /** Configuration of the instance  */
    @Getter(AccessLevel.PROTECTED)
    private final VegaContext vegaContext;

    /** Manager for pollers */
    @Getter(AccessLevel.PROTECTED)
    private final SubscribersPollersManager pollersManager;

    /** Stores the topic subscribers by topic name */
    private final Map<String, TopicSubscriber> topicSubscribersByTopicName = new HashMap<>();

    /** Stores all the registered auto-discovery topic infos for unregister lookup */
    private final Map<UUID, AutoDiscTopicInfo> registeredTopicInfosByTopicId = new HashMap<>();

    /** Store all the topic subscribers in the manager that are related to a pattern subscription */
    private final HashMapOfHashSet<String, TopicSubscriber> topicSubsByPatternSubs = new HashMapOfHashSet<>();

    /**
     * Stores the relationships between topic subscribers and topic publishers for quick lookup on incoming messages.
     * This information comes from the auto-discovery mechanism.
     */
    private final TopicSubAndTopicPubIdRelations topicSubAndTopicPubIdRelations;

    /**
     * Notifier for security changes in subscriber topics
     */
    private final ISecurityRequesterNotifier subSecurityNotifier;

    /** Lock for instance synchronization */
    protected final Object lock = new Object();

    /** True if closed */
    @Getter(AccessLevel.PROTECTED)
    private boolean closed = false;

    /**
     * Constructor
     *
     * @param vegaContext the context of the manager instance
     * @param pollersManager manager that handle the pollers
     * @param topicSubAndTopicPubIdRelations relationships between topic subscribers and topic publishers
     * @param subSecurityNotifier notifier for security changes
     */
    AbstractSubscribersManager(final VegaContext vegaContext,
                               final SubscribersPollersManager pollersManager,
                               final TopicSubAndTopicPubIdRelations topicSubAndTopicPubIdRelations,
                               final ISecurityRequesterNotifier subSecurityNotifier)
    {
        this.vegaContext = vegaContext;
        this.pollersManager = pollersManager;
        this.topicSubAndTopicPubIdRelations = topicSubAndTopicPubIdRelations;
        this.subSecurityNotifier = subSecurityNotifier;
    }

    /**
     * Process topic subscriber that is going to be destroyed for additional actions
     * @param topicSubscriber the topic subscriber that is going to be destroyed
     */
    protected abstract void processTopicSubscriberBeforeDestroy(final TopicSubscriber topicSubscriber);

    /**
     * Clean after closing
     */
    protected abstract void cleanAfterClose();

    @Override
    public void close()
    {
        synchronized (this.lock)
        {
            if (this.closed)
            {
                return;
            }

            // Destroy all the topic subscribers
            this.topicSubscribersByTopicName.values().forEach(this::destroyTopicSubscriber);
            this.topicSubscribersByTopicName.clear();

            this.registeredTopicInfosByTopicId.clear();
            this.topicSubscribersByTopicName.clear();

            // Clean children info
            this.cleanAfterClose();

            this.closed = true;
        }
    }

    /**
     * Subscribe to the given topic in order to get messages and responses for the topic name
     *
     * @param topicName the name of the topic to onNewPubTopicForPattern to
     * @param templateCfg topic template configuration
     * @param securityTemplateConfig security template configuration, null if security is not configured for the topic
     * @param listener listener that will process incoming messages on the topic
     * @throws VegaException exception thrown if there is a problem or already subscribed
     */
    public void subscribeToTopic(final String topicName, final TopicTemplateConfig templateCfg, final TopicSecurityTemplateConfig securityTemplateConfig, final ITopicSubListener listener) throws VegaException
    {
        log.info("Subscribing to topic [{}]", topicName);

        synchronized (this.lock)
        {
            if (this.closed)
            {
                log.error("Trying to subscribe to topic but the manager has been already closed");
                throw new VegaException("Trying to subscribe to topic but the manager has been already closed");
            }

            // Find the topic subscriber
            TopicSubscriber topicSubscriber = this.topicSubscribersByTopicName.get(topicName);

            // Dont exists
            if (topicSubscriber == null)
            {
                // Create a new one
                if (securityTemplateConfig == null)
                {
                    topicSubscriber = new TopicSubscriber(topicName, templateCfg);
                }
                else
                {
                    topicSubscriber = new SecureTopicSubscriber(topicName, templateCfg, securityTemplateConfig);
                }

                // Add the listener
                topicSubscriber.setNormalListener(listener);
                // Process as a new topic subscriber
                this.processCreatedTopicSubscriber(topicSubscriber);
            }
            else if (!topicSubscriber.setNormalListener(listener))
            {
                log.error("Already subscribed to topic topic [{}]", topicName);
                throw new VegaException(String.format("Already subscribed to topic topic [%s]", topicName));
            }
        }
    }

    /**
     * Process a created topic subscriber for additional actions
     * @param topicSubscriber the subscribers to process
     */
    protected void processCreatedTopicSubscriber(final TopicSubscriber topicSubscriber)
    {
        // Store the topic subscriber in a map
        this.topicSubscribersByTopicName.put(topicSubscriber.getTopicName(), topicSubscriber);

        // Get the autodiscovery transport type for publishers on the same transport
        final AutoDiscTransportType autoDiscPubTransport = this.convertToPubAutodiscTransportType(topicSubscriber.getTopicConfig().getTransportType());

        // Subscribe to get adverts of publishers on the same transport
        this.vegaContext.getAutodiscoveryManager().subscribeToTopic(topicSubscriber.getTopicName(), autoDiscPubTransport, this);

        // Register the topic information in auto-discovery
        this.registerTopicInfoInAutodiscovery(topicSubscriber);
    }

    /**
     * Unsubscribe from the given topic name
     *
     * @param topicName the name of the topic to destroyTopicSubscriber from
     * @throws VegaException exception thrown if there is a problem or if not previously subscribed
     */
    public void unsubscribeFromTopic(final String topicName) throws VegaException
    {
        log.info("Unsubscribing from topic [{}]", topicName);

        synchronized (this.lock)
        {
            if (this.closed)
            {
                log.error("Trying to unsubscribe from topic but the manager has been already closed");
                throw new VegaException("Trying to unsubscribe from topic but the manager has been already closed");
            }

            // Get the topic subscriber
            final TopicSubscriber topicSubscriber = this.topicSubscribersByTopicName.get(topicName);
            if (topicSubscriber == null || !topicSubscriber.removeNormalListener())
            {
                log.error("No subscriber found for topic [{}]", topicName);
                throw new VegaException("There is no subscriber for topic " + topicName);
            }

            // If no more listeners, remove and destroy the topic subscriber
            this.removeAndDestroyIfNoMoreListeners(topicSubscriber);
        }
    }

    /**
     * Unsubscribe from the given topic subscriber
     *
     * @param topicSubscriber topic subscriber to unsubscribe from
     */
    private void destroyTopicSubscriber(final TopicSubscriber topicSubscriber)
    {
        // Unregister the topic info from auto-discovery
        this.unRegisterTopicInfoFromAutodiscovery(topicSubscriber);

        // Get the autodiscovery transport type for publishers on the same transport
        final AutoDiscTransportType autoDiscPubTransport = this.convertToPubAutodiscTransportType(topicSubscriber.getTopicConfig().getTransportType());

        // Unsubscribe from adverts from publishers on the same transport
        this.vegaContext.getAutodiscoveryManager().unsubscribeFromTopic(topicSubscriber.getTopicName(), autoDiscPubTransport, this);

        // Destroy the topic subscriber
        this.processTopicSubscriberBeforeDestroy(topicSubscriber);

        // Delete relation between topic publisher and topic subscribers for this subscriber name
        this.topicSubAndTopicPubIdRelations.removeTopicSubscriber(topicSubscriber);

        // If is a secure topic, notify about the removal
        if (topicSubscriber.hasSecurity())
        {
            this.subSecurityNotifier.removedSecureSubTopic(topicSubscriber.getUniqueId());
        }

        // Close the subscriber
        topicSubscriber.close();
    }

    /**
     * Called when there is a new publisher for a topic pattern
     *
     * @param pubTopicInfo the new publisher topic information
     * @param topicPattern the pattern
     * @param securityConfig topic security config, null if no security has been defined for the topic
     * @param templateCfg the configuration for the topic
     * @param listener the listener for incomming messages
     */
    protected void onNewPubTopicForPattern(final AutoDiscTopicInfo pubTopicInfo, final String topicPattern, final TopicTemplateConfig templateCfg, final TopicSecurityTemplateConfig securityConfig, final ITopicSubListener listener)
    {
        log.info("New pub topic [{}] for pattern [{}] received from auto-discovery", pubTopicInfo, topicPattern);

        synchronized (this.lock)
        {
            // Find the topic subscriber
            TopicSubscriber topicSubscriber = this.topicSubscribersByTopicName.get(pubTopicInfo.getTopicName());

            // Dont exists
            if (topicSubscriber == null)
            {
                // Create a new one
                if (securityConfig == null)
                {
                    topicSubscriber = new TopicSubscriber(pubTopicInfo.getTopicName(), templateCfg);
                }
                else
                {
                    topicSubscriber = new SecureTopicSubscriber(pubTopicInfo.getTopicName(), templateCfg, securityConfig);
                }

                // Add the listener
                topicSubscriber.addPatternListener(topicPattern, listener);
                // Process as a new topic subscriber
                this.processCreatedTopicSubscriber(topicSubscriber);
            }

            // Add the pattern listener
            topicSubscriber.addPatternListener(topicPattern, listener);

            // Add to the map of topic subscribers by the pattern subscription related
            this.topicSubsByPatternSubs.put(topicPattern, topicSubscriber);
        }
    }

    /**
     * Called when there is a publisher for a topic pattern has been removed
     *
     * @param topicName the name of the pub topic that match the pattern
     * @param topicPattern the pattern
     */
    public void onPubTopicForPatternRemoved(final String topicName, final String topicPattern)
    {
        log.info("Pub topic [{}] for pattern [{}] timed out in auto-discovery", topicName, topicPattern);

        synchronized (this.lock)
        {
            // Get the topic subscriber
            final TopicSubscriber topicSubscriber = this.topicSubscribersByTopicName.get(topicName);

            // Remove the pattern listener
            topicSubscriber.removePatternListener(topicPattern);

            // Remove from the map of topic subscribers by the pattern subscription related
            this.topicSubsByPatternSubs.remove(topicPattern, topicSubscriber);

            // Remove and destroy if no more listeners
            this.removeAndDestroyIfNoMoreListeners(topicSubscriber);
        }
    }

    /**
     * Remove and destroy the topic subscriber if there are no more listeners attached
     *
     * @param topicSubscriber the topic subscriber
     */
    private void removeAndDestroyIfNoMoreListeners(final TopicSubscriber topicSubscriber)
    {
        // If no more listeners, remove and destroy the topic subscriber
        if (topicSubscriber.hasNoListeners())
        {
            this.topicSubscribersByTopicName.remove(topicSubscriber.getTopicName());
            this.destroyTopicSubscriber(topicSubscriber);
        }
    }

    /**
     * Called when there has been an un-subscription from a topic pattern. It will remove all the added listeners
     * due to putForTopicPatternAdded and delete the topic subscribers if no more listeners attached
     *
     * @param topicPattern the topic pattern that has been unsubscribed
     */
    void onTopicPatternUnsubscription(final String topicPattern)
    {
        log.info("Topic pattern [{}] unsubscribed", topicPattern);

        synchronized (this.lock)
        {
            // Remove the listeners and destroy the TopicSubscribers if required
            this.topicSubsByPatternSubs.removeAndConsumeIfKeyEquals(topicPattern, topicSubscriber ->
            {
                topicSubscriber.removePatternListener(topicPattern);
                this.removeAndDestroyIfNoMoreListeners(topicSubscriber);
            });
        }
    }

    @Override
    public void onNewAutoDiscTopicInfo(final AutoDiscTopicInfo pubTopicInfo)
    {
        synchronized (this.lock)
        {
            if (this.closed)
            {
                return;
            }

            final TopicSubscriber topicSubscriber = this.topicSubscribersByTopicName.get(pubTopicInfo.getTopicName());

            if (topicSubscriber == null)
            {
                return;
            }

            // Check the security to decide if the topic should be filtered. Both topics should have or not have security, if secured the security id of
            // the publisher should be in the list of valid id's for the subscriber
            if (!performSecurityFilter(topicSubscriber, pubTopicInfo))
            {
                return;
            }

            // Store the relationship between publisher and subscriber
            this.topicSubAndTopicPubIdRelations.addTopicPubRelation(pubTopicInfo.getUniqueId(), topicSubscriber);

            // If the publisher uses encryption and the subscriber as well, register to obtain the session key
            if (pubTopicInfo.hasSecurity() && topicSubscriber.hasSecurity())
            {
                this.subSecurityNotifier.addedPubForSubTopic(pubTopicInfo, topicSubscriber.getUniqueId(), ((SecureTopicSubscriber) topicSubscriber).getTopicSecurityConfig());
            }
        }
    }

    /**
     *  Check the security to decide if the topic should be filtered. Both topics should have or not have security, if secured the security id of
     *  the publisher should be in the list of valid id's for the subscriber.
     *
     * @param topicSubscriber the existing topic subscriber
     * @param pubTopicInfo the new topic publisher information
     * @return true if it pass the security filger
     */
    private boolean performSecurityFilter(final TopicSubscriber topicSubscriber, final AutoDiscTopicInfo pubTopicInfo)
    {
        // If the topic subscriber is secured
        if (topicSubscriber.hasSecurity())
        {
            final SecureTopicSubscriber secureTopicSubscriber = (SecureTopicSubscriber)topicSubscriber;

            // The topic subscriber has security configured, the sub topic socket should have as well
            if (!pubTopicInfo.hasSecurity())
            {
                log.warn("Non-secured new PubTopicInfo event received but the subscriber has security configured. {}", pubTopicInfo);
                return false;
            }

            // The publisher secure id should be in the configured list of valid secure id's
            if(!secureTopicSubscriber.isTopicPubSecureIdAllowed(pubTopicInfo.getSecurityId()))
            {
                log.warn("New secure publisher auto-discovery topic info received but the subscriber don't have the security id on the list of valid id's or is not secured at all. {}", pubTopicInfo);
                return false;
            }

            // Finally we should have the public key of the publisher
            if (!this.vegaContext.getSecurityContext().getRsaCrypto().isSecurityIdRegistered(pubTopicInfo.getSecurityId()))
            {
                // If is in the list, the public key should have been loaded as well
                log.warn("New secure publisher auto-discovery topic info received but the public key cannot be found in any of the pub keys files. {}", pubTopicInfo);
                return false;
            }

            return true;
        }
        else if (pubTopicInfo.hasSecurity())
        {
            log.warn("New non secure publisher auto-discovery topic info received but the subscriber has security configured. {}", pubTopicInfo);
            return false;
        }

        return true;
    }

    @Override
    public void onTimedOutAutoDiscTopicInfo(final AutoDiscTopicInfo pubTopicInfo)
    {
        synchronized (this.lock)
        {
            if (this.closed)
            {
                return;
            }

            final TopicSubscriber topicSubscriber = this.topicSubscribersByTopicName.get(pubTopicInfo.getTopicName());

            if (topicSubscriber == null)
            {
                return;
            }

            this.topicSubAndTopicPubIdRelations.removeTopicPubRelation(pubTopicInfo.getUniqueId(), topicSubscriber);

            // If the publisher uses encryption and the subscriber as well, register to obtain the session key
            if (pubTopicInfo.hasSecurity() && topicSubscriber.hasSecurity())
            {
                this.subSecurityNotifier.removedPubForSubTopic(pubTopicInfo, topicSubscriber.getUniqueId());
            }
        }
    }

    /**
     * Register the created topic subscriber information in auto-discovery
     * @param topicSubscriber the topic subscriber which information is going to be registered
     */
    private void registerTopicInfoInAutodiscovery(final TopicSubscriber topicSubscriber)
    {
        // Create the info and store
        final AutoDiscTopicInfo autoDiscTopicInfo = new AutoDiscTopicInfo(
                this.vegaContext.getInstanceUniqueId(),
                this.convertToSubAutodiscTransportType(topicSubscriber.getTopicConfig().getTransportType()),
                topicSubscriber.getUniqueId(),
                topicSubscriber.getTopicName(),
                topicSubscriber.hasSecurity() ? this.vegaContext.getSecurityContext().getSecurityId() : AutoDiscTopicInfo.NO_SECURED_CONSTANT);

        this.registeredTopicInfosByTopicId.put(autoDiscTopicInfo.getUniqueId(), autoDiscTopicInfo);

        // Register in auto-discovery
        this.vegaContext.getAutodiscoveryManager().registerTopicInfo(autoDiscTopicInfo);
    }

    /**
     * Unregister the created topic subscriber information from auto-discovery
     * @param topicSubscriber the topic subscriber which information is going to be unregsitered
     */
    private void unRegisterTopicInfoFromAutodiscovery(final TopicSubscriber topicSubscriber)
    {
        // Find the stored topic info and unregisterInstanceInfo
        final AutoDiscTopicInfo autoDiscTopicInfo = registeredTopicInfosByTopicId.get(topicSubscriber.getUniqueId());
        if (autoDiscTopicInfo != null)
        {
            this.vegaContext.getAutodiscoveryManager().unregisterTopicInfo(autoDiscTopicInfo);
        }
    }

    /**
     * Create the pub autodiscovery transport type using the transport media type given. It will just add the direction.
     * @param transportMediaType the transport media type
     * @return the autodiscovery transport type equivalent
     */
    private AutoDiscTransportType convertToPubAutodiscTransportType(final TransportMediaType transportMediaType)
    {
        switch (transportMediaType)
        {
            case MULTICAST: return AutoDiscTransportType.PUB_MUL;
            case IPC: return AutoDiscTransportType.PUB_IPC;
            case UNICAST: return AutoDiscTransportType.PUB_UNI;
            default: return null;
        }
    }

    /**
     * Create the sub autodiscovery transport type using the transport media type given. It will just add the direction.
     * @param transportMediaType the transport media type
     * @return the autodiscovery transport type equivalent
     */
    private AutoDiscTransportType convertToSubAutodiscTransportType(final TransportMediaType transportMediaType)
    {
        switch (transportMediaType)
        {
            case MULTICAST: return AutoDiscTransportType.SUB_MUL;
            case IPC: return AutoDiscTransportType.SUB_IPC;
            case UNICAST: return AutoDiscTransportType.SUB_UNI;
            default: return null;
        }
    }

    /**
     * Return the stored topic subscriber for the given topic name. Stored topic subscriber are the ones created by "onNewPubTopicForPattern" call
     *
     * @param topicName the name of the topic
     * @return the stored topic subscriber
     */
    TopicSubscriber getTopicSubscriberForTopicName(final String topicName)
    {
        return this.topicSubscribersByTopicName.get(topicName);
    }
}