package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.autodiscovery.subscriber.IAutodiscTopicSubListener;
import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.control.IOwnSecPubTopicsChangesListener;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


/**
 * Base class for publishers manager. The subscribers manager handles the topic publishers that are active in the system.
 *
 * Specific implementations are created for multicast, unicast and ipc.
 *
 * This class is thread safe!
 */
@Slf4j
abstract class AbstractPublishersManager<T extends AbstractTopicPublisher> implements Closeable, IAutodiscTopicSubListener
{
    /** Context of the library instance  */
    @Getter(AccessLevel.PROTECTED)
    private final VegaContext vegaContext;

    /** Stores the topic publishers by the topic name used to create them  */
    private final Map<String, T> topicPublishersByTopicName = new HashMap<>();

    /** Stores all the registered auto-discovery topic infos for quick lookup */
    private final Map<UUID, AutoDiscTopicInfo> registeredTopicInfosByTopicId = new HashMap<>();

    /** True if it has been closed */
    @Getter(AccessLevel.PROTECTED)
    private boolean closed = false;

    /** Notifier to call when there is a change on a secure topic */
    final IOwnSecPubTopicsChangesListener secureChangesNotifier;

    /** Lock for class access */
    final Object lock = new Object();

    /**
     * Constructor
     *
     * @param vegaContext the context of the library instance
     * @param secureChangesNotifier to call when there is a change on a secure topic
     *
     */
    AbstractPublishersManager(final VegaContext vegaContext, final IOwnSecPubTopicsChangesListener secureChangesNotifier)
    {
        this.vegaContext = vegaContext;
        this.secureChangesNotifier = secureChangesNotifier;
    }

    /**
     * Process a created topic publisher for additional actions
     * @param topicPublisher the topic publisher to process
     */
    protected abstract void processCreatedTopicPublisher(T topicPublisher);

    /**
     * Instantiate a new topic publisher instance
     * @param topicName the name of the topic
     * @param templateCfg the topic configuraton

     * @return the created instance
     */
    protected abstract T instantiateTopicPublisher(String topicName, TopicTemplateConfig templateCfg);

    /**
     * Instantiate a new secure topic publisher instance
     * @param topicName the name of the topic
     * @param templateCfg the topic configuraton
     * @param securityTemplateConfig security template configuration, null if not settled
     * @return the created instance
     */
    protected abstract T instantiateSecureTopicPublisher(String topicName, TopicTemplateConfig templateCfg, TopicSecurityTemplateConfig securityTemplateConfig) throws VegaException;

    /**
     * Process a topic publisher that is ging to be destroyed for additional actions
     * @param topicPublisher the topic publisher that is going to be destroyed
     */
    protected abstract void processTopicPublisherBeforeDestroy(T topicPublisher);

    /**
     * Clean any additional information after closing the manager
     */
    protected abstract void cleanAfterClose();

    @Override
    public void close()
    {
        synchronized (this.lock)
        {
            // Ignore if already closed, should not happen but just in case
            if (this.closed)
            {
                return;
            }

            // Destroy all the topic publishers registered
            this.topicPublishersByTopicName.values().forEach(this::destroyTopicPublisher);
            this.topicPublishersByTopicName.clear();

            // It should be empty but clear anyway
            this.registeredTopicInfosByTopicId.clear();

            // Clean internal information of specific instances
            this.cleanAfterClose();

            this.closed = true;
        }
    }

    /**
     * Called when the user creates a new topic publisher.
     *
     * It will create a new AeronPublisher or reuse an existing one and register the TopicInfo in autodiscovery.
     * It will also add the publisher to the topic publisher.
     *
     * @param topicName to user created topic publisher
     * @param templateCfg template configuration
     * @param securityTemplateConfig security template configuration, null if no security is configured for the publisher topic
     *
     * @return the created topic publisher
     */
    T createTopicPublisher(final String topicName, final TopicTemplateConfig templateCfg, final TopicSecurityTemplateConfig securityTemplateConfig) throws VegaException
    {
        synchronized (this.lock)
        {
            // Make sure that is not closed
            if (this.closed)
            {
                log.error("Trying to create a publisher but the manager has been already closed");
                throw new VegaException("Trying to create a publisher but the manager has been already closed");
            }

            // Check if already created
            if (this.topicPublishersByTopicName.containsKey(topicName))
            {
                log.error("There is already a publisher created for topic [{}]", topicName);
                throw new VegaException("There is already a publisher created for topic " + topicName);
            }

            // Create the right type of topic publisher and store
            T topicPublisher;
            if (securityTemplateConfig == null)
            {
                topicPublisher = this.instantiateTopicPublisher(topicName, templateCfg);
            }
            else
            {
                topicPublisher = this.instantiateSecureTopicPublisher(topicName, templateCfg, securityTemplateConfig);
            }

            this.topicPublishersByTopicName.put(topicName, topicPublisher);

            // Get the autodiscovery transport type for subscribers on the same transport
            final AutoDiscTransportType autoDiscSubTransport = this.convertToSubAutodiscTransportType(topicPublisher.getTopicConfig().getTransportType());

            // Subscribe to topic adverts for subscribers in auto-discovery with the same transport type
            this.vegaContext.getAutodiscoveryManager().subscribeToTopic(topicPublisher.getTopicName(), autoDiscSubTransport, this);

            // Register the new topic info in auto-discovery
            this.registerTopicInfoInAutodiscovery(topicPublisher);

            // Process created topic publisher
            this.processCreatedTopicPublisher(topicPublisher);

            // Return the created topic publisher
            return topicPublisher;
        }
    }

    /**
     * Destroys the topic publisher that matches the given topic name
     *
     * It will remove the AeronPublishers from the topic publisher and destroy the aeron publisher if no more topics are attached to it.
     *
     * @param topicName the deleted topic publisher for the given name
     */
    void destroyTopicPublisher(final String topicName) throws VegaException
    {
        synchronized (this.lock)
        {
            if (this.closed)
            {
                log.error("Trying to create a publisher but the manager has been already closed");
                throw new VegaException("Trying to destroy a publisher but the manager has been already closed");
            }

            // Find and remove from internal map
            final T topicPublisher = this.topicPublishersByTopicName.remove(topicName);
            if (topicPublisher == null)
            {
                log.error("Cannot find any topic publisher for name [{}]", topicName);
                throw new VegaException("No topic publisher found for name " + topicName);
            }

            this.destroyTopicPublisher(topicPublisher);
        }
    }

    /**
     * Destroys the topic publisher
     *
     * It will remove the AeronPublishers from the topic publisher and destroy the aeron publisher if no more topics are attached to it.
     *
     * @param topicPublisher the topic publisher to destroy
     */
    private void destroyTopicPublisher(final T topicPublisher)
    {
        // Unregister the topic publisher from auto-discovery
        this.unRegisterTopicInfoFromAutodiscovery(topicPublisher);

        // Get the autodiscovery transport type for subscribers on the same transport
        final AutoDiscTransportType autoDiscSubTransport = this.convertToSubAutodiscTransportType(topicPublisher.getTopicConfig().getTransportType());

        // Unsubscribe from topic adverts in auto-discovery
        this.vegaContext.getAutodiscoveryManager().unsubscribeFromTopic(topicPublisher.getTopicName(), autoDiscSubTransport, this);

        this.processTopicPublisherBeforeDestroy(topicPublisher);
    }

    /**
     * Register the topic information in auto-discovery
     * @param topicPublisher the topic publisher which information is going to be registered
     */
    private void registerTopicInfoInAutodiscovery(final T topicPublisher)
    {
        // Create the right autodiscovery transport type
        final AutoDiscTransportType autoDiscTransportType = this.convertToPubAutodiscTransportType(topicPublisher.getTopicConfig().getTransportType());

        // Create the info and store
        final AutoDiscTopicInfo autoDiscTopicInfo = new AutoDiscTopicInfo(
                this.getVegaContext().getInstanceUniqueId(),
                autoDiscTransportType,
                topicPublisher.getUniqueId(),
                topicPublisher.getTopicName(),
                topicPublisher.hasSecurity() ? this.vegaContext.getSecurityContext().getSecurityId() : AutoDiscTopicInfo.NO_SECURED_CONSTANT);

        this.registeredTopicInfosByTopicId.put(autoDiscTopicInfo.getUniqueId(), autoDiscTopicInfo);

        // Register in auto-discovery
        this.vegaContext.getAutodiscoveryManager().registerTopicInfo(autoDiscTopicInfo);
    }

    /**
     * Unregister the topic information from auto-discovery
     * @param topicPublisher the topic publisher which information is going to be unregistered
     */
    private void unRegisterTopicInfoFromAutodiscovery(final T topicPublisher)
    {
        // Find the stored topic info and unregisterInstanceInfo
        final AutoDiscTopicInfo autoDiscTopicInfo = registeredTopicInfosByTopicId.remove(topicPublisher.getUniqueId());
        if (autoDiscTopicInfo != null)
        {
            this.vegaContext.getAutodiscoveryManager().unregisterTopicInfo(autoDiscTopicInfo);
        }
    }

    /**
     * Return the stored topic publisher for the given topic name. Stored topic publishers are the ones created by "createTopicPublisher" call
     *
     * @param topicName the name of the topic
     * @return the stores topic publisher
     */
    T getTopicPublisherForTopicName(final String topicName)
    {
        return this.topicPublishersByTopicName.get(topicName);
    }

    /**
     * Create the pub autodiscovery transport type using the transport media type given. It will just add the direction.
     * @param transportMediaType the transport media type
     * @return the autodiscovery transport type equivalent
     */
    AutoDiscTransportType convertToPubAutodiscTransportType(final TransportMediaType transportMediaType)
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
}
