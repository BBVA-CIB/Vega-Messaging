package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.subscriber.IAutodiscInstanceListener;
import com.bbva.kyof.vega.autodiscovery.subscriber.IAutodiscPubTopicPatternListener;
import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.msg.RcvMessage;
import com.bbva.kyof.vega.msg.RcvRequest;
import com.bbva.kyof.vega.msg.RcvResponse;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.control.ISecuredMsgsDecoder;
import com.bbva.kyof.vega.protocol.control.ISecurityRequesterNotifier;
import com.bbva.kyof.vega.protocol.publisher.AeronPublisher;
import com.bbva.kyof.vega.util.crypto.AESCrypto;
import lombok.extern.slf4j.Slf4j;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Main class to handle the receiving functionality on the framework. It contains separate managers for unicast messaging and multicast/ipc messaging.
 * The reason fo this separation is that in unicast the subscriber is the end point and in multicast/ipc is the opposite. This creates a different required
 * behaviour for both cases.<p>
 *
 * It also keep track of the created subscribers and configurations. <p>
 *
 * This class is thread safe!!
 */
@Slf4j
public final class ReceiveManager implements ISubscribersPollerListener, Closeable, IAutodiscInstanceListener, IAutodiscPubTopicPatternListener
{
    /** Configuration is available in this object */
    private final VegaContext vegaContext;

    /** Manager for unicast reception */
    private final SubscribersManagerUnicast subscribersManagerUnicast;

    /** Manager for multicast and ipc reception */
    private final SubscribersManagerIpcMcast subscribersManagerIpcMcast;

    /** Manager for response publishers */
    private final ResponsePublishersManager responsePublishersManager;

    /** Security manager for subscriptions, it will be usd to decrypt messages */
    private final ISecuredMsgsDecoder subSecurityManager;

    /**
     * Stores the relationships between topic subscribers and topic publishers for quick lookup on incomming messages.
     * This information comes from the auto-discovery mechanism
     */
    private final TopicSubAndTopicPubIdRelations topicSubAndTopicPubIdRelations = new TopicSubAndTopicPubIdRelations();

    /** Listeners for all the subscribed patterns */
    private final Map<String, ITopicSubListener> listenersByTopicPattern = new HashMap<>();

    /** Manager that handles all the pollers */
    private final SubscribersPollersManager pollersManager;

    /** Content for a sent heartbeat response */
    private final UnsafeBuffer heartbeatRespContent = new UnsafeBuffer(new byte[0]);

    /** Lock for class synchronization */
    private final Object lock = new Object();

    /** True if it has been closed */
    private boolean isClosed = false;

    /**
     *  Constructor of the class
     *
     * @param vegaContext context of the instance
     * @param subSecurityManager security manager for the subscribers
     * @param securityRequesterNotifier notifier to tell about new security requests
     * @throws VegaException exception thrown if there is a problem creating the manager
     */
    public ReceiveManager(final VegaContext vegaContext, final ISecuredMsgsDecoder subSecurityManager, final ISecurityRequesterNotifier securityRequesterNotifier) throws VegaException
    {
        this.vegaContext = vegaContext;
        this.subSecurityManager = subSecurityManager;

        // Create the pollers manager
        this.pollersManager = new SubscribersPollersManager(vegaContext, this);

        // Create the managers to receive messages
        this.subscribersManagerUnicast = new SubscribersManagerUnicast(vegaContext, this.pollersManager, this.topicSubAndTopicPubIdRelations, securityRequesterNotifier);
        this.subscribersManagerIpcMcast = new SubscribersManagerIpcMcast(vegaContext, this.pollersManager, this.topicSubAndTopicPubIdRelations, securityRequesterNotifier);

        // Create the manager for response publishers
        this.responsePublishersManager = new ResponsePublishersManager(vegaContext);

        // Subscribe to instance info changes
        this.vegaContext.getAutodiscoveryManager().subscribeToInstances(this);
    }

    /**
     * Subscribe to the given topic in order to get messages and responses for the topic name
     * @param topicName the name of the topic to onNewPubTopicForPattern to
     * @param listener listener that will process incoming messages on the topic
     *
     * @throws VegaException exception thrown if there is a problem or already subscribed
     */
    public void subscribeToTopic(final String topicName, final ITopicSubListener listener) throws VegaException
    {
        log.info("Subscribing to topic [{}]", topicName);

        synchronized (this.lock)
        {
            if (this.isClosed)
            {
                log.error("Cannot subscribe to topic [{}] on a closed manager", topicName);
                throw new VegaException("Trying to subscribe to topic on a closed manager");
            }

            // Find the template configuration
            final TopicTemplateConfig templateCfg = this.findTopicConfigAndFailIfNotFound(topicName);

            // Find the security template configuration, it may be null if not configured
            final TopicSecurityTemplateConfig securityTemplateConfig = this.findTopicSecurityConfig(topicName);

            // Verify the topic subscriber security
            this.verifyTopicSubscriberSecurityOrFail(securityTemplateConfig);

            // Call the right manager to do the rest of the job
            switch (templateCfg.getTransportType())
            {
                case UNICAST:
                    this.subscribersManagerUnicast.subscribeToTopic(topicName, templateCfg, securityTemplateConfig, listener);
                    break;
                case MULTICAST:
                case IPC:
                    this.subscribersManagerIpcMcast.subscribeToTopic(topicName, templateCfg, securityTemplateConfig, listener);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Verify the topic subscriber security. If security is enabled, it check if the application secure id match a valid secure id for the topic
     * @param securityTemplateConfig security configuration for the topic
     * @throws VegaException exception thrown if the security is not valid
     */
    private void verifyTopicSubscriberSecurityOrFail(final TopicSecurityTemplateConfig securityTemplateConfig) throws VegaException
    {
        if (securityTemplateConfig != null && !securityTemplateConfig.getSubSecIds().contains(this.vegaContext.getSecurityContext().getSecurityId()))
        {
            log.error("Trying to subscribe to a secured topic, but the application secure id is not in the configuration list of valid subscribers");
            throw new VegaException("The current security id is not configured in the list of sub secure ids for the topic");
        }
    }

    /**
     * Unsubscribe from the given topic name
     *
     * @param topicName the name of the topic to onPubTopicForPatternRemoved from
     * @throws VegaException exception thrown if there is a problem or if not previously subscribed
     */
    public void unsubscribeFromTopic(final String topicName) throws VegaException
    {
        log.info("Unsubscribing from topic [{}]", topicName);

        synchronized (this.lock)
        {
            if (this.isClosed)
            {
                log.error("Cannot unsubscribe from topic [{}] on a closed manager", topicName);
                throw new VegaException("Trying to unsubscribe from topic on a closed manager");
            }

            // Find the template configuration
            final TopicTemplateConfig templateCfg = this.findTopicConfigAndFailIfNotFound(topicName);

            // Call the right manager to do the rest of the job
            switch (templateCfg.getTransportType())
            {
                case UNICAST:
                    this.subscribersManagerUnicast.unsubscribeFromTopic(topicName);
                    break;
                case MULTICAST:
                case IPC:
                    this.subscribersManagerIpcMcast.unsubscribeFromTopic(topicName);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Subscribe to receive messages and requests from any topic that match the given pattern
     *
     * @param topicPattern the topic pattern to subscribe to
     * @param listener the listener for messages that arrive to topics that match the pattern
     * @throws VegaException exception thrown if closed or already subscribed to the pattern
     */
    public void subscribeToPattern(final String topicPattern, final ITopicSubListener listener) throws VegaException
    {
        synchronized (this.lock)
        {
            if (this.isClosed)
            {
                log.error("Cannot subscribe to patterns on a closed manager");
                throw new VegaException("Trying to subscribe to pattern on a closed manager");
            }

            // Ensure it is not already subscribed
            if (this.listenersByTopicPattern.containsKey(topicPattern))
            {
                log.error("Trying to subscribe to the topic pattern [{}] twice", topicPattern);
                throw new VegaException("Trying to subscribe twice to the same topic pattern");
            }

            // Add and subscribe in auto-discovery
            this.listenersByTopicPattern.put(topicPattern, listener);
            this.vegaContext.getAutodiscoveryManager().subscribeToPubTopicPattern(topicPattern, this);
        }
    }

    /**
     * Unsubscribe to stop receive messages and requests from a topic pattern
     *
     * @param topicPattern the topic pattern to unsubscribe from
     * @throws VegaException exception thrown if closed or not subscribed to the pattern
     */
    public void unsubscribefromPattern(final String topicPattern) throws VegaException
    {
        synchronized (this.lock)
        {
            if (this.isClosed)
            {
                log.error("Cannot unsubscribe from patterns on a closed manager");
                throw new VegaException("Trying to unsubscribe from pattern on a closed manager");
            }

            if (this.listenersByTopicPattern.remove(topicPattern) == null)
            {
                log.error("Trying to unsubscribe from the topic pattern [{}], but is not subscribed", topicPattern);
                throw new VegaException("Trying to unsubscribe from a non subscribed topic pattern");
            }
            else
            {
                // Remove auto-discovery subscription
                this.vegaContext.getAutodiscoveryManager().unsubscribeFromPubTopicPattern(topicPattern);

                // Tell the specific managers about the unsubscription
                this.subscribersManagerUnicast.onTopicPatternUnsubscription(topicPattern);
                this.subscribersManagerIpcMcast.onTopicPatternUnsubscription(topicPattern);
            }
        }
    }

    @Override
    public void onNewPubTopicForPattern(final AutoDiscTopicInfo pubTopicInfo, final String topicPattern)
    {
        synchronized (this.lock)
        {
            if (this.isClosed)
            {
                return;
            }

            // Get the listener for the pattern, it can be null if it has been removed but there was a pending auto-discovery event
            final ITopicSubListener listener = this.listenersByTopicPattern.get(topicPattern);
            if (listener == null)
            {
                return;
            }

            // Find the template configuration
            final TopicTemplateConfig templateCfg = this.findTopicConfig(pubTopicInfo.getTopicName());

            // If there is no configuration for the topic, just ignore the event
            if (templateCfg == null)
            {
                return;
            }

            // Find the security template configuration, will be null if not configured
            final TopicSecurityTemplateConfig securityTemplateConfig = this.findTopicSecurityConfig(pubTopicInfo.getTopicName());

            // Perform a security filter, both should be secured or non secured, and if secured the security id should be configured
            if (!this.performNewPubForPatternSecurityFilter(securityTemplateConfig, topicPattern, pubTopicInfo))
            {
                return;
            }

            // Call the right manager to do the rest of the job
            switch (templateCfg.getTransportType())
            {
                case UNICAST:
                    this.subscribersManagerUnicast.onNewPubTopicForPattern(pubTopicInfo, topicPattern, templateCfg, securityTemplateConfig, listener);
                    break;
                case MULTICAST:
                case IPC:
                    this.subscribersManagerIpcMcast.onNewPubTopicForPattern(pubTopicInfo, topicPattern, templateCfg, securityTemplateConfig, listener);
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public void onPubTopicForPatternRemoved(final AutoDiscTopicInfo pubTopicInfo, final String topicPattern)
    {
        synchronized (this.lock)
        {
            if (this.isClosed)
            {
                return;
            }

            // Get the listener for the pattern, it can be null if it has been removed but there was a pending auto-discovery event
            final ITopicSubListener listener = this.listenersByTopicPattern.get(topicPattern);
            if (listener == null)
            {
                return;
            }

            // Find the template configuration
            final TopicTemplateConfig templateCfg = this.findTopicConfig(pubTopicInfo.getTopicName());

            // If there is no configuration for the topic, just ignore the event
            if (templateCfg == null)
            {
                return;
            }

            // Call the right manager to do the rest of the job
            switch (templateCfg.getTransportType())
            {
                case UNICAST:
                    this.subscribersManagerUnicast.onPubTopicForPatternRemoved(pubTopicInfo.getTopicName(), topicPattern);
                    break;
                case MULTICAST:
                case IPC:
                    this.subscribersManagerIpcMcast.onPubTopicForPatternRemoved(pubTopicInfo.getTopicName(), topicPattern);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Find the topic configuration for the given topic name
     *
     * @param topicName the topic name to find the configuration for
     * @return the template configuration, null if not found
     */
    private TopicTemplateConfig findTopicConfig(final String topicName)
    {
        // Find the subscriber configuration given the topic
        return this.vegaContext.getInstanceConfig().getTopicTemplateForTopic(topicName);
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
     * Find the topic configuration for the given topic name, throw an exception if it cannot be found
     *
     * @param topicName the topic name to find the configuration for
     * @return an optional with the found configuration
     * @throws VegaException exception thrown if the configuration cannot be found
     */
    private TopicTemplateConfig findTopicConfigAndFailIfNotFound(final String topicName) throws VegaException
    {
        // Find the subscriber configuration given the topic
        final TopicTemplateConfig config = this.findTopicConfig(topicName);

        if (config == null)
        {
            log.error("No configuration found for topic [{}]", topicName);
            throw new VegaException("Cannot find any valid configuration for topic " + topicName);
        }

        return config;
    }

    /**
     *  Check the security to decide if the topic should be filtered. Both topics should have or not have security, if secured the security id of
     *  the publisher should be in the list of valid id's for the subscriber.
     *
     * @param securityTemplate the security template for the topic
     * @param pattern the pattern for the pattern subscription that has originated the call
     * @param pubTopicInfo the new topic publisher information
     * @return true if it pass the security filter
     */
    private boolean performNewPubForPatternSecurityFilter(final TopicSecurityTemplateConfig securityTemplate, final String pattern, final AutoDiscTopicInfo pubTopicInfo)
    {
        // There is a security template for the topic
        if (securityTemplate != null)
        {
            // First make sure we have permissions to subscribe to the secure topic
            if (!securityTemplate.getSubSecIds().contains(this.vegaContext.getSecurityContext().getSecurityId()))
            {
                log.warn("New secure publisher auto-discovery topic info received for pattern subscription [{}] but the application is not allowed to subscribe to the secured topic {}", pattern, pubTopicInfo);
                return false;
            }

            // Now check that the event is also from a secure source
            if (!pubTopicInfo.hasSecurity())
            {
                log.warn("New non secure auto-discovery topic info received for pattern subscription [{}] but the topic configuration has security enabled {}", pattern, pubTopicInfo);
                return false;
            }

            // Make sure the secure source is in the list of valid secure id's
            if(!securityTemplate.getPubSecIds().contains(pubTopicInfo.getSecurityId()))
            {
                log.warn("New secure publisher auto-discovery topic info received for pattern subscription [{}] but the subscriber don't have the security id on the list of valid pub id's. {}", pattern, pubTopicInfo);
                return false;
            }

            // Make sure we have the public key of the source
            if(!this.vegaContext.getSecurityContext().getRsaCrypto().isSecurityIdRegistered(pubTopicInfo.getSecurityId()))
            {
                log.warn("New secure publisher auto-discovery topic info received for pattern subscription [{}] but the public key of the publisher has not been loaded. {}", pattern, pubTopicInfo);
                return false;
            }
        }
        else if (pubTopicInfo.hasSecurity())
        {
            // The subscriber has no security, but the event comes from a secure publisher
            log.warn("New secure publisher auto-discovery topic info received for pattern subscription [{}] but the subscriber has no secure configuration. {}", pattern, pubTopicInfo);
            return false;
        }

        return true;
    }

    @Override
    public void close()
    {
        log.info("Stopping receiver manager for instance ID [{}]", this.vegaContext.getInstanceUniqueId());

        synchronized (this.lock)
        {
            // Ignore multiple close calls
            if (this.isClosed)
            {
                return;
            }

            // Unsubscribe from patterns and clear the map
            this.listenersByTopicPattern.forEach((key, value) -> this.subscribersManagerUnicast.onTopicPatternUnsubscription(key));
            this.listenersByTopicPattern.forEach((key, value) -> this.subscribersManagerIpcMcast.onTopicPatternUnsubscription(key));
            this.listenersByTopicPattern.clear();

            // Close the specific managers
            this.subscribersManagerIpcMcast.close();
            this.subscribersManagerUnicast.close();

            // Stop all pollers to prevent more messaging processing
            this.pollersManager.close();

            // Unusbscribe from instance info changes
            this.vegaContext.getAutodiscoveryManager().unsubscribeFromInstances(this);

            // Clean relationships map
            this.topicSubAndTopicPubIdRelations.clear();

            this.isClosed = true;
        }

        log.info("Subscriptions manager stopped");
    }

    @Override
    public void onNewAutoDiscInstanceInfo(final AutoDiscInstanceInfo info)
    {
        log.debug("New autodisc inscance info event received {}", info);

        synchronized (this.lock)
        {
            if (this.isClosed)
            {
                return;
            }

            this.responsePublishersManager.onNewAutoDiscInstanceInfo(info);
        }
    }

    @Override
    public void onTimedOutAutoDiscInstanceInfo(final AutoDiscInstanceInfo info)
    {
        log.debug("New timed out autodisc instance info event received {}", info);

        synchronized (this.lock)
        {
            if (this.isClosed)
            {
                return;
            }

            this.responsePublishersManager.onTimedOutAutoDiscInstanceInfo(info);
        }
    }

    /** @return the parameters of the Aeron Subscriber that handle responses */
    public AeronSubscriberParams getResponseSubscriberParams()
    {
        return this.subscribersManagerUnicast.getResponsesSubscriberParams();
    }

    @Override
    public void onDataMsgReceived(final RcvMessage msg)
    {
        // Find the related topic subscriber if any
        final TopicSubscriber topicSubscriber = this.topicSubAndTopicPubIdRelations.getTopicSubscriberForTopicPublisherId(msg.getTopicPublisherId());
        if (topicSubscriber != null)
        {
            // The message is not encrypted, make sure the topic has no security
            if (topicSubscriber.hasSecurity())
            {
                log.warn("Non encrypted message received on a secured topic subscriber. {}", msg);
                return;
            }

            // Set the topic name
            msg.setTopicName(topicSubscriber.getTopicName());

            // Send to the listener
            topicSubscriber.onMessageReceived(msg);
        }
    }

    @Override
    public void onEncryptedDataMsgReceived(final RcvMessage msg)
    {
        // Find the related topic subscriber if any
        final TopicSubscriber topicSubscriber = this.topicSubAndTopicPubIdRelations.getTopicSubscriberForTopicPublisherId(msg.getTopicPublisherId());
        if (topicSubscriber != null)
        {
            // Set the topic name
            msg.setTopicName(topicSubscriber.getTopicName());

            // The message is encrypted, make sure the topic has security
            if (!topicSubscriber.hasSecurity())
            {
                log.warn("Encrypted message received on a non secured topic subscriber. {}", msg);
                return;
            }

            // Get the decoder
            final AESCrypto aesDecoder = this.subSecurityManager.getAesCryptoForSecPub(msg.getTopicPublisherId());

            // It may be null if it has not found yet, or we don't have permissions, or not configured, etc etc
            if (aesDecoder != null)
            {
                ((SecureTopicSubscriber)topicSubscriber).onSecureMsgReceived(msg, aesDecoder);
            }
        }
    }

    @Override
    public void onDataRequestMsgReceived(final RcvRequest request)
    {
        // Look for the responder socket for the given sender application instance id
        final AeronPublisher responsePublisher = this.responsePublishersManager.getResponsePublisherForInstance(request.getInstanceId());

        if (responsePublisher == null)
        {
            log.info("Request received but no responder instance id found for it in auto-discovery. Requester id [{}]", request.getInstanceId());
            return;
        }

        // Set the responder socket
        request.setRequestResponder(responsePublisher);

        // Find the topic subscriber and notify to the listener
        final TopicSubscriber topicSubscriber = this.topicSubAndTopicPubIdRelations.getTopicSubscriberForTopicPublisherId(request.getTopicPublisherId());
        if (topicSubscriber != null)
        {
            // Set the topic name
            request.setTopicName(topicSubscriber.getTopicName());
            topicSubscriber.onRequestReceived(request);
        }
    }

    @Override
    public void onHeartbeatRequestMsgReceived(final UUID senderInstanceId, final UUID requestId)
    {
        // Look for the responder socket for the given sender application instance id
        final AeronPublisher responsePublisher = this.responsePublishersManager.getResponsePublisherForInstance(senderInstanceId);

        if (responsePublisher != null)
        {
            responsePublisher.sendResponse(requestId, heartbeatRespContent, 0, 0);
        }
    }

    @Override
    public void onDataResponseMsgReceived(final RcvResponse response)
    {
        this.vegaContext.getAsyncRequestManager().processResponse(response);
    }

    /**
     * Return true if subscribed to pattern
     *
     * Method added for testing purposes
     *
     * @param pattern the pattern to look for
     * @return true if subscribed to pattern
     */
    boolean isSubscribedToPattern(final String pattern)
    {
        return this.listenersByTopicPattern.get(pattern) != null;
    }

    /**
     * Return true if subscribed to topic, due to topic subscription or pattern subscription
     *
     * Method added for testing purposes
     *
     * @param topicName the name of the topic
     * @return true if susbscribed
     */
    TopicSubscriber getTopicSubscriber(final String topicName)
    {
        // Find the template configuration
        final TopicTemplateConfig templateCfg = this.findTopicConfig(topicName);

        // If there is no configuration for the topic, just ignore the event
        if (templateCfg == null)
        {
            return null;
        }

        // Call the right manager to do the rest of the job
        switch (templateCfg.getTransportType())
        {
            case UNICAST:
                return this.subscribersManagerUnicast.getTopicSubscriberForTopicName(topicName);
            case MULTICAST:
            case IPC:
                return this.subscribersManagerIpcMcast.getTopicSubscriberForTopicName(topicName);
            default:
                return null;
        }
    }

    /**
     * Return the relations between topic subscribers and topic publishers
     *
     * Method added for testing purposes
     *
     * @return the relations between topic publishers and subscribers
     */
    TopicSubAndTopicPubIdRelations getTopicSubAndTopicPubIdRelations()
    {
        return this.topicSubAndTopicPubIdRelations;
    }
}