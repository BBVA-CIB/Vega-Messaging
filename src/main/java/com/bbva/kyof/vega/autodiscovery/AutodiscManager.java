package com.bbva.kyof.vega.autodiscovery;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.autodiscovery.publisher.AbstractAutodiscSender;
import com.bbva.kyof.vega.autodiscovery.publisher.AutodiscMcastSender;
import com.bbva.kyof.vega.autodiscovery.publisher.AutodiscUnicastSender;
import com.bbva.kyof.vega.autodiscovery.subscriber.*;
import com.bbva.kyof.vega.config.general.AutoDiscoType;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.util.threads.RecurrentTask;
import io.aeron.Aeron;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.agrona.concurrent.BackoffIdleStrategy;

import java.util.LinkedList;
import java.util.UUID;

/**
 * Main class that manages all the auto-discovery for a library instance. It handles all the reception / sending of messages
 * and all the events that should reach the rest of the library. <p>
 *
 * The implementation follows a single thread model in which all the required actions are performed in sequence. This approach reduces
 * the amount of CPU consumed and simplify the synchronization process. <p>
 *
 * The actions of registering / unregistering / subscribing / unsubscribing are performed asynchronously on the background.
 */
@Slf4j
public class AutodiscManager extends RecurrentTask implements IAutodiscManager, IAutodiscGlobalEventListener
{
    /** Handler for publishing functionality for autodiscovery */
    private final AbstractAutodiscSender autodiscPub;
    /** Handler for receiving functionality on autodiscovery */
    private final AbstractAutodiscReceiver autodiscSub;
    /** Actions that haven't been executed yet over the manager */
    private final LinkedList<AutodiscAction> pendingActions = new LinkedList<>();
    /** Unique id of the library instance this object belongs to */
    @Getter private final UUID instanceId;

    /**
     * Create a new instance of the auto-discovery manager
     *
     * @param aeron the Aeron instance
     * @param config the auto-discovery configuration
     * @param instanceId unique id of the library instance the manager belongs to
     */
    public AutodiscManager(final Aeron aeron, final AutoDiscoveryConfig config, final UUID instanceId)
    {
        // Idle strategy will be back off to use as less CPU as possible
        super(new BackoffIdleStrategy(1, 1, 1, 1));

        this.instanceId = instanceId;

        // Instantiate the right type of senders and receivers
        if (config.getAutoDiscoType() == AutoDiscoType.MULTICAST)
        {
            this.autodiscSub = new AutodiscMcastReceiver(instanceId, aeron, config, this);
            this.autodiscPub = new AutodiscMcastSender(aeron, config);
        }
        else
        {
            this.autodiscSub = new AutodiscUnicastReceiver(instanceId, aeron, config, this);
            this.autodiscPub = new AutodiscUnicastSender(aeron, config, ((AutodiscUnicastReceiver)this.autodiscSub).getDaemonClientInfo());
        }
    }

    @Override
    public void start()
    {
        super.start("AutodiscoveryManager_" + this.instanceId);
    }

    @Override
    public void registerInstanceInfo(final AutoDiscInstanceInfo instanceInfo)
    {
        log.debug("Register AutoDiscInstanceInfo, action added [{}]", instanceInfo);

        synchronized (pendingActions)
        {
            this.pendingActions.add(new AutodiscAction<>(AutodiscActionType.REGISTER_INSTANCE, instanceInfo));
        }
    }

    @Override
    public void registerTopicInfo(final AutoDiscTopicInfo autoDiscTopicInfo)
    {
        log.debug("Register AutoDiscTopicInfo, action added [{}]", autoDiscTopicInfo);

        synchronized (pendingActions)
        {
            this.pendingActions.add(new AutodiscAction<>(AutodiscActionType.REGISTER_TOPIC, autoDiscTopicInfo));
        }
    }

    @Override
    public void registerTopicSocketInfo(final AutoDiscTopicSocketInfo autoDiscTopicSocketInfo)
    {
        log.debug("Register AutoDiscTopicSocketInfo, action added [{}]", autoDiscTopicSocketInfo);

        synchronized (pendingActions)
        {
            this.pendingActions.add(new AutodiscAction<>(AutodiscActionType.REGISTER_TOPIC_SOCKET, autoDiscTopicSocketInfo));
        }
    }

    @Override
    public void unregisterInstanceInfo()
    {
        log.debug("Unregister AutoDiscInstanceInfo, action added");

        synchronized (pendingActions)
        {
            this.pendingActions.add(new AutodiscAction<>(AutodiscActionType.UNREGISTER_INSTANCE, null));
        }
    }

    @Override
    public void unregisterTopicInfo(final AutoDiscTopicInfo autoDiscTopicInfo)
    {
        log.debug("Unregister AutoDiscTopicInfo, action added [{}]", autoDiscTopicInfo);

        synchronized (pendingActions)
        {
            this.pendingActions.add(new AutodiscAction<>(AutodiscActionType.UNREGISTER_TOPIC, autoDiscTopicInfo));
        }
    }

    @Override
    public void unregisterTopicSocketInfo(final AutoDiscTopicSocketInfo autoDiscTopicSocketInfo)
    {
        log.debug("Unregister AutoDiscTopicSocketInfo, action added [{}]", autoDiscTopicSocketInfo);

        synchronized (pendingActions)
        {
            this.pendingActions.add(new AutodiscAction<>(AutodiscActionType.UNREGISTER_TOPIC_SOCKET, autoDiscTopicSocketInfo));
        }
    }

    @Override
    public void subscribeToInstances(final IAutodiscInstanceListener listener)
    {
        log.debug("Subscribe to instances [{}], action added", listener);

        synchronized (pendingActions)
        {
            this.pendingActions.add(new AutodiscAction<>(AutodiscActionType.SUBSCRIBE_TO_INSTANCES, new AutodiscInstancesSubcribeActionContent(listener)));
        }
    }

    @Override
    public void unsubscribeFromInstances(final IAutodiscInstanceListener listener)
    {
        log.debug("Unsubscribe from instances, action added [{}]", listener);

        synchronized (pendingActions)
        {
            this.pendingActions.add(new AutodiscAction<>(AutodiscActionType.UNSUBSCRIBE_FROM_INSTANCES, new AutodiscInstancesSubcribeActionContent(listener)));
        }
    }

    @Override
    public void subscribeToTopic(final String topicName, final AutoDiscTransportType transportDirType, final IAutodiscTopicSubListener listener)
    {
        log.debug("Subscribe to topic [{}], action added [{}]", topicName, transportDirType);

        synchronized (pendingActions)
        {
            this.pendingActions.add(new AutodiscAction<>(AutodiscActionType.SUBSCRIBE_TO_TOPIC, new AutodiscTopicSubcribeActionContent(topicName, transportDirType, listener)));
        }
    }

    @Override
    public void unsubscribeFromTopic(final String topicName, final AutoDiscTransportType transportDirType, final IAutodiscTopicSubListener listener)
    {
        log.debug("Unsubscribe from topic [{}], action added [{}]", topicName, transportDirType);

        synchronized (pendingActions)
        {
            this.pendingActions.add(new AutodiscAction<>(AutodiscActionType.UNSUBSCRIBE_FROM_TOPIC, new AutodiscTopicSubcribeActionContent(topicName, transportDirType, listener)));
        }
    }

    @Override
    public void subscribeToPubTopicPattern(final String topicPattern, final IAutodiscPubTopicPatternListener listener)
    {
        log.debug("Add pattern listener for pattern [{}], action added", topicPattern);

        synchronized (pendingActions)
        {
            this.pendingActions.add(new AutodiscAction<>(AutodiscActionType.SUBSCRIBE_TO_PUB_PATTERN, new AutodiscSubscribeToPubPatternActionContent(topicPattern, listener)));
        }
    }

    @Override
    public void unsubscribeFromPubTopicPattern(final String topicPattern)
    {
        log.debug("Removing pattern listener for pattern [{}], action added", topicPattern);

        synchronized (pendingActions)
        {
            this.pendingActions.addLast(new AutodiscAction<>(AutodiscActionType.UNSUBSCRIBE_FROM_PUB_PATTERN, new AutodiscSubscribeToPubPatternActionContent(topicPattern, null)));
        }
    }

    @Override
    public void onNewTopicInfo(final AutoDiscTopicInfo info)
    {
        // If the info comes from a topic publisher or subscriber that is not end-point
        // we need to send all the information we have about the topic immediately to speed up the auto-discovery time
        switch (info.getTransportType())
        {
            case PUB_UNI:
                this.autodiscPub.republishAllInfoAboutTopic(info.getTopicName(), AutoDiscTransportType.SUB_UNI);
                break;
            case SUB_MUL:
                this.autodiscPub.republishAllInfoAboutTopic(info.getTopicName(), AutoDiscTransportType.PUB_MUL);
                break;
            case SUB_IPC:
                this.autodiscPub.republishAllInfoAboutTopic(info.getTopicName(), AutoDiscTransportType.PUB_IPC);
                break;
            default:
                break;
        }
    }

    @Override
    public void onNewInstanceInfo(final AutoDiscInstanceInfo info)
    {
        log.debug("On new instance info [{}]", info);

        // Reply it's own instance information every time a new instance appears to speed up auto-discovery time
        this.autodiscPub.republishInstanceInfo();
    }

    @Override
    public int action()
    {
        // Apply pending user actions
        int actionsApplied = this.applyNextPendingAction();

        // Pub next topic advert
        actionsApplied += this.autodiscPub.sendNextTopicAdverts();

        // Receive next topic advert
        actionsApplied += this.autodiscSub.pollNextMessage();

        // Check next timeout
        actionsApplied += this.autodiscSub.checkNextTimeout();

        // Return the number of actions taken
        return actionsApplied;
    }

    @Override
    public void cleanUp()
    {
        log.info("Cleaning up after being stopped");

        // Close publisher and subscriber
        this.autodiscPub.close();
        this.autodiscSub.close();

        // Clean maps
        this.pendingActions.clear();
    }

    /**
     * Apply all the pending actions in the queue
     *
     * @return the number of actions that have been performed
     */
    private int applyNextPendingAction()
    {
        // Get the first action to apply
        AutodiscAction action;
        synchronized (pendingActions)
        {
            if (this.pendingActions.isEmpty())
            {
                return 0;
            }

            action = this.pendingActions.removeFirst();
        }

        log.debug("Applying pending action {}", action);

        this.applyAction(action);
        return 1;
    }

    /**
     * Apply the given action
     *
     * @param action the action to apply
     */
    private void applyAction(final AutodiscAction action)
    {
        switch (action.getActionType())
        {
            case REGISTER_INSTANCE:
                this.autodiscPub.registerInstance((AutoDiscInstanceInfo)action.getContent());
                break;
            case UNREGISTER_INSTANCE:
                this.autodiscPub.unRegisterInstance();
                break;
            case REGISTER_TOPIC:
                final AutoDiscTopicInfo registerTopicInfo = (AutoDiscTopicInfo)action.getContent();
                this.autodiscPub.registerTopic(registerTopicInfo);
                break;
            case UNREGISTER_TOPIC:
                final AutoDiscTopicInfo unregisterTopicInfo = (AutoDiscTopicInfo)action.getContent();
                this.autodiscPub.unregisterTopic(unregisterTopicInfo);
                break;
            case REGISTER_TOPIC_SOCKET:
                final AutoDiscTopicSocketInfo registerTopicSocketInfo = (AutoDiscTopicSocketInfo) action.getContent();
                this.autodiscPub.registerTopicSocket(registerTopicSocketInfo);
                break;
            case UNREGISTER_TOPIC_SOCKET:
                final AutoDiscTopicSocketInfo unregisterTopicSocketInfo = (AutoDiscTopicSocketInfo) action.getContent();
                this.autodiscPub.unregisterTopicSocket(unregisterTopicSocketInfo);
                break;
            case SUBSCRIBE_TO_INSTANCES:
                final AutodiscInstancesSubcribeActionContent subscribeInstanceContent = (AutodiscInstancesSubcribeActionContent)action.getContent();
                this.autodiscSub.subscribeToInstances(subscribeInstanceContent.getListener());
                break;
            case UNSUBSCRIBE_FROM_INSTANCES:
                final AutodiscInstancesSubcribeActionContent unsubscribeInstanceContent = (AutodiscInstancesSubcribeActionContent)action.getContent();
                this.autodiscSub.unsubscribeFromInstances(unsubscribeInstanceContent.getListener());
                break;
            case SUBSCRIBE_TO_TOPIC:
                final AutodiscTopicSubcribeActionContent subscribeContent = (AutodiscTopicSubcribeActionContent)action.getContent();
                this.autodiscSub.subscribeToTopic(subscribeContent.getTopicName(), subscribeContent.getTransportType(), subscribeContent.getListener());
                break;
            case UNSUBSCRIBE_FROM_TOPIC:
                final AutodiscTopicSubcribeActionContent unsubscribeContent = (AutodiscTopicSubcribeActionContent)action.getContent();
                this.autodiscSub.unsubscribeFromTopic(unsubscribeContent.getTopicName(), unsubscribeContent.getTransportType(), unsubscribeContent.getListener());
                break;
            case SUBSCRIBE_TO_PUB_PATTERN:
                final AutodiscSubscribeToPubPatternActionContent subscribePatternContent = (AutodiscSubscribeToPubPatternActionContent)action.getContent();
                this.autodiscSub.subscribeToPubPattern(subscribePatternContent.getPattern(), subscribePatternContent.getPatternListener());
                break;
            case UNSUBSCRIBE_FROM_PUB_PATTERN:
                final AutodiscSubscribeToPubPatternActionContent unsubscribePatternContent = (AutodiscSubscribeToPubPatternActionContent)action.getContent();
                this.autodiscSub.unsubscribeFromPubPattern(unsubscribePatternContent.getPattern());
                break;
            default:
                break;
        }
    }
}
