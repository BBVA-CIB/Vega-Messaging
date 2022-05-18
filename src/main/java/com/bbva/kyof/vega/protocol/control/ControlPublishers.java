package com.bbva.kyof.vega.protocol.control;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.util.collection.HashMapOfHashSet;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class handles the dynamically created control publishers for the library instances that joins the cluster.
 *
 * Each publisher can send control messages to a different vega instance. Some vega instances may share the same "socket"
 * in this case the same control publisher may be sending the message to both instances. The message will be discarded upon reception
 * by the instance that is not interested in it.
 *
 * Only the public access to the response publishers by id is thread safe in this class!
 */
@Slf4j
class ControlPublishers implements Closeable
{
    /** Store all the aeron control publishers by the params used to create them */
    private final Map<ControlPublisherParams, ControlPublisher> controlPublishersByParams = new HashMap<>();

    /** Store the control publishers for each instance id of the application they respond to */
    private final Map<UUID, ControlPublisher> controlPublisherByInstanceId = new ConcurrentHashMap<>();

    /** Store all the instance ids that are related to the same aeron publisher */
    private final HashMapOfHashSet<ControlPublisher, UUID> instanceIdsPerControlPub = new HashMapOfHashSet<>();

    /** Vega instance context */
    private final VegaContext vegaContext;

    /**
     * Create a new control publishers instance
     *
     * @param vegaContext the vega context of the instance
     */
    ControlPublishers(final VegaContext vegaContext)
    {
        this.vegaContext = vegaContext;
    }

    @Override
    public void close()
    {
        // Destroy all control publishers
        this.controlPublisherByInstanceId.values().forEach(ControlPublisher::close);

        // Clear the internal maps
        this.controlPublishersByParams.clear();
        this.controlPublisherByInstanceId.clear();
        this.instanceIdsPerControlPub.clear();
    }

    /**
     * Return the control publisher associated to the given library instance id
     *
     * This call is thread-safe
     *
     * @param instanceId the library unique instance id
     * @return the associated publisher, null if none
     */
    ControlPublisher getControlPublisherForInstance(final UUID instanceId)
    {
        return this.controlPublisherByInstanceId.get(instanceId);
    }

    /**
     * Called then there is a new autodiscovery instance info event.
     *
     * It will create a new control publisher for the new found instance or reuse an existing one
     *
     * @param info the information event
     */
    void onNewAutoDiscInstanceInfo(final AutoDiscInstanceInfo info)
    {
        // If there is already a control publisher for the instance, ignore, it may happen if there are duplicated events
        if (this.controlPublisherByInstanceId.containsKey(info.getUniqueId()))
        {
            return;
        }

        // Create the parameters for the response publisher
        final ControlPublisherParams params = new ControlPublisherParams(
                info.getControlRcvTransportIp(),
                info.getControlRcvTransportPort(),
                info.getControlRcvTransportStreamId(),
                vegaContext.getInstanceConfig().getControlRcvConfig().getSubnetAddress(),
                info.getControlRcvHostname());

        // Check if there is already a control publisher with that parameters
        ControlPublisher controlPublisher = this.controlPublishersByParams.get(params);
        if (controlPublisher == null)
        {
            log.info("Creating new control publisher for new encountered instance {}", info);
            controlPublisher = new ControlPublisher(this.vegaContext, params);
            this.controlPublishersByParams.put(params, controlPublisher);
        }
        else
        {
            log.info("Reusing existing control publisher for new instance {}", info);
        }

        // Store the relation in both directions
        this.controlPublisherByInstanceId.put(info.getUniqueId(), controlPublisher);
        this.instanceIdsPerControlPub.put(controlPublisher, info.getUniqueId());
    }

    /**
     * Called then there is a new timed out instance info event.
     *
     * It will destroy the associated control publisher if there are no more instances associated with it
     *
     * @param info the information event
     */
    void onTimedOutAutoDiscInstanceInfo(final AutoDiscInstanceInfo info)
    {
        // Find the control publisher for the instance
        final ControlPublisher controlPublisher = this.controlPublisherByInstanceId.remove(info.getUniqueId());

        // It may not be there if is a duplicated event or was called after closed
        if (controlPublisher == null)
        {
            return;
        }

        // Remove from the map the instance id for that control publisher
        this.instanceIdsPerControlPub.remove(controlPublisher, info.getUniqueId());

        // If the response publisher has no more instances related we should close it
        if (this.instanceIdsPerControlPub.containsKey(controlPublisher))
        {
            log.info("Control publisher still has related instances, won't be closed {}", info);
        }
        else
        {
            log.info("Closing control publisher due to time out instance {}", info);

            controlPublisher.close();
            this.controlPublishersByParams.remove(controlPublisher.getParams());
        }
    }
}
