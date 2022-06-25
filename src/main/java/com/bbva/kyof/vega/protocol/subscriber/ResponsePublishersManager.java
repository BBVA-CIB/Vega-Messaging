package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.config.general.ResponsesConfig;
import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.publisher.AeronPublisher;
import com.bbva.kyof.vega.protocol.publisher.AeronPublisherParams;
import com.bbva.kyof.vega.util.collection.HashMapOfHashSet;
import com.bbva.kyof.vega.util.net.InetUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class handles the dynamically created response publishers for the library instances that joins the cluster
 *
 * Only the access to the response publishers by id is thread safe!
 */
@Slf4j
class ResponsePublishersManager implements Closeable
{
    /** Store all the aeron response publishers by the params used to create them */
    private final Map<AeronPublisherParams, AeronPublisher> responsePublishersByParams = new HashMap<>();

    /** Store all the response publishers by the instance id of the application they respond to */
    private final Map<UUID, AeronPublisher> responsePublishersByInstanceId = new ConcurrentHashMap<>();

    /** Store all the instance ids that are related to the same aeron publisher */
    private final HashMapOfHashSet<AeronPublisher, UUID> instanceIdsPerResponsePub = new HashMapOfHashSet<>();

    /** Vega instance context */
    private final VegaContext vegaContext;

    ResponsePublishersManager(final VegaContext vegaContext)
    {
        this.vegaContext = vegaContext;
    }

    @Override
    public void close()
    {
        // Destroy all response publishers
        this.responsePublishersByInstanceId.values().forEach(AeronPublisher::close);

        this.responsePublishersByParams.clear();
        this.responsePublishersByInstanceId.clear();
        this.instanceIdsPerResponsePub.clear();
    }

    /**
     * Return the response publisher associated to the given library instance id
     *
     * This call is thread-safe
     *
     * @param instanceId the library unique instance id
     * @return the associated publisher, null if none
     */
    AeronPublisher getResponsePublisherForInstance(final UUID instanceId)
    {
        return this.responsePublishersByInstanceId.get(instanceId);
    }

    /**
     * Called then there is a new autodiscovery instance info event.
     *
     * It will create a new response publisher for the new found instance or reuse an existing one
     *
     * @param info the information event
     */
    void onNewAutoDiscInstanceInfo(final AutoDiscInstanceInfo info)
    {
        // If there is already a response publisher for the instance, ignore, it may happen if there are duplicated events
        if (this.responsePublishersByInstanceId.containsKey(info.getUniqueId()))
        {
            return;
        }

        // Create the parameters for the response publisher
        final AeronPublisherParams params = createResponseAeronPublisherParams(info);

        // Check if there is already a response publisher with that parameters
        AeronPublisher responsePublisher = this.responsePublishersByParams.get(params);
        if (responsePublisher == null)
        {
            log.info("Creating new response publisher for new encountered instance {}", info);
            responsePublisher = new AeronPublisher(this.vegaContext, params);
            this.responsePublishersByParams.put(params, responsePublisher);
        }
        else
        {
            log.info("Reusing existing response publisher for new instance {}", info);
        }

        // Store the relation in both directions
        this.responsePublishersByInstanceId.put(info.getUniqueId(), responsePublisher);
        this.instanceIdsPerResponsePub.put(responsePublisher, info.getUniqueId());
    }

    /**
     * Called then there is a new timed out instance info event.
     *
     * It will detroy the associated response publisher if there are no more instances associated to it
     *
     * @param info the information event
     */
    void onTimedOutAutoDiscInstanceInfo(final AutoDiscInstanceInfo info)
    {
        // Find the response publisher for the instance
        final AeronPublisher responsePublisher = this.responsePublishersByInstanceId.remove(info.getUniqueId());

        // It may not be there if is a duplicated event or was called after closed
        if (responsePublisher == null)
        {
            return;
        }

        // Remove from the map
        this.instanceIdsPerResponsePub.remove(responsePublisher, info.getUniqueId());

        // If the reponse publisher has no more instances related we should close it
        if (this.instanceIdsPerResponsePub.containsKey(responsePublisher))
        {
            log.info("Response publisher still has related instances, won't be closed {}", info);
        }
        else
        {
            log.info("Closing response publisher for new timed out instance {}", info);

            responsePublisher.close();
            this.responsePublishersByParams.remove(responsePublisher.getParams());
        }
    }

    /**
     * Return the number of stored distinct instances info
     *
     * Added for testing purposes
     */
    long getNumRemoteInstancesInfo()
    {
        return this.responsePublishersByInstanceId.size();
    }

    /**
     * Return the number response publishers created
     *
     * Added for testing purposes
     */
    long getNumResponsePublishers()
    {
        return this.responsePublishersByInstanceId.values().stream().distinct().count();
    }

    /**
     * Create a AeronPublisherParams by AutoDiscInstanceInfo for Response publishers
     * The main reason of this method is to obtain the ResponsePublisher IP, resolved by hostname or get default ip sent by counterpart.
     *
     * @param info AutoDiscInstanceInfo of counterpart
     * @return a well-formed AeronPublisherParams for Response publishers
     */
    private AeronPublisherParams createResponseAeronPublisherParams(AutoDiscInstanceInfo info)
    {
        //check the ip to use, by default informed ip
        final ResponsesConfig myResponseConfig = vegaContext.getInstanceConfig().getResponsesConfig();

        int addressIp = info.getResponseTransportIp();
        if(myResponseConfig.getIsResolveHostname() && !myResponseConfig.getHostname().equals(info.getResponseTransportHostname()))
        {
            addressIp = InetUtil.getIpAddressAsIntByHostnameOrDefault(info.getResponseTransportHostname(), info.getResponseTransportIp());
            log.trace("ResponsePublisher address ip obtained by hostname: [{}] from [{}]", addressIp, info);
        }

        return new AeronPublisherParams(
                TransportMediaType.UNICAST,
                addressIp,
                info.getResponseTransportPort(),
                info.getResponseTransportStreamId(),
                vegaContext.getInstanceConfig().getResponsesConfig().getSubnetAddress());
    }
}
