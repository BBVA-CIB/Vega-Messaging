package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import io.aeron.Aeron;
import io.aeron.Publication;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of auto-discovery publisher handler for multicast auto-discovery type
 */
@Slf4j
public class AutodiscMcastSender extends AbstractAutodiscSender
{
    /**
     * Constructor to create a new auto-discovery multicast publisher
     *
     * @param aeron the Aeron instance object
     * @param config the autodiscovery configuration
     */
    public AutodiscMcastSender(final Aeron aeron, final AutoDiscoveryConfig config)
    {
        super(aeron, config);
    }

    @Override
    public Publication createPublication(final Aeron aeron, final AutoDiscoveryConfig config)
    {
        // Create the aeron channel for multicast using the configuration
        final String channel = AeronChannelHelper.createMulticastChannelString(config.getMulticastAddress(),
                config.getMulticastPort(),
                config.getSubnetAddress());

        log.debug("Creating multicast publication with channel [{}] and stream [{}]", channel, config.getDefaultStreamId());

        // Create the publication
        return aeron.addPublication(channel, config.getDefaultStreamId());
    }
}
