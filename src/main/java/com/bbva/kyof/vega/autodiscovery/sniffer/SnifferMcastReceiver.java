package com.bbva.kyof.vega.autodiscovery.sniffer;

import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import io.aeron.Aeron;
import io.aeron.Subscription;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;


/**
 * Sniffer implementation for multicast
 * <p>
 * The class is not thread-safe!
 */
@Slf4j
public class SnifferMcastReceiver extends AbstractSnifferReceiver
{
    /**
     * Create a new autodiscovery subscriber handler
     *
     * @param instanceId      unique id of the library instance the subscriber belongs to
     * @param aeron           the Aeron instance
     * @param parameters      the configuration of sniffer
     * @param snifferListener the listener that will receive events of new created or timed out adverts
     */
    public SnifferMcastReceiver(final UUID instanceId,
                                final Aeron aeron,
                                final SnifferParameters parameters,
                                final ISnifferListener snifferListener)
    {
        super(instanceId, aeron, parameters, snifferListener);
    }

    @Override
    public Subscription createSubscription(final UUID instanceId, final Aeron aeron, final SnifferParameters parameters, final ISnifferListener snifferListener)
    {
        // Create the aeron channel
        final String channel = AeronChannelHelper.createMulticastChannelString(parameters.getIpAddress(), parameters.getPort(), parameters.getSubnetAddress());

        log.info("SNIFFER: Creating sniffer multicast receiver aeron subscription on channel {} and stream id {}", channel, SnifferParameters.DEFAULT_STREAM_ID);

        // Create the aeron subscription
        return aeron.addSubscription(channel, SnifferParameters.DEFAULT_STREAM_ID);
    }
}
