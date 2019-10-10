package com.bbva.kyof.vega.autodiscovery.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscDaemonServerInfo;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import io.aeron.Aeron;
import io.aeron.Subscription;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;


/**
 * Auto-Discovery implementation for multicast
 *
 * The class is not thread-safe!
 */
@Slf4j
public class AutodiscMcastReceiver extends AbstractAutodiscReceiver
{
    /**
     * Create a new autodiscovery subscriber handler
     *
     * @param instanceId unique id of the library instance the subscriber belongs to
     * @param aeron the Aeron instance
     * @param config the configuration of auto-discovery
     * @param autodiscoverySubListener the listener that will receive events of new created or timed out adverts
     */
    public AutodiscMcastReceiver(final UUID instanceId,
                                 final Aeron aeron,
                                 final AutoDiscoveryConfig config,
                                 final IAutodiscGlobalEventListener autodiscoverySubListener)
    {
        super(instanceId, aeron, config, autodiscoverySubListener);
    }

    @Override
    public Subscription createSubscription(final UUID instanceId, final Aeron aeron, final AutoDiscoveryConfig config)
    {
        // Create the aeron channel
        final String channel = AeronChannelHelper.createMulticastChannelString(config.getMulticastAddress(), config.getMulticastPort(), config.getSubnetAddress());

        log.info("Creating auto-discovery multicast receiver aeron subscription on channel {} and stream id {}", channel, config.getDefaultStreamId());

        // Create the aeron subscription
        return aeron.addSubscription(channel, config.getDefaultStreamId());
    }

    @Override
    protected boolean processAutoDiscDaemonServerInfoMsg(final AutoDiscDaemonServerInfo autoDiscDaemonServerInfo)
    {
        // Do nothing for multicast
        // Return false because the buffer is not consumed
        return false;
    }

    @Override
    protected int checkAutoDiscDaemonServerInfoTimeouts()
    {
        // Do nothing for multicast
        return 0;
    }
}
