package com.bbva.kyof.vega.autodiscovery.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscDaemonClientInfo;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import com.bbva.kyof.vega.util.net.InetUtil;
import io.aeron.Aeron;
import io.aeron.Subscription;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;


/**
 * Auto-Discovery implementation for unicast.<p>
 *
 * The Aeron Subscription channel is created using the StreamID and Port Range provided in the configuration.
 * A hash of the instance id is performed and used to calculate the stream and the port of the range to use. <p>
 *
 * The class is not thread-safe!
 */
@Slf4j
public class AutodiscUnicastReceiver extends AbstractAutodiscReceiver
{
    /** Store the information of this unicast daemon client. The information contains the socket it uses to get messages from the Daemon. <p>
     * Since the socket is created int he subscriber and the publisher needs to send this information to the daemon we store it here when the actual
     * socket is created with the socket information .*/
    @Getter private AutoDiscDaemonClientInfo daemonClientInfo;

    /**
     * Create a new autodiscovery unicast subscriber handler
     *
     * @param instanceId unique id of the library instance the subscriber belongs to
     * @param aeron the Aeron instance
     * @param config the configuration of auto-discovery
     * @param autodiscoverySubListener the listener that will receive events of new created or timed out adverts
     */
    public AutodiscUnicastReceiver(final UUID instanceId,
                                   final Aeron aeron,
                                   final AutoDiscoveryConfig config,
                                   final IAutodiscGlobalEventListener autodiscoverySubListener)
    {
        super(instanceId, aeron, config, autodiscoverySubListener);
    }

    @Override
    public Subscription createSubscription(final UUID instanceId, final Aeron aeron, final AutoDiscoveryConfig config)
    {
        // Create a hash for the instance id. We will use it to select a random port and stream
        final int instanceIdHash = instanceId.hashCode();

        // Select the Stream ID from the range
        final int streamId = AeronChannelHelper.selectStreamFromRange(instanceIdHash, config.getUnicastResolverRcvNumStreams());

        // Select the ip address using the subnet address, since we are using 32 bit mask subnets we can use that address directly
        final String ipAddress = config.getSubnetAddress().getIpAddres().getHostAddress();

        // Select the port from the range
        final int portNumber = AeronChannelHelper.selectPortFromRange(instanceIdHash, config.getUnicastResolverRcvPortMin(), config.getUnicastResolverRcvPortMax());

        // Create the channel
        final String channel = AeronChannelHelper.createUnicastChannelString(ipAddress, portNumber, config.getSubnetAddress());

        // Store the daemon client information with the subscription information that is about to be created
        this.daemonClientInfo = new AutoDiscDaemonClientInfo(instanceId, InetUtil.convertIpAddressToInt(ipAddress), portNumber, streamId);

        log.info("Creating auto-discovery unicast receiver aeron subscription on channel {} and stream id {}", channel, streamId);

        // Create the Aeron Subscription
        return aeron.addSubscription(channel, streamId);
    }
}
