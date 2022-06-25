package com.bbva.kyof.vega.autodiscovery.subscriber;

import com.bbva.kyof.vega.autodiscovery.advert.ActiveAdvertsQueue;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscDaemonClientInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscDaemonServerInfo;
import com.bbva.kyof.vega.autodiscovery.publisher.IPublicationsManager;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import com.bbva.kyof.vega.util.net.InetUtil;
import io.aeron.Aeron;
import io.aeron.Subscription;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
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
public class AutodiscUnicastReceiver extends AbstractAutodiscReceiver implements Closeable
{
    /**
     * PublicationsManager that manages all the publications to the
     * unicast daemon servers.
     */
    private final IPublicationsManager publicationsManager;

    /** Store the information of this unicast daemon client. The information contains the socket it uses to get messages from the Daemon. <p>
     * Since the socket is created int he subscriber and the publisher needs to send this information to the daemon we store it here when the actual
     * socket is created with the socket information .*/
    @Getter private AutoDiscDaemonClientInfo daemonClientInfo;

    /** Store the queue with all the active instance information adverts */
    private final ActiveAdvertsQueue<AutoDiscDaemonServerInfo> autoDiscDaemonServerInfoActiveAdvertsQueue;

    /**
     * Create a new autodiscovery unicast subscriber handler
     *
     * @param instanceId unique id of the library instance the subscriber belongs to
     * @param aeron the Aeron instance
     * @param config the configuration of auto-discovery
     * @param autodiscoverySubListener the listener that will receive events of new created or timed out adverts
     * @param pPublicationsManager PublicationsManager instance
     */
    public AutodiscUnicastReceiver(final UUID instanceId,
                                   final Aeron aeron,
                                   final AutoDiscoveryConfig config,
                                   final IAutodiscGlobalEventListener autodiscoverySubListener,
                                   final IPublicationsManager pPublicationsManager)
    {
        super(instanceId, aeron, config, autodiscoverySubListener);
        this.publicationsManager = pPublicationsManager;

        // When the Daemon Server which the client is subscribed gets down, the adverts in the other clients
        // will not been received.
        // If the timeout for all the adverts (instance, topic & topicSocket adverts) is the same that
        // the timeout for the daemon server info advert, all the topics (in the other clients)
        // can timeout at the same time the client discover that the daemon server fails (because all the
        // adverts that this client sent to the other clients could not arrive).
        // But if the client detects that the daemon server is down before the topics become timeout, the client
        // can change the daemon server and publicate their topics before them timeout in the other clients
        // Timeout for Daemon Server Info is the middle of the topics timeot
        long timeountForDaemonServerInfo = config.getTimeout() / 2;

        // Create the queues with the timeout of the configuration
        this.autoDiscDaemonServerInfoActiveAdvertsQueue = new ActiveAdvertsQueue<>(timeountForDaemonServerInfo);
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
        this.daemonClientInfo = new AutoDiscDaemonClientInfo(instanceId, InetUtil.convertIpAddressToInt(ipAddress), portNumber, streamId, config.getHostname());

        log.info("Creating auto-discovery unicast receiver aeron subscription on channel {} and stream id {}", channel, streamId);

        // Create the Aeron Subscription
        return aeron.addSubscription(channel, streamId);
    }

    @Override
    protected boolean processAutoDiscDaemonServerInfoMsg(final AutoDiscDaemonServerInfo autoDiscDaemonServerInfo)
    {
        // Add or update, if false is an update and there is nothing else to do
        if (!autoDiscDaemonServerInfoActiveAdvertsQueue.addOrUpdateAdvert(autoDiscDaemonServerInfo))
        {
            //The buffer was not consumed, so return false
            return false;
        }

        // Enable the publication of this disabled unicast server
        publicationsManager.enablePublication(autoDiscDaemonServerInfo);

        //Check if the message is from a restarted daemon to make cleaning
        publicationsManager.checkOldDaemonServerInfo();

        //The buffer was consumed (saved into autoDiscDaemonServerInfoActiveAdvertsQueue ), so return true
        return true;
    }

    @Override
    protected int checkAutoDiscDaemonServerInfoTimeouts()
    {
        final AutoDiscDaemonServerInfo autoDiscDaemonServerInfo = this.autoDiscDaemonServerInfoActiveAdvertsQueue.returnNextTimedOutElement();
        if (autoDiscDaemonServerInfo != null)
        {
            // Disable the timeout publication
            publicationsManager.disablePublication(autoDiscDaemonServerInfo);
            return 1;
        }
        return 0;
    }

    @Override
    public void close()
    {
        log.info("Closing auto discovery unicast receiver manager");
        // Clear internal queues
        this.autoDiscDaemonServerInfoActiveAdvertsQueue.clear();

        super.close();
    }
}
