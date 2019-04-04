package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscDaemonClientInfo;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import io.aeron.Aeron;
import io.aeron.Publication;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of auto-discovery sender handler for unicast auto-discovery type
 */
@Slf4j
public class AutodiscUnicastSender extends AbstractAutodiscSender
{
    /** Client info min send interval */
    private static final long CLIENT_INFO_MIN_SEND_INTERVAL = 10;
    /** Client info max send interval */
    private static final long CLIENT_INFO_MAX_SEND_INTERVAL = 500;
    /** Client info send interval increment factor */
    private static final int CLIENT_INFO_SEND_INC_FACTOR = 2;

    /**
     * Information of the daemon client represented by the auto-discovery instance of the library and
     * that has to be periodically sent with the reception socket info of the client.
     * The daemon will use this information to send the adverts to the client.
     */
    private volatile VariableSendRegisteredInfo<AutoDiscDaemonClientInfo> registeredDaemonClientInfo = null;

    /**
     * Creates a new auto-discovery unicast sender
     * @param aeron the aeron instance
     * @param config the configuration for autodiscovery
     * @param daemonClientInfo the information of the daemon client with the reception socket information
     */
    public AutodiscUnicastSender(final Aeron aeron, final AutoDiscoveryConfig config, final AutoDiscDaemonClientInfo daemonClientInfo)
    {
        super(aeron, config);
        this.registeredDaemonClientInfo = new VariableSendRegisteredInfo<>(daemonClientInfo, CLIENT_INFO_MIN_SEND_INTERVAL, CLIENT_INFO_MAX_SEND_INTERVAL, CLIENT_INFO_SEND_INC_FACTOR);
    }

    @Override
    public Publication createPublication(final Aeron aeron, final AutoDiscoveryConfig config)
    {
        // Create the aeron channel
        final String channel = AeronChannelHelper.createUnicastChannelString(config.getResolverDaemonAddress(),
                config.getResolverDaemonPort(),
                config.getSubnetAddress());

        log.debug("Creating uniscast publication with channel [{}] and stream [{}]", channel, config.getDefaultStreamId());

        // Create the publication
        return aeron.addPublication(channel, config.getDefaultStreamId());
    }

    @Override
    public int sendNextTopicAdverts()
    {
        // Get the daemon client info it if should be sent
        final AutoDiscDaemonClientInfo daemonClientInfo = this.registeredDaemonClientInfo.getIfShouldSendAndResetIfRequired(System.currentTimeMillis());

        // Send the daemon client and hte rest of topic adverts
        return this.sendMessageIfNotNull(MsgType.AUTO_DISC_DAEMON_CLIENT_INFO, daemonClientInfo) + super.sendNextTopicAdverts();
    }
}
