package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import io.aeron.Aeron;
import io.aeron.Publication;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;

/**
 * Implementation of auto-discovery publisher handler for multicast auto-discovery type
 */
@Slf4j
public class AutodiscMcastSender extends AbstractAutodiscSender implements Closeable
{

    /** Publication aeron socket used to send the messages */
    @Getter private final Publication publication;

    /**
     * Constructor to create a new auto-discovery multicast publisher
     *
     * @param aeron the Aeron instance object
     * @param config the autodiscovery configuration
     */
    public AutodiscMcastSender(final Aeron aeron, final AutoDiscoveryConfig config)
    {
        super(aeron, config);

        // Create the multicast publication
        this.publication = this.createPublication(aeron, config);
    }

    /**
     * Creates the Aeron publication object to send auto-discovery messages.
     * @param aeron the Aeron instance
     * @param config the auto-discovery configuration
     *
     * @return the created Aeron publication
     */
    private Publication createPublication(final Aeron aeron, final AutoDiscoveryConfig config)
    {
        // Create the aeron channel for multicast using the configuration
        final String channel = AeronChannelHelper.createMulticastChannelString(config.getMulticastAddress(),
                config.getMulticastPort(),
                config.getSubnetAddress());

        log.debug("Creating multicast publication with channel [{}] and stream [{}]", channel, config.getDefaultStreamId());

        // Create the publication
        return aeron.addPublication(channel, config.getDefaultStreamId());
    }

    @Override
    public void close()
    {
        log.info("Closing auto discovery sender: publication");

        this.publication.close();

        super.close();
    }
}
