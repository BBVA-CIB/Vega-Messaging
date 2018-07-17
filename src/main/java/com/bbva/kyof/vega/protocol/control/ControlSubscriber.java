package com.bbva.kyof.vega.protocol.control;


import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;

/**
 * Wrapper class to encapsulate an Aeron Subscriber for control messages.
 *
 * This class is thread safe!!
 */
@Slf4j
class ControlSubscriber implements Closeable
{
    /** Parameters of the aeron subscriber */
    @Getter private final ControlSubscriberParams params;

    /** Aeron subscription wrapped by this subscriber */
    private final Subscription subscription;

    /** Lock of the class for synchronization */
    private final Object lock = new Object();

    /**
     * Create a new Aeron Subscriber
     *
     * @param vegaContext library instance context
     * @param params parameters for the susbcriber
     */
    ControlSubscriber(final VegaContext vegaContext, final ControlSubscriberParams params)
    {
        this.params = params;

        // Create the Aeron subscriber channel
        final String publicationChannel = AeronChannelHelper.createUnicastChannelString(params.getIpAddress(), params.getPort(), params.getSubnetAddress());

        // Create the Aeron subscriber
        this.subscription = vegaContext.getAeron().addSubscription(publicationChannel, params.getStreamId());

        log.info("Created control subscriber with params {}", params);
    }

    @Override
    public void close()
    {
        synchronized (this.lock)
        {
            if (this.subscription.isClosed())
            {
                return;
            }

            log.info("Closing control subscriber [{}]", this.params);

            this.subscription.close();
        }
    }

    /**
     * Perform a reception poll
     *
     * @param fragmentHandler the fragment handler that will process the message
     * @param maxFragments maximum number of fragments to get in the polling
     * @return the number of messages retrieved
     */
    public int poll(final FragmentHandler fragmentHandler, final int maxFragments)
    {
        synchronized (this.lock)
        {
            // Check if closed, it may happen that a poll is performed while it is being closed or after until removed from poller
            if (this.subscription.isClosed())
            {
                return 0;
            }

            return this.subscription.poll(fragmentHandler, maxFragments);
        }
    }
}