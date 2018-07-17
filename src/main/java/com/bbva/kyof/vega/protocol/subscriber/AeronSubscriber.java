package com.bbva.kyof.vega.protocol.subscriber;


import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;

/**
 * Wrapper class to encapsulate an Aeron Subscriber.
 *
 * The objective of the class is to ensure thread safety, store all topics subscribers related to the aeron subscriber and perform the polling over
 * the aeron socket.
 *
 * It also handles the lifecycle of the subscriber.
 *
 * This class is thread safe!!
 */
@Slf4j
class AeronSubscriber implements Closeable
{
    /** Parameters of the aeron subscriber */
    @Getter private final AeronSubscriberParams params;

    /** Aeron subscription wrapped by this subscriber */
    private final Subscription subscription;

    /** Object for instance synchronization */
    private final Object lock = new Object();

    /**
     * Create a new Aeron Subscriber
     *
     * @param vegaContext library instance context
     * @param params parameters for the susbcriber
     */
    AeronSubscriber(final VegaContext vegaContext, final AeronSubscriberParams params)
    {
        this.params = params;

        // Create the Aeron subscriber channel
        final String publicationChannel = this.createSubscriptionChannel(params);

        // Create the Aeron subscriber
        this.subscription = vegaContext.getAeron().addSubscription(publicationChannel, params.getStreamId());

        log.info("Created aeron subscriber with params {}", params);
    }

    /**
     * Create the subscription channel given the parameters
     * @param params the parameters of hte usbscriber
     *
     * @return the created channel String representation
     */
    private String createSubscriptionChannel(final AeronSubscriberParams params)
    {
        // Create the publication channel string
        switch (params.getTransportType())
        {
            case UNICAST:
                return AeronChannelHelper.createUnicastChannelString(params.getIpAddress(), params.getPort(), params.getSubnetAddress());
            case MULTICAST:
                return AeronChannelHelper.createMulticastChannelString(params.getIpAddress(), params.getPort(), params.getSubnetAddress());
            case IPC:
                return AeronChannelHelper.createIpcChannelString();
            default:
                return null;
        }
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

            log.info("Closing aeron subscriber [{}]", this.params);

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