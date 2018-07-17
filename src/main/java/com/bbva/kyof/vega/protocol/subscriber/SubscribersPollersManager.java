package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.config.general.RcvPollerConfig;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

/**
 * Manager that handles all the pollers that are active in the instance.
 *
 * This class is thread safe!
 */
@Slf4j
class SubscribersPollersManager implements Closeable
{
    /** Store all subscriber pollers by poller configuration name */
    private final Map<String, SubcribersPoller> subscriberPollersByName = new HashMap<>();
    /** Library instance context */
    private final VegaContext vegaContext;
    /** Listener for incoming messages from the pollers */
    private final ISubscribersPollerListener listener;
    /** Lock for instance synchronization */
    private final Object lock = new Object();

    /**
     * Create a new manager instance
     * @param vegaContext context of the lubrary instance
     * @param listener listneer for incoming messages from the pollers
     */
    SubscribersPollersManager(final VegaContext vegaContext, final ISubscribersPollerListener listener)
    {
        this.listener = listener;
        this.vegaContext = vegaContext;
    }

    /**
     * Return a poller given the poller name. It will create a new one if required.
     *
     * @param pollerName the name of the poller
     * @return the created poller or an existing one if it was already created
     */
    SubcribersPoller getPoller(final String pollerName)
    {
        synchronized (this.lock)
        {
            SubcribersPoller poller = this.subscriberPollersByName.get(pollerName);

            if (poller == null)
            {
                final RcvPollerConfig pollerConfig = this.vegaContext.getInstanceConfig().getPollerConfigForPollerName(pollerName);

                if (pollerConfig == null)
                {
                    log.error("Trying to create a poller with name [{}], but no configuration found that match that poller name");
                    throw new IllegalArgumentException("No configuration found for poller name " + pollerName);
                }

                poller = new SubcribersPoller(this.listener, pollerConfig);
                this.subscriberPollersByName.put(pollerName, poller);
                poller.start();
                return poller;
            }
            else
            {
                return poller;
            }
        }
    }

    @Override
    public void close()
    {
        log.info("Stopping SubscribersPollersManager for instance id [{}]...", this.vegaContext.getInstanceUniqueId());

        synchronized (this.lock)
        {
            // Stop all pollers
            this.subscriberPollersByName.values().forEach(SubcribersPoller::close);

            // Clear the map
            this.subscriberPollersByName.clear();
        }
    }
}
