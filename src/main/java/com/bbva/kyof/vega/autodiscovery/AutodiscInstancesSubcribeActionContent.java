package com.bbva.kyof.vega.autodiscovery;

import com.bbva.kyof.vega.autodiscovery.subscriber.IAutodiscInstanceListener;
import lombok.Getter;
import lombok.ToString;

/**
 * Contents of a subscribeToInstances / unsubscribeFromInstances action
 */
@ToString
final class AutodiscInstancesSubcribeActionContent
{
    /** Listener for events on this subscription */
    @Getter private final IAutodiscInstanceListener listener;

    /**
     * Create a new subscription action content
     * @param listener listener for events related to the subscription
     */
    AutodiscInstancesSubcribeActionContent(final IAutodiscInstanceListener listener)
    {

        this.listener = listener;
    }
}
