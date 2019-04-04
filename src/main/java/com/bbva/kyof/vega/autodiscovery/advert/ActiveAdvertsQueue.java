package com.bbva.kyof.vega.autodiscovery.advert;

import com.bbva.kyof.vega.autodiscovery.model.IAutoDiscInfo;
import com.bbva.kyof.vega.util.collection.HashMapStack;

import java.util.UUID;
import java.util.function.Consumer;

/**
 * This class represents a queue of active adverts information that is going to be periodically checked for time outs.<p>
 *
 * The queue stores the information on a HashMapStack by stored information unique id. This allows quick access
 * for information removal and existence check and at the same time by using a HashMapStack it keeps the order of
 * the added elements. <p>
 *
 * The order of insertion is performed to ensure the next element that may time out is the last one added. Every
 * time an element times out, is removed and added at the end. Since the timeout interval is shared by all elements
 * this mechanism ensures that only checking the next element is enough to know if the next element has timed out. <p>
 *
 * This class is not thread safe!
 *
 * @param <T> The internal type of the objects in the queue
 */
public class ActiveAdvertsQueue<T extends IAutoDiscInfo>
{
    /** HashMapStack with all the active adverts stored by the advert content unique id */
    private final HashMapStack<UUID, ActiveAdvert<T>> activeAdvertsByAutodiscInfoId = new HashMapStack<>();

    /** Timeout value for the created adverts */
    private final long advertTimeout;

    /**
     * Create a new adverts queue in which all the elements will have the given timeout period
     * @param advertTimeout the timeout for the created adverts
     */
    public ActiveAdvertsQueue(final long advertTimeout)
    {
        this.advertTimeout = advertTimeout;
    }

    /**
     * Add a new advert for the given advert info or update received time if it already exists <p>
     *
     * The existence of the advert is check against the unique id of the auto-discovery information.
     *
     * @param advertInfo the auto-discovery advert information
     *
     * @return true if the advert is a new advert and has been added and false if it has been updated
     */
    public boolean addOrUpdateAdvert(final T advertInfo)
    {
        // Look for an existing advert with the same advert info id and remove it
        final ActiveAdvert<T> existingAdvert = this.activeAdvertsByAutodiscInfoId.remove(advertInfo.getUniqueId());

        // If is a repeated element, update and add again. It is added again to force it to be the newest element and
        // therefore the last to check for timeouts since it has been just updated
        if (existingAdvert == null)
        {
            // Is a new element, create a new Active Advert and add it
            final ActiveAdvert<T> newAdvert = new ActiveAdvert<>(advertInfo, this.advertTimeout);
            this.activeAdvertsByAutodiscInfoId.put(advertInfo.getUniqueId(), newAdvert);

            return true;
        }
        else
        {
            // Update received time
            existingAdvert.updateLastUpdateReceived();

            // Add again to the map
            this.activeAdvertsByAutodiscInfoId.put(advertInfo.getUniqueId(), existingAdvert);

            return false;
        }
    }

    /**
     * Check the next element in the internal stack of active adverts for a timeout.
     *
     * If time out, it will remove the element and return it
     *
     * @return timed out element, null if there is no time out
     */
    public T returnNextTimedOutElement()
    {
        // Get the eldest key and value on the map
        final UUID eldestKey = this.activeAdvertsByAutodiscInfoId.getEldestKey();
        final ActiveAdvert<T> eldestValue = this.activeAdvertsByAutodiscInfoId.getEldestValue();

        if (eldestKey == null || eldestValue == null)
        {
            return null;
        }

        // If oldest element has timed out remove and notify and return, if not do nothing
        if (eldestValue.hasTimedOut())
        {
            this.activeAdvertsByAutodiscInfoId.remove(eldestKey);
            return eldestValue.getAutoDiscInfo();
        }

        return null;
    }

    /**
     * Run the given consumer for all the elements in the queue
     *
     * @param consumer consumer to run for each element
     */
    public void runForEachElement(final Consumer<T> consumer)
    {
        this.activeAdvertsByAutodiscInfoId.consumeAllValues(advert -> consumer.accept(advert.getAutoDiscInfo()));
    }

    /**
     * Clear all internal contents
     */
    public void clear()
    {
        this.activeAdvertsByAutodiscInfoId.clear();
    }
}
