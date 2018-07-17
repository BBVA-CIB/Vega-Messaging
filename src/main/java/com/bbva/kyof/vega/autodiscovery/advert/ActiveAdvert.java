package com.bbva.kyof.vega.autodiscovery.advert;

import lombok.Getter;

/**
 * This class represents a received advert with auto-discovery information.
 *
 * It will store the timeout and the last time an update is received and help calculate when the advert times out.
 */
public class ActiveAdvert<T>
{
    /** The timeout fot he topic advert. Max times without an update before considering it expired */
    private final long timeout;
    /** Auto-discovery information stored */
    @Getter private final T autoDiscInfo;
    /** Store the time in milliseconds of the last update received */
    private long lastUpdateReceived = 0;

    /**
     * Creates a new topic advert
     *
     * @param autoDiscInfo information of the auto-discovery info that generated the advert
     * @param timeout timeout of the topic advert
     */
    ActiveAdvert(final T autoDiscInfo, final long timeout)
    {
        this.autoDiscInfo = autoDiscInfo;
        this.timeout = timeout;

        this.lastUpdateReceived = System.currentTimeMillis();
    }

    /**
     * Update the last received time using the current time
     */
    void updateLastUpdateReceived()
    {
        this.lastUpdateReceived = System.currentTimeMillis();
    }

    /**
     * Check if the advert has timed out
     *
     * @return true if it has timed out
     */
    boolean hasTimedOut()
    {
       return this.lastUpdateReceived + this.timeout < System.currentTimeMillis();
    }

    @Override
    public boolean equals(final Object target)
    {
        if (this == target)
        {
            return true;
        }
        if (target == null || getClass() != target.getClass())
        {
            return false;
        }

        final ActiveAdvert<?> that = (ActiveAdvert<?>) target;

        if (autoDiscInfo == null)
        {
            return that.autoDiscInfo == null;
        }
        else
        {
            return autoDiscInfo.equals(that.autoDiscInfo);
        }
    }

    @Override
    public int hashCode()
    {
        if (autoDiscInfo == null)
        {
            return 0;
        }
        else
        {
            return autoDiscInfo.hashCode();
        }
    }
}
