package com.bbva.kyof.vega.autodiscovery.publisher;

import lombok.Getter;

/**
 * Represents a piece of auto-discovery information that has been stored in order to be periodically sent in the form of
 * an auto-discovery advert.
 *
 * The registered info is generic and valid for any stored information. It contains the helper methods required to know
 * when the refresh interval has been reached and the registered information should be sent.
 */
class RegisteredInfo<T>
{
    /** Data that represent the registered information */
    @Getter private final T info;

    /** Sending interval expressed in milliseconds */
    private final long sendIntervalMillis;

    /** Time when next sending is expected, expressed in milliseconds */
    private long nextExpectedSend;

    /**
     * Create a new registered information
     *
     * @param info the information object to registerTopicInfo
     * @param sendIntervalMillis the send interval in milliseconds
     */
    RegisteredInfo(final T info, final long sendIntervalMillis)
    {
        this.nextExpectedSend = System.currentTimeMillis() + sendIntervalMillis;
        this.sendIntervalMillis = sendIntervalMillis;
        this.info = info;
    }

    /**
     * Returns true if the registered information should be sent. This means the send interval has been reached.
     *
     * If it has been reached, it will update the next expected send time to be used in the next call.
     *
     * @param now the current time in milliseconds
     * @return true if info should be sent
     */
    boolean checkIfshouldSendAndResetIfRequired(final long now)
    {
        if (now >= this.nextExpectedSend)
        {
            this.nextExpectedSend = now + this.sendIntervalMillis;
            return true;
        }

        return false;
    }

    /**
     * Return the registered information if the send interval has been reached, null in other case.
     *
     * If send interval has been reached, it will update the next expected send time to be used in the next call.
     *
     * @param now the current time in milliseconds
     * @return the registered information if it should be sent, null in other case
     */
    T getIfShouldSendAndResetIfRequired(final long now)
    {
        if (this.checkIfshouldSendAndResetIfRequired(now))
        {
            return this.info;
        }

        return null;
    }

    /**
     * Reset the time for the next expected send to be now + sendInterval.
     *
     * @param now the current time in milliseconds
     */
    void resetNextExpectedSent(final long now)
    {
        this.nextExpectedSend = now + this.sendIntervalMillis;
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

        RegisteredInfo<?> that = (RegisteredInfo<?>) target;

        if (this.info == null)
        {
            return that.info == null;
        }
        else
        {
            return this.info.equals(that.info);
        }
    }

    @Override
    public int hashCode()
    {
        if (this.info == null)
        {
            return 0;
        }
        else
        {
            return this.info.hashCode();
        }
    }
}
