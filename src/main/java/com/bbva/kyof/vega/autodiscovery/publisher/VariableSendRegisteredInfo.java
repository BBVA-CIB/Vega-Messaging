package com.bbva.kyof.vega.autodiscovery.publisher;

import lombok.Getter;

/**
 * Represents a piece of auto-discovery information that has been stored in order to be periodically sent in the form of
 * an auto-discovery advert.
 *
 * The registered info is generic and valid for any stored information. It contains the helper methods required to know
 * when the refresh interval has been reached and the registered information should be sent.
 *
 * It supports variable send interval times, starting by minSendInterval and growing using the incremental factor until max send interval is reached
 */
class VariableSendRegisteredInfo<T>
{
    /** Data that represent the registered information */
    @Getter private final T info;

    /** Maximum send interval */
    private final long maxSendInterval;

    /** Send interval increment factor */
    private final int sendIncrementFactor;

    /** Time when next sending is expected, expressed in milliseconds */
    private long nextExpectedSend;

    /** Current value of send interval */
    private long currentSendInterval;

    /**
     * Create a new registered information
     *
     * @param info the information object to registerTopicInfo
     * @param minSendInterval min send interval in milliseconds
     * @param maxSendInterval max send interval in milliseconds
     * @param sendIncrementFactor send increment factor
     */
    VariableSendRegisteredInfo(final T info, final long minSendInterval, final long maxSendInterval, final int sendIncrementFactor)
    {
        this.maxSendInterval = maxSendInterval;
        this.sendIncrementFactor = sendIncrementFactor;
        this.currentSendInterval = minSendInterval;
        this.nextExpectedSend = System.currentTimeMillis() + minSendInterval;
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
    private boolean checkIfshouldSendAndResetIfRequired(final long now)
    {
        if (now >= this.nextExpectedSend)
        {
            this.calculateNextSend(now);
            return true;
        }

        return false;
    }

    /**
     * Calculate the next send and update the intervals information
     * @param now current time
     */
    private void calculateNextSend(final long now)
    {
        // Increment current send interval if max not reached
        if (this.currentSendInterval != this.maxSendInterval)
        {
            this.currentSendInterval = this.currentSendInterval * this.sendIncrementFactor;

            if (this.currentSendInterval > this.maxSendInterval)
            {
                this.currentSendInterval = this.maxSendInterval;
            }
        }

        // Calculate next expected send
        this.nextExpectedSend = now + this.currentSendInterval;
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

        VariableSendRegisteredInfo<?> that = (VariableSendRegisteredInfo<?>) target;

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
