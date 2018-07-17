package com.bbva.kyof.vega.msg;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents the information of a sent request. <p>
 *
 * Is a good practice to close requests when no more responses are expected instead of waiting for the time out.
 *
 * It is possible to reset the time out of a sent request. This is specially useful when there are multiple responses. <p>
 *
 * This class is thread safe!
 */
@Slf4j
public class SentRequest implements ISentRequest
{
    /** Return the result of the request sent */
    @Getter @Setter private PublishResult sentResult;

    /** Unique identifier of the request */
    @Getter private final UUID requestId;

    /** Request expiration time */
    private final AtomicLong expirationTime;

    /** Listener for responses on this request */
    private final IResponseListener responseListener;

    /** Number of received responses for this request */
    private final AtomicInteger numResponses = new AtomicInteger();

    /** The topicName the request belongs to */
    @Getter private final String topicName;

    /** True if the request has been closed */
    private boolean closed = false;

    /** Lock for class syncrhonization */
    private final Object lock = new Object();

    /**
     * Constructor of the sent request information
     *
     * @param topicName the topicName the request belong to
     * @param timeout timeout for the request expiration
     * @param responseListener listener for responses
     * @param rndGenerator random number generator that will be used to create the unique ID of the request
     */
    public SentRequest(
            final String topicName,
            final long timeout,
            final IResponseListener responseListener,
            final Random rndGenerator)
    {
        this.topicName = topicName;
        this.requestId = new UUID(rndGenerator.nextLong(), rndGenerator.nextLong());
        this.responseListener = responseListener;
        this.expirationTime = new AtomicLong(System.currentTimeMillis() + timeout);
    }

    @Override
    public boolean hasExpired()
    {
        final long expirationTimeValue = this.expirationTime.get();
        return System.currentTimeMillis() >= expirationTimeValue;
    }

    @Override
    public void resetExpiration(final long newTimeout)
    {
        this.expirationTime.set(System.currentTimeMillis() + newTimeout);
    }

    @Override
    public void closeRequest()
    {
        synchronized (this.lock)
        {
            this.closed = true;
        }
    }

    @Override
    public boolean isClosed()
    {
        synchronized (this.lock)
        {
            return this.closed;
        }
    }

    @Override
    public int getNumberOfResponses()
    {
        return this.numResponses.get();
    }

    /**
     * Process a received response and notify the response listener if the request has not been closed yet
     *
     * @param response the received response
     */
    public void onResponseReceived(final RcvResponse response)
    {
        synchronized (this.lock)
        {
            // If already closed ignore the response
            if (this.closed)
            {
                log.info("Response received on an already closed or expired request. Request ID [{}], Responder AppId [{}]", this.requestId, response.getInstanceId());
                return;
            }

            // Increment the number of received responses
            this.numResponses.getAndIncrement();

            // If not look for a response listener and sendMsg the response
            if (this.responseListener != null)
            {
                try
                {
                    this.responseListener.onResponseReceived(this, response);
                }
                catch (final RuntimeException e)
                {
                    log.error("Uncaught exception processing received response for request ID " + this.requestId, e);
                }
            }
        }
    }

    /**
     * Called when the request times out, a timeout request should be closed if not already closed and the timeout listener notified if exists.
     */
    public void onRequestTimeout()
    {
        synchronized (this.lock)
        {
            // If already closed just return
            if (this.closed)
            {
                return;
            }

            // Set the request as closed
            this.closed = true;

            // If there is a listener notify
            if (this.responseListener != null)
            {
                try
                {
                    this.responseListener.onRequestTimeout(this);
                }
                catch (final RuntimeException e)
                {
                    log.error("Uncaught exception processing request timeout for request id " + this.requestId, e);
                }
            }
        }
    }
}
