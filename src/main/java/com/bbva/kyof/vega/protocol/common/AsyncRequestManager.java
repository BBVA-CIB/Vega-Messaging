package com.bbva.kyof.vega.protocol.common;

import com.bbva.kyof.vega.msg.RcvResponse;
import com.bbva.kyof.vega.msg.SentRequest;
import com.bbva.kyof.vega.util.threads.RecurrentTask;
import lombok.extern.slf4j.Slf4j;
import org.agrona.concurrent.SleepingIdleStrategy;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class manages the asynchronous requests in the framework
 *
 * The purpose of this class is to control the active requests, timeouts, and memory usage.
 *
 * A background thread that is always active will handle the closing of expired requests.
 */
@Slf4j
public class AsyncRequestManager extends RecurrentTask
{
    /** List of requests that has timeouts */
    private final ConcurrentMap<UUID, SentRequest> sentRequestsById = new ConcurrentHashMap<>();

    /** Stores the number of actions performed in the current iteration */
    private final AtomicInteger numActionsInIteration = new AtomicInteger(0);

    /**
     * Constructor of a request sync manager
     *
     * @param instanceId unique ID of the library instance
     */
    public AsyncRequestManager(final UUID instanceId)
    {
        // 1 millisecond of idle strategy
        super(new SleepingIdleStrategy(TimeUnit.MILLISECONDS.toNanos(1)));
        this.start("AsyncRequestManager_" + instanceId);
    }

    @Override
    public int action()
    {
        this.numActionsInIteration.set(0);

        // Check expiration for each active sent request
        this.sentRequestsById.forEach((id, request) ->
        {
            // Check should stop to avoid processing new requests if stopping
            if (!this.shouldStop())
            {
                if (request.isClosed())
                {
                    this.sentRequestsById.remove(request.getRequestId());
                    this.numActionsInIteration.getAndIncrement();
                }
                else if (request.hasExpired())
                {
                    this.sentRequestsById.remove(request.getRequestId());
                    request.onRequestTimeout();
                    this.numActionsInIteration.getAndIncrement();
                }
            }
        });

        return this.numActionsInIteration.get();
    }

    @Override
    public void cleanUp()
    {
        // Close all pending requests
        this.sentRequestsById.values().forEach(SentRequest::closeRequest);
        this.sentRequestsById.clear();
    }

    /**
     * Add a new request into the manager
     * @param request the request to add
     */
    public void addNewRequest(final SentRequest request)
    {
        this.sentRequestsById.put(request.getRequestId(), request);
    }

    /**
     * Process a received response, it will look for a request that match the response in the internal map
     * @param response the response to process
     */
    public void processResponse(final RcvResponse response)
    {
        // Find the request for the received response
        final SentRequest sentRequest = this.sentRequestsById.get(response.getOriginalRequestId());

        if (sentRequest != null)
        {
            // Set the topic name
            response.setTopicName(sentRequest.getTopicName());
            // Notify the listener
            sentRequest.onResponseReceived(response);
        }
    }
}
