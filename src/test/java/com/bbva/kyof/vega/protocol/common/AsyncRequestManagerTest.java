package com.bbva.kyof.vega.protocol.common;

import com.bbva.kyof.vega.msg.*;
import lombok.Getter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * Created by cnebrera on 11/08/16.
 */
public class AsyncRequestManagerTest
{
    AsyncRequestManager requestManager;
    UUID instanceId;
    List<SentRequest> requests;

    @Before
    public void before()
    {
        requests = new LinkedList<>();
        instanceId = UUID.randomUUID();
        requestManager = new AsyncRequestManager(instanceId);
    }

    @Test
    public void action() throws Exception
    {
        requestManager.close();

        // Make sure that after closing all requests have expired
        requests.forEach((request) -> Assert.assertTrue(request.isClosed()));
    }

    @Test
    public void addRequestAndWaitForTimeout() throws Exception
    {
        final Listener listener = new Listener();

        SentRequest sentRequest = new SentRequest("topic", 100, listener, new Random());
        requestManager.addNewRequest(sentRequest);
        requests.add(sentRequest);

        // Should not be closed nor expired
        Assert.assertFalse(sentRequest.isClosed());
        Assert.assertFalse(sentRequest.hasExpired());

        Thread.sleep(200);

        // Should be closed and expired
        Assert.assertTrue(sentRequest.isClosed());
        Assert.assertTrue(sentRequest.hasExpired());
        Assert.assertTrue(listener.getTimedOutRequests().contains(sentRequest.getRequestId()));

    }

    @Test
    public void closeRequestAndWaitForTimeout() throws Exception
    {
        final Listener listener = new Listener();

        SentRequest sentRequest = new SentRequest("topic", 100, listener, new Random());
        requestManager.addNewRequest(sentRequest);
        requests.add(sentRequest);

        // Close it
        sentRequest.closeRequest();

        // Should be closed but not expired and should have not called expired at all
        Thread.sleep(200);

        Assert.assertTrue(sentRequest.isClosed());
        Assert.assertFalse(listener.getTimedOutRequests().contains(sentRequest.getRequestId()));
    }

    @Test
    public void extendExpirationTime() throws Exception
    {
        final Listener listener = new Listener();

        SentRequest sentRequest = new SentRequest("topic", 100, listener, new Random());
        requestManager.addNewRequest(sentRequest);
        requests.add(sentRequest);

        // Extend expiration time
        sentRequest.resetExpiration(300);

        // Should not be expired
        Thread.sleep(200);

        Assert.assertFalse(sentRequest.isClosed());
        Assert.assertFalse(sentRequest.hasExpired());
        Assert.assertFalse(listener.getTimedOutRequests().contains(sentRequest.getRequestId()));

        // Wait a bit more
        Thread.sleep(400);

        Assert.assertTrue(sentRequest.isClosed());
        Assert.assertTrue(sentRequest.hasExpired());
        Assert.assertTrue(listener.getTimedOutRequests().contains(sentRequest.getRequestId()));
    }

    @Test
    public void responseReceived() throws Exception
    {
        final Listener listener = new Listener();

        SentRequest sentRequest = new SentRequest("topic", 100, listener, new Random());
        requestManager.addNewRequest(sentRequest);
        requests.add(sentRequest);

        // Wait a bit and send a response
        Thread.sleep(50);

        RcvResponse response = new RcvResponse();
        response.setOriginalRequestId(sentRequest.getRequestId());
        this.requestManager.processResponse(response);

        Assert.assertFalse(sentRequest.isClosed());
        Assert.assertFalse(sentRequest.hasExpired());
        Assert.assertFalse(listener.getTimedOutRequests().contains(sentRequest.getRequestId()));
        Assert.assertTrue(listener.getReceivedResponses().contains(sentRequest.getRequestId()));
        Assert.assertTrue(listener.getResponseReceivedRequests().contains(sentRequest.getRequestId()));

        // Wait a bit more, it should expire
        Thread.sleep(100);

        Assert.assertTrue(sentRequest.isClosed());
        Assert.assertTrue(sentRequest.hasExpired());
        Assert.assertTrue(listener.getTimedOutRequests().contains(sentRequest.getRequestId()));
    }

    @Test
    public void responseReceivedAndClose() throws Exception
    {
        final Listener listener = new Listener();

        SentRequest sentRequest = new SentRequest("topic", 100, listener, new Random());
        requestManager.addNewRequest(sentRequest);
        requests.add(sentRequest);

        // Wait a bit and send a response
        Thread.sleep(50);

        RcvResponse response = new RcvResponse();
        response.setOriginalRequestId(sentRequest.getRequestId());
        this.requestManager.processResponse(response);
        sentRequest.closeRequest();

        Assert.assertTrue(sentRequest.isClosed());
        Assert.assertFalse(sentRequest.hasExpired());
        Assert.assertFalse(listener.getTimedOutRequests().contains(sentRequest.getRequestId()));
        Assert.assertTrue(listener.getReceivedResponses().contains(sentRequest.getRequestId()));
        Assert.assertTrue(listener.getResponseReceivedRequests().contains(sentRequest.getRequestId()));

        // Wait a bit more, it should expire
        Thread.sleep(100);

        Assert.assertTrue(sentRequest.isClosed());
        Assert.assertTrue(sentRequest.hasExpired());
        Assert.assertFalse(listener.getTimedOutRequests().contains(sentRequest.getRequestId()));
    }

    @Test
    public void responseReceivedAfterClose() throws Exception
    {
        final Listener listener = new Listener();

        SentRequest sentRequest = new SentRequest("topic", 100, listener, new Random());
        requestManager.addNewRequest(sentRequest);
        requests.add(sentRequest);

        // Wait a bit and send a response
        Thread.sleep(50);

        RcvResponse response = new RcvResponse();
        response.setOriginalRequestId(sentRequest.getRequestId());
        sentRequest.closeRequest();
        this.requestManager.processResponse(response);

        Thread.sleep(10);

        Assert.assertTrue(sentRequest.isClosed());
        Assert.assertFalse(sentRequest.hasExpired());
        Assert.assertFalse(listener.getTimedOutRequests().contains(sentRequest.getRequestId()));
        Assert.assertFalse(listener.getReceivedResponses().contains(sentRequest.getRequestId()));
        Assert.assertFalse(listener.getResponseReceivedRequests().contains(sentRequest.getRequestId()));

        // Wait a bit more, it should expire, but is closed
        Thread.sleep(100);

        Assert.assertTrue(sentRequest.isClosed());
        Assert.assertTrue(sentRequest.hasExpired());
        Assert.assertFalse(listener.getTimedOutRequests().contains(sentRequest.getRequestId()));
    }

    @Test
    public void wrongResponseReceived() throws Exception
    {
        final Listener listener = new Listener();

        requestManager.addNewRequest(new SentRequest("topic", 100, listener, new Random()));
        requestManager.addNewRequest(new SentRequest("topic", 100, listener, new Random()));
        requestManager.addNewRequest(new SentRequest("topic", 100, listener, new Random()));
    }

    @Test
    public void closeRequestsAfterClosingManager() throws Exception
    {
        final Listener listener = new Listener();

        SentRequest sentRequest = new SentRequest("topic", 100, listener, new Random());
        requestManager.addNewRequest(sentRequest);
    }

    class Listener implements IResponseListener
    {
        @Getter final Set<UUID> timedOutRequests = new HashSet<>();
        @Getter final Set<UUID> responseReceivedRequests = new HashSet<>();
        @Getter final Set<UUID> receivedResponses = new HashSet<>();

        @Override
        public void onRequestTimeout(ISentRequest originalSentRequest)
        {
            timedOutRequests.add(originalSentRequest.getRequestId());
        }

        @Override
        public void onResponseReceived(ISentRequest originalSentRequest, IRcvResponse response)
        {
            responseReceivedRequests.add(originalSentRequest.getRequestId());
            receivedResponses.add(response.getOriginalRequestId());
        }
    }
}