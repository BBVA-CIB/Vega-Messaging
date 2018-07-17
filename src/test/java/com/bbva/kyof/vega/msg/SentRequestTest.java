package com.bbva.kyof.vega.msg;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

/**
 * Created by cnebrera on 02/08/16.
 */
public class SentRequestTest implements IResponseListener
{
    private SentRequest sentRequest;
    private int numResponses;
    private boolean hasTimedOut;

    @Before
    public void setUp()
    {
        this.sentRequest = new SentRequest("topicName", 100, this, new Random());
        this.numResponses = 0;
        this.hasTimedOut = false;
    }

    @Test
    public void testGetterSetter()
    {
        Assert.assertEquals(sentRequest.getTopicName(), "topicName");
        Assert.assertNotNull(sentRequest.getRequestId());
        Assert.assertNull(sentRequest.getSentResult());
        Assert.assertEquals(sentRequest.getNumberOfResponses(), 0);
    }

    @Test
    public void testClose()
    {
        this.sentRequest.closeRequest();
        Assert.assertTrue(this.sentRequest.isClosed());
    }

    @Test
    public void testExpired() throws Exception
    {
        Assert.assertFalse(this.sentRequest.hasExpired());

        Thread.sleep(300);

        Assert.assertTrue(this.sentRequest.hasExpired());
    }

    @Test
    public void testResetExpiration() throws Exception
    {
        Assert.assertFalse(this.sentRequest.hasExpired());
        this.sentRequest.resetExpiration(1000);
        Thread.sleep(300);
        Assert.assertFalse(this.sentRequest.hasExpired());
    }

    @Test
    public void testResponseReceived()
    {
        this.sentRequest.onResponseReceived(new RcvResponse());
        this.sentRequest.onResponseReceived(new RcvResponse());

        Assert.assertTrue(this.sentRequest.getNumberOfResponses() == this.numResponses);
        Assert.assertTrue(this.sentRequest.getNumberOfResponses() == 2) ;

        // Close, there should be no new responses
        this.sentRequest.closeRequest();
        this.sentRequest.onResponseReceived(new RcvResponse());

        Assert.assertTrue(this.sentRequest.getNumberOfResponses() == this.numResponses);
        Assert.assertTrue(this.sentRequest.getNumberOfResponses() == 2) ;
    }

    @Test
    public void testTimeout()
    {
        this.sentRequest.onRequestTimeout();

        Assert.assertTrue(this.hasTimedOut);
    }

    @Test
    public void testTimeoutAfterClose()
    {
        this.sentRequest.closeRequest();
        this.sentRequest.onRequestTimeout();

        Assert.assertFalse(this.hasTimedOut);
    }

    @Test
    public void testWithNoListeners()
    {
        final SentRequest request = new SentRequest("topicName", 100, null, new Random());

        request.onRequestTimeout();
        request.onResponseReceived(new RcvResponse());

        Assert.assertTrue(this.numResponses == 0);
        Assert.assertFalse(this.hasTimedOut);
        Assert.assertNotEquals(sentRequest.getRequestId(), request.getRequestId());
    }

    @Test
    public void testUncaughtExceptions()
    {
        final IResponseListener respListener = new IResponseListener()
        {
            @Override
            public void onResponseReceived(ISentRequest originalSentRequest, IRcvResponse response)
            {
                throw new IllegalArgumentException();
            }

            @Override
            public void onRequestTimeout(ISentRequest originalSentRequest)
            {
                throw new IllegalArgumentException();
            }
        };


        final SentRequest request = new SentRequest("topicName", 100, respListener, new Random());

        // The exceptions should not propagate
        request.onResponseReceived(new RcvResponse());
        request.onRequestTimeout();
    }

    @Override
    public void onRequestTimeout(ISentRequest originalSentRequest)
    {
        this.hasTimedOut = true;
    }

    @Override
    public void onResponseReceived(ISentRequest originalSentRequest, IRcvResponse response)
    {
        this.numResponses++;
    }
}