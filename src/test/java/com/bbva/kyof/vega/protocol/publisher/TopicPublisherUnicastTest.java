package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.msg.PublishResult;
import com.bbva.kyof.vega.msg.SentRequest;
import com.bbva.kyof.vega.protocol.common.AsyncRequestManager;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import org.agrona.concurrent.UnsafeBuffer;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by cnebrera on 11/08/16.
 */
public class TopicPublisherUnicastTest
{
    private TopicTemplateConfig topicConfig;
    private VegaContext vegaContext;
    private AsyncRequestManager asyncRequestManager;
    private int sentRequests;
    private int sentMessages;

    @Before
    public void beforeTest()
    {
        sentMessages = 0;
        sentRequests = 0;

        topicConfig = TopicTemplateConfig.builder().name("name").transportType(TransportMediaType.UNICAST).build();
        vegaContext = new VegaContext(null, null);

        asyncRequestManager = new AsyncRequestManager(UUID.randomUUID());
        vegaContext.setAsyncRequestManager(asyncRequestManager);
    }

    @After
    public void afterTest()
    {
        asyncRequestManager.close();
    }

    @Test
    public void testSendAndClose() throws Exception
    {
        final TopicPublisherUnicast topicPublisher = new TopicPublisherUnicast("topic", topicConfig, vegaContext);

        // Create the Aeron publishers
        AeronPublisher publisher1 = createAeronPublisherMock(PublishResult.OK);
        AeronPublisher publisher2 = createAeronPublisherMock(PublishResult.OK);
        AeronPublisher publisher3 = createAeronPublisherMock(PublishResult.OK);
        topicPublisher.addAeronPublisher(publisher1);
        topicPublisher.addAeronPublisher(publisher2);
        topicPublisher.addAeronPublisher(publisher3);
        
        // Check sequence number before sending the messages
        Assert.assertEquals(0, topicPublisher.getSequenceNumber());

        // Send a message, should habe been sent 3 times and result is OK since all internal publishers have give OK
        UnsafeBuffer message = new UnsafeBuffer(ByteBuffer.allocate(1024));
        Assert.assertEquals(topicPublisher.sendMsg(message, 0, 1024), PublishResult.OK);
        Assert.assertEquals(3, this.sentMessages);

        // Check sequence number after sending the request
        Assert.assertEquals(1, topicPublisher.getSequenceNumber());

        // Send a request, should habe been sent 3 times and result is OK since all internal publishers have give OK
        message =  new UnsafeBuffer(ByteBuffer.allocate(1024));
        SentRequest sentRequest = topicPublisher.sendRequest(message, 0, 1024, 100L, null);
        Assert.assertEquals(sentRequest.getSentResult(), PublishResult.OK);
        Assert.assertEquals(3, this.sentRequests);
        Assert.assertEquals(3, this.sentMessages);
        
        // Check sequence number after sending the request
        Assert.assertEquals(2, topicPublisher.getSequenceNumber());

        // Wait a bit, the request should close by expiration
        Thread.sleep(200);
        assertTrue(sentRequest.isClosed());

        // Now close the publisher
        topicPublisher.close();

        // Send should not work anymore
        Assert.assertEquals(topicPublisher.sendMsg(message, 0, 1024), PublishResult.UNEXPECTED_ERROR);
        Assert.assertEquals(3, this.sentMessages);

        sentRequest = topicPublisher.sendRequest(message, 0, 1024, 100L, null);
        Assert.assertEquals(sentRequest.getSentResult(), PublishResult.UNEXPECTED_ERROR);
        Assert.assertEquals(3, this.sentRequests);
        Assert.assertEquals(3, this.sentMessages);
    }

    @Test
    public void testBackPressureError()
    {
        final TopicPublisherUnicast topicPublisher = new TopicPublisherUnicast("topic", topicConfig, vegaContext);

        // Create the Aeron publishers
        AeronPublisher publisher1 = createAeronPublisherMock(PublishResult.OK);
        AeronPublisher publisher2 = createAeronPublisherMock(PublishResult.BACK_PRESSURED);
        AeronPublisher publisher3 = createAeronPublisherMock(PublishResult.OK);
        topicPublisher.addAeronPublisher(publisher1);
        topicPublisher.addAeronPublisher(publisher2);
        topicPublisher.addAeronPublisher(publisher3);

        // Send a message, should have been sent 3 times and result is BACK_PRESSURED
        UnsafeBuffer message = new UnsafeBuffer(ByteBuffer.allocate(1024));
        Assert.assertEquals(topicPublisher.sendMsg(message, 0, 1024), PublishResult.BACK_PRESSURED);
        Assert.assertEquals(3, this.sentMessages);

        SentRequest sentRequest = topicPublisher.sendRequest(message, 0, 1024, 100L, null);
        Assert.assertEquals(sentRequest.getSentResult(), PublishResult.BACK_PRESSURED);
        Assert.assertEquals(3, this.sentRequests);
    }

    @Test
    public void testUnexpectedError()
    {
        final TopicPublisherUnicast topicPublisher = new TopicPublisherUnicast("topic", topicConfig, vegaContext);

        // Create the Aeron publishers
        AeronPublisher publisher1 = createAeronPublisherMock(PublishResult.OK);
        AeronPublisher publisher2 = createAeronPublisherMock(PublishResult.UNEXPECTED_ERROR);
        AeronPublisher publisher3 = createAeronPublisherMock(PublishResult.OK);
        topicPublisher.addAeronPublisher(publisher1);
        topicPublisher.addAeronPublisher(publisher2);
        topicPublisher.addAeronPublisher(publisher3);

        // Send a message, should have been sent 1 times, as soon as there is unexpected error it should stop
        UnsafeBuffer message = new UnsafeBuffer(ByteBuffer.allocate(1024));
        Assert.assertEquals(topicPublisher.sendMsg(message, 0, 1024), PublishResult.UNEXPECTED_ERROR);
        Assert.assertEquals(2, this.sentMessages);

        SentRequest sentRequest = topicPublisher.sendRequest(message, 0, 1024, 100L, null);
        Assert.assertEquals(sentRequest.getSentResult(), PublishResult.UNEXPECTED_ERROR);
        Assert.assertEquals(2, this.sentRequests);

        // Remove wrong one and test again
        topicPublisher.removeAeronPublisher(publisher2);

        Assert.assertEquals(topicPublisher.sendMsg(message, 0, 1024), PublishResult.OK);
        assertEquals(4, this.sentMessages);
    }

    private AeronPublisher createAeronPublisherMock(PublishResult pubResult)
    {
        AeronPublisher publisher = EasyMock.createNiceMock(AeronPublisher.class);
        EasyMock.expect(publisher.sendMessage(EasyMock.anyByte(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyLong(), EasyMock.anyInt(), EasyMock.anyInt())).andAnswer(() -> this.sendMessage(pubResult)).anyTimes();
        EasyMock.expect(publisher.sendRequest(EasyMock.anyByte(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyLong(), EasyMock.anyInt(), EasyMock.anyInt())).andAnswer(() -> this.sendRequest(pubResult)).anyTimes();
        EasyMock.replay(publisher);
        return publisher;
    }

    private PublishResult sendRequest(PublishResult pubResult)
    {
        sentRequests++;
        return pubResult;
    }

    private PublishResult sendMessage(PublishResult pubResult)
    {
        System.out.println("Send message " + pubResult);
        sentMessages++;
        return pubResult;
    }
}