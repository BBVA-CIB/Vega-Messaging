package com.bbva.kyof.vega.msg;

import com.bbva.kyof.vega.protocol.publisher.AeronPublisher;
import org.agrona.concurrent.UnsafeBuffer;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by cnebrera on 02/08/16.
 */
public class RcvRequestTest
{
    @Test
    public void tryGettersSettersAndPromote()
    {
        // Create the message
        final UUID instanceId = UUID.randomUUID();
        final UUID requestId = UUID.randomUUID();
        final RcvRequest rcvRequest = new RcvRequest();
        final ByteBuffer bytebufferContentes = ByteBuffer.allocate(1024);
        final UnsafeBuffer unsafeContents = new UnsafeBuffer(bytebufferContentes);
        final AeronPublisher responderSocket = EasyMock.createNiceMock(AeronPublisher.class);
        EasyMock.replay(responderSocket);
        rcvRequest.setTopicName("TopicName");
        rcvRequest.setInstanceId(instanceId);
        rcvRequest.setUnsafeBufferContent(unsafeContents);
        rcvRequest.setContentOffset(0);
        rcvRequest.setContentLength(128);
        rcvRequest.setRequestId(requestId);
        rcvRequest.setRequestResponder(responderSocket);

        // Get
        Assert.assertEquals(rcvRequest.getTopicName(), "TopicName");
        Assert.assertEquals(rcvRequest.getInstanceId(), instanceId);
        Assert.assertTrue(rcvRequest.getContentOffset() == 0);
        Assert.assertTrue(rcvRequest.getContentLength() == 128);
        Assert.assertEquals(rcvRequest.getContents(), unsafeContents);
        Assert.assertEquals(rcvRequest.getRequestId(), requestId);

        rcvRequest.sendResponse(new UnsafeBuffer(ByteBuffer.allocate(128)), 0, 8);

        // Promote the message and try again
        final IRcvMessage promotedMessage = rcvRequest.promote();

        Assert.assertEquals(promotedMessage.getTopicName(), "TopicName");
        Assert.assertEquals(promotedMessage.getInstanceId(), instanceId);
        Assert.assertTrue(promotedMessage.getContentOffset() == 0);
        Assert.assertTrue(promotedMessage.getContentLength() == 128);
        Assert.assertNotNull(promotedMessage.getContents());
        Assert.assertNotEquals(promotedMessage.getContents(), unsafeContents);
        Assert.assertEquals(rcvRequest.getRequestId(), requestId);

        rcvRequest.sendResponse(new UnsafeBuffer(ByteBuffer.allocate(128)), 0, 8);
    }
}