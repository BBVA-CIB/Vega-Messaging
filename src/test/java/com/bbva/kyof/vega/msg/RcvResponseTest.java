package com.bbva.kyof.vega.msg;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by cnebrera on 02/08/16.
 */
public class RcvResponseTest
{
    @Test
    public void tryGettersSettersAndPromote()
    {
        // Create the message
        final UUID instanceId = UUID.randomUUID();
        final UUID requestId = UUID.randomUUID();
        final RcvResponse rcvResponse = new RcvResponse();
        final ByteBuffer bytebufferContentes = ByteBuffer.allocate(1024);
        final UnsafeBuffer unsafeContents = new UnsafeBuffer(bytebufferContentes);

        rcvResponse.setTopicName("TopicName");
        rcvResponse.setInstanceId(instanceId);
        rcvResponse.setUnsafeBufferContent(unsafeContents);
        rcvResponse.setContentOffset(0);
        rcvResponse.setContentLength(128);
        rcvResponse.setOriginalRequestId(requestId);

        // Get
        Assert.assertEquals(rcvResponse.getTopicName(), "TopicName");
        Assert.assertEquals(rcvResponse.getInstanceId(), instanceId);
        Assert.assertTrue(rcvResponse.getContentOffset() == 0);
        Assert.assertTrue(rcvResponse.getContentLength() == 128);
        Assert.assertEquals(rcvResponse.getContents(), unsafeContents);
        Assert.assertEquals(rcvResponse.getOriginalRequestId(), requestId);

        // Promote the message and try again
        final IRcvMessage promotedMessage = rcvResponse.promote();

        Assert.assertEquals(promotedMessage.getTopicName(), "TopicName");
        Assert.assertEquals(promotedMessage.getInstanceId(), instanceId);
        Assert.assertTrue(promotedMessage.getContentOffset() == 0);
        Assert.assertTrue(promotedMessage.getContentLength() == 128);
        Assert.assertNotNull(promotedMessage.getContents());
        Assert.assertNotEquals(promotedMessage.getContents(), unsafeContents);
        Assert.assertEquals(rcvResponse.getOriginalRequestId(), requestId);
    }
}