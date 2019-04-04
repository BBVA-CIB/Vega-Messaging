package com.bbva.kyof.vega.msg;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Test the class VegaRcvMessageTest
 */
public class RcvMessageTest
{
    @Test
    public void tryGettersSettersAndPromote()
    {
        // Create the message
        final UUID instanceId = UUID.randomUUID();
        final UUID publisherId = UUID.randomUUID();
        final RcvMessage rcvMessage = new RcvMessage();
        final ByteBuffer bytebufferContentes = ByteBuffer.allocate(1024);
        final UnsafeBuffer unsafeContents = new UnsafeBuffer(bytebufferContentes);

        rcvMessage.setTopicName("TopicName");
        rcvMessage.setInstanceId(instanceId);
        rcvMessage.setUnsafeBufferContent(unsafeContents);
        rcvMessage.setContentOffset(0);
        rcvMessage.setContentLength(128);
        rcvMessage.setTopicPublisherId(publisherId);

        // Get
        Assert.assertEquals(rcvMessage.getTopicName(), "TopicName");
        Assert.assertEquals(rcvMessage.getInstanceId(), instanceId);
        Assert.assertTrue(rcvMessage.getContentOffset() == 0);
        Assert.assertTrue(rcvMessage.getContentLength() == 128);
        Assert.assertEquals(rcvMessage.getContents(), unsafeContents);
        Assert.assertEquals(rcvMessage.getTopicPublisherId(), publisherId);

        // Promote the message and try again
        final IRcvMessage promotedMessage = rcvMessage.promote();

        Assert.assertEquals(promotedMessage.getTopicName(), "TopicName");
        Assert.assertEquals(promotedMessage.getInstanceId(), instanceId);
        Assert.assertTrue(promotedMessage.getContentOffset() == 0);
        Assert.assertTrue(promotedMessage.getContentLength() == 128);
        Assert.assertNotNull(promotedMessage.getContents());
        Assert.assertNotEquals(promotedMessage.getContents(), unsafeContents);
        Assert.assertEquals(rcvMessage.getTopicPublisherId(), publisherId);
    }
}