package com.bbva.kyof.vega.msg;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by cnebrera on 02/08/16.
 */
public class BaseRcvMessageTest
{
    @Test
    public void tryGettersSettersAndPromote()
    {
        // Create the message
        final UUID instanceId = UUID.randomUUID();
        final BaseRcvMessage baseRcvMessage = new BaseRcvMessage() {};
        final ByteBuffer bytebufferContentes = ByteBuffer.allocate(1024);
        final UnsafeBuffer unsafeContents = new UnsafeBuffer(bytebufferContentes);
        baseRcvMessage.setTopicName("TopicName");
        baseRcvMessage.setInstanceId(instanceId);
        baseRcvMessage.setUnsafeBufferContent(unsafeContents);
        baseRcvMessage.setContentOffset(0);
        baseRcvMessage.setContentLength(128);

        // Get
        Assert.assertEquals(baseRcvMessage.getTopicName(), "TopicName");
        Assert.assertEquals(baseRcvMessage.getInstanceId(), instanceId);
        Assert.assertTrue(baseRcvMessage.getContentOffset() == 0);
        Assert.assertTrue(baseRcvMessage.getContentLength() == 128);
        Assert.assertEquals(baseRcvMessage.getContents(), unsafeContents);

        // Promote the message and try again
        final BaseRcvMessage promotedMessage = new BaseRcvMessage() {};
        baseRcvMessage.promote(promotedMessage);

        Assert.assertEquals(promotedMessage.getTopicName(), "TopicName");
        Assert.assertEquals(promotedMessage.getInstanceId(), instanceId);
        Assert.assertTrue(promotedMessage.getContentOffset() == 0);
        Assert.assertTrue(promotedMessage.getContentLength() == 128);
        Assert.assertNotNull(promotedMessage.getContents());
        Assert.assertNotEquals(promotedMessage.getContents(), unsafeContents);
    }

    @Test
    public void getContentAsByteBufferOrArray()
    {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(128);
        final UnsafeBuffer unsafeContents = new UnsafeBuffer(byteBuffer);

        unsafeContents.putInt(4, 128);
        unsafeContents.putInt(8, 256);

        final BaseRcvMessage baseRcvMessage = new BaseRcvMessage() {};

        baseRcvMessage.setUnsafeBufferContent(unsafeContents);
        baseRcvMessage.setContentLength(8);
        baseRcvMessage.setContentOffset(4);

        // Promote the message
        final BaseRcvMessage promotedMessage = new BaseRcvMessage() {};
        baseRcvMessage.promote(promotedMessage);

        Assert.assertTrue(promotedMessage.getContents().getInt(0) == 128);
        Assert.assertTrue(promotedMessage.getContents().getInt(4) == 256);
        Assert.assertTrue(promotedMessage.getContentLength() == 8);
        Assert.assertTrue(promotedMessage.getContentOffset() == 0);
    }
}