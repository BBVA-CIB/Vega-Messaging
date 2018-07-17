package com.bbva.kyof.vega.autodiscovery.model;

import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by cnebrera on 02/08/16.
 */
public class AutoDiscTopicInfoTest
{
    @Test
    public void fromBinaryToBinary()
    {
        // Create the header
        final UUID uniqueId = UUID.randomUUID();

        final UUID instanceId = UUID.randomUUID();
        final AutoDiscTopicInfo topicInfo = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId, "topic");

        // Create the buffer to serialize it
        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        UnsafeBufferSerializer serializer = new UnsafeBufferSerializer();
        serializer.wrap(buffer);

        // Write to binary
        topicInfo.toBinary(serializer);

        // Check the current offset, should be the serialization size
        Assert.assertTrue(serializer.getOffset() == topicInfo.serializedSize());

        // Flip the buffer
        buffer.limit(serializer.getOffset());
        buffer.position(0);

        // Wrap again and read
        serializer.wrap(buffer);

        AutoDiscTopicInfo readedInfo = new AutoDiscTopicInfo();
        readedInfo.fromBinary(serializer);

        // Check all values
        Assert.assertEquals(topicInfo, topicInfo);
        Assert.assertEquals(topicInfo, readedInfo);
        Assert.assertNotEquals(topicInfo, null);
        Assert.assertNotEquals(topicInfo, new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic"));
        Assert.assertNotNull(topicInfo.toString());
        Assert.assertTrue(topicInfo.hashCode() == readedInfo.hashCode());
        Assert.assertEquals(topicInfo.getTransportType(), AutoDiscTransportType.PUB_IPC);
        Assert.assertEquals(topicInfo.getUniqueId(), uniqueId);
        Assert.assertEquals(topicInfo.getTopicName(), "topic");
        Assert.assertFalse(topicInfo.hasSecurity());
        Assert.assertEquals(readedInfo.getInstanceId(), instanceId);

        // Check again the limits
        Assert.assertTrue(serializer.getOffset() == readedInfo.serializedSize());
    }

    @Test
    public void fromBinaryToBinaryWithSecurityId()
    {
        // Create the header
        final UUID instanceId = UUID.randomUUID();
        final UUID uniqueId = UUID.randomUUID();
        final int securityId = 12345;

        final AutoDiscTopicInfo topicInfo = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId, "topic", securityId);

        // Create the buffer to serialize it
        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        UnsafeBufferSerializer serializer = new UnsafeBufferSerializer();
        serializer.wrap(buffer);

        // Write to binary
        topicInfo.toBinary(serializer);

        // Check the current offset, should be the serialization size
        Assert.assertTrue(serializer.getOffset() == topicInfo.serializedSize());

        // Flip the buffer
        buffer.limit(serializer.getOffset());
        buffer.position(0);

        // Wrap again and read
        serializer.wrap(buffer);

        AutoDiscTopicInfo readedInfo = new AutoDiscTopicInfo();
        readedInfo.fromBinary(serializer);

        // Check all values
        Assert.assertEquals(topicInfo, topicInfo);
        Assert.assertEquals(topicInfo, readedInfo);
        Assert.assertNotEquals(topicInfo, null);
        Assert.assertNotEquals(topicInfo, new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic", securityId));
        Assert.assertNotNull(topicInfo.toString());
        Assert.assertTrue(topicInfo.hashCode() == readedInfo.hashCode());
        Assert.assertEquals(topicInfo.getTransportType(), AutoDiscTransportType.PUB_IPC);
        Assert.assertEquals(topicInfo.getUniqueId(), uniqueId);
        Assert.assertEquals(topicInfo.getTopicName(), "topic");
        Assert.assertTrue(topicInfo.getSecurityId() == securityId);
        Assert.assertTrue(topicInfo.hasSecurity());
        Assert.assertEquals(readedInfo.getInstanceId(), instanceId);

        // Check again the limits
        Assert.assertTrue(serializer.getOffset() == readedInfo.serializedSize());
    }
}