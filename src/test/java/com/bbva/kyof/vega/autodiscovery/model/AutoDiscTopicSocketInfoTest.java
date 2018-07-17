package com.bbva.kyof.vega.autodiscovery.model;

import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by cnebrera on 02/08/16.
 */
public class AutoDiscTopicSocketInfoTest
{
    final static int IP = 44;
    final static int PORT = 33;
    final static int STREAM_ID = 22;

    @Test
    public void fromBinaryToBinary()
    {
        // Create the header
        final UUID instanceId = UUID.randomUUID();
        final UUID uniqueId = UUID.randomUUID();
        final UUID topicId = UUID.randomUUID();

        final AutoDiscTopicSocketInfo topicSocketInfo = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId, "topic", topicId, IP, PORT, STREAM_ID);

        // Create the buffer to serialize it
        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        UnsafeBufferSerializer serializer = new UnsafeBufferSerializer();
        serializer.wrap(buffer);

        // Write to binary
        topicSocketInfo.toBinary(serializer);

        // Check the current offset, should be the serialization size
        Assert.assertTrue(serializer.getOffset() == topicSocketInfo.serializedSize());

        // Flip the buffer
        buffer.limit(serializer.getOffset());
        buffer.position(0);

        // Wrap again and read
        serializer.wrap(buffer);

        AutoDiscTopicSocketInfo readedInfo = new AutoDiscTopicSocketInfo();
        readedInfo.fromBinary(serializer);

        // Check all values
        Assert.assertEquals(topicSocketInfo, topicSocketInfo);
        Assert.assertEquals(topicSocketInfo, readedInfo);
        Assert.assertNotEquals(topicSocketInfo, null);
        Assert.assertNotEquals(topicSocketInfo, new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic", topicId, IP, PORT, STREAM_ID));
        Assert.assertNotNull(topicSocketInfo.toString());
        Assert.assertTrue(topicSocketInfo.hashCode() == readedInfo.hashCode());
        Assert.assertEquals(topicSocketInfo.getInstanceId(), instanceId);
        Assert.assertEquals(topicSocketInfo.getUniqueId(), uniqueId);
        Assert.assertEquals(topicSocketInfo.getTopicName(), "topic");
        Assert.assertEquals(topicSocketInfo.getTransportType(), AutoDiscTransportType.PUB_IPC);
        Assert.assertEquals(topicSocketInfo.getTopicId(), topicId);
        Assert.assertTrue(topicSocketInfo.getIpAddress() == IP);
        Assert.assertTrue(topicSocketInfo.getPort() == PORT);
        Assert.assertTrue(topicSocketInfo.getStreamId() == STREAM_ID);
        Assert.assertFalse(topicSocketInfo.hasSecurity());

        // Check again the limits
        Assert.assertTrue(serializer.getOffset() == readedInfo.serializedSize());
    }

    @Test
    public void fromBinaryToBinaryWithSecurity()
    {
        // Create the header
        final UUID instanceId = UUID.randomUUID();
        final UUID uniqueId = UUID.randomUUID();
        final UUID topicId = UUID.randomUUID();

        final AutoDiscTopicSocketInfo topicSocketInfo = new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId, "topic", topicId, IP, PORT, STREAM_ID, 22);

        // Create the buffer to serialize it
        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        UnsafeBufferSerializer serializer = new UnsafeBufferSerializer();
        serializer.wrap(buffer);

        // Write to binary
        topicSocketInfo.toBinary(serializer);

        // Check the current offset, should be the serialization size
        Assert.assertTrue(serializer.getOffset() == topicSocketInfo.serializedSize());

        // Flip the buffer
        buffer.limit(serializer.getOffset());
        buffer.position(0);

        // Wrap again and read
        serializer.wrap(buffer);

        AutoDiscTopicSocketInfo readedInfo = new AutoDiscTopicSocketInfo();
        readedInfo.fromBinary(serializer);

        // Check all values
        Assert.assertEquals(topicSocketInfo, topicSocketInfo);
        Assert.assertEquals(topicSocketInfo, readedInfo);
        Assert.assertNotEquals(topicSocketInfo, null);
        Assert.assertNotEquals(topicSocketInfo, new AutoDiscTopicSocketInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic", topicId, IP, PORT, STREAM_ID));
        Assert.assertNotNull(topicSocketInfo.toString());
        Assert.assertTrue(topicSocketInfo.hashCode() == readedInfo.hashCode());
        Assert.assertEquals(topicSocketInfo.getInstanceId(), instanceId);
        Assert.assertEquals(topicSocketInfo.getUniqueId(), uniqueId);
        Assert.assertEquals(topicSocketInfo.getTopicName(), "topic");
        Assert.assertEquals(topicSocketInfo.getTransportType(), AutoDiscTransportType.PUB_IPC);
        Assert.assertEquals(topicSocketInfo.getTopicId(), topicId);
        Assert.assertTrue(topicSocketInfo.getIpAddress() == IP);
        Assert.assertTrue(topicSocketInfo.getPort() == PORT);
        Assert.assertTrue(topicSocketInfo.getStreamId() == STREAM_ID);
        Assert.assertTrue(topicSocketInfo.hasSecurity());
        Assert.assertTrue(topicSocketInfo.getSecurityId() == 22);

        // Check again the limits
        Assert.assertTrue(serializer.getOffset() == readedInfo.serializedSize());
    }
}