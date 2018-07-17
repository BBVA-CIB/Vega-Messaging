package com.bbva.kyof.vega.msg;

import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;

/**
 * Test the {@link MsgDataHeader} class
 */
public class MsgDataHeaderTest
{
    @Test
    public void fromBinaryToBinary()
    {
        // Create the header
        final UUID instanceId = UUID.randomUUID();
        final UUID topicId = UUID.randomUUID();
        final long sequenceNumber = new Random().nextLong();
        final MsgDataHeader testHeader = new MsgDataHeader(instanceId, topicId, sequenceNumber);

        // Create the buffer to serialize it
        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        UnsafeBufferSerializer serializer = new UnsafeBufferSerializer();
        serializer.wrap(buffer);

        // Write to binary
        testHeader.toBinary(serializer);

        // Check the current offset, should be the serialization size
        Assert.assertEquals(serializer.getOffset(), testHeader.serializedSize());

        // Flip the buffer
        buffer.limit(serializer.getOffset());
        buffer.position(0);

        // Wrap again and read
        serializer.wrap(buffer);

        MsgDataHeader readedHeader = new MsgDataHeader();
        readedHeader.fromBinary(serializer);

        // Check all values of the header
        org.junit.Assert.assertEquals(testHeader, readedHeader);

        // Check again the limits
        Assert.assertEquals(serializer.getOffset(), readedHeader.serializedSize());
    }
}