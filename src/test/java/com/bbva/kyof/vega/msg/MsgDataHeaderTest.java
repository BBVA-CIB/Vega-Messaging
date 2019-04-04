package com.bbva.kyof.vega.msg;

import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import org.junit.Test;

import java.nio.ByteBuffer;
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
        final MsgDataHeader testHeader = new MsgDataHeader(instanceId, topicId);

        // Create the buffer to serialize it
        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        UnsafeBufferSerializer serializer = new UnsafeBufferSerializer();
        serializer.wrap(buffer);

        // Write to binary
        testHeader.toBinary(serializer);

        // Check the current offset, should be the serialization size
        org.junit.Assert.assertTrue(serializer.getOffset() == testHeader.serializedSize());

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
        org.junit.Assert.assertTrue(serializer.getOffset() == readedHeader.serializedSize());
    }
}