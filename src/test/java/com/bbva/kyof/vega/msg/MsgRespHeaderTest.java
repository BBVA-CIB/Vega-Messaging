package com.bbva.kyof.vega.msg;

import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by cnebrera on 02/08/16.
 */
public class MsgRespHeaderTest
{
    @Test
    public void fromBinaryToBinary()
    {
        // Create the header
        final UUID instanceId = UUID.randomUUID();
        final UUID requestId = UUID.randomUUID();
        final MsgRespHeader testHeader = new MsgRespHeader(instanceId, requestId);

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

        MsgRespHeader readedHeader = new MsgRespHeader();
        readedHeader.fromBinary(serializer);

        // Check all values of the header
        org.junit.Assert.assertEquals(testHeader, readedHeader);

        // Check again the limits
        org.junit.Assert.assertTrue(serializer.getOffset() == readedHeader.serializedSize());
    }
}