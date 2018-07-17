package com.bbva.kyof.vega.msg;

import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Created by cnebrera on 02/08/16.
 */
public class BaseHeaderTest
{
    @Test
    public void fromBinaryToBinary()
    {
        // Create the header
        final BaseHeader testHeader = new BaseHeader(MsgType.DATA, Version.LOCAL_VERSION);

        // Create the buffer to serialize it
        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        UnsafeBufferSerializer serializer = new UnsafeBufferSerializer();
        serializer.wrap(buffer);

        // Write to binary
        testHeader.toBinary(serializer);

        // Check the current offset, should be the serialization size
        Assert.assertTrue(serializer.getOffset() == testHeader.serializedSize());

        // Flip the buffer
        buffer.limit(serializer.getOffset());
        buffer.position(0);

        // Wrap again and read
        serializer.wrap(buffer);

        BaseHeader readedHeader = new BaseHeader();
        readedHeader.fromBinary(serializer);

        // Check all values of the header
        Assert.assertEquals(testHeader, readedHeader);
        Assert.assertTrue(testHeader.isVersionCompatible());

        // Check again the limits
        Assert.assertTrue(serializer.getOffset() == readedHeader.serializedSize());
    }
}