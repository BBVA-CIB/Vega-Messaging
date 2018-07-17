package com.bbva.kyof.vega.serialization;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Class created to test {@link UnsafeBufferSerializer}
 *
 * Created by XE52727 on 05/07/2016.
 */
public class UnsafeBufferSerializerTest
{
    /** Reusable send buffer */
    private static ByteBuffer REUSABLE_SEND_BUFFER = ByteBuffer.allocate(1024);

    /** Reusable send buffer serializer */
    private static UnsafeBufferSerializer BUFFER_SERIALIZER = new UnsafeBufferSerializer();

    @Before
    public void setUp() throws Exception
    {
        BUFFER_SERIALIZER.wrap(REUSABLE_SEND_BUFFER);
    }

    @After
    public void tearDown() throws Exception
    {
        REUSABLE_SEND_BUFFER.clear();
        BUFFER_SERIALIZER.wrap(REUSABLE_SEND_BUFFER);
    }

    @Test
    public void wrap() throws Exception
    {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(128));

        BUFFER_SERIALIZER.wrap(buffer);
        BUFFER_SERIALIZER.writeInt(786777);
        BUFFER_SERIALIZER.wrap(buffer);

        Assert.assertEquals(786777, BUFFER_SERIALIZER.readInt());

        BUFFER_SERIALIZER.wrap(buffer);

        Assert.assertEquals(0, BUFFER_SERIALIZER.getOffset());
        Assert.assertEquals(buffer.capacity(), BUFFER_SERIALIZER.getMsgLength());
    }

    @Test
    public void wrap1() throws Exception
    {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(128));

        BUFFER_SERIALIZER.wrap(buffer, 0, 4);
        BUFFER_SERIALIZER.writeInt(786777);
        BUFFER_SERIALIZER.wrap(buffer, 0, 4);

        Assert.assertEquals(786777, BUFFER_SERIALIZER.readInt());

        BUFFER_SERIALIZER.wrap(buffer, 0, 4);

        Assert.assertEquals(0, BUFFER_SERIALIZER.getOffset());
        Assert.assertEquals(4, BUFFER_SERIALIZER.getMsgLength());
    }

    @Test
    public void wrap2() throws Exception
    {
        BUFFER_SERIALIZER.writeInt(786777);

        REUSABLE_SEND_BUFFER.flip();
        BUFFER_SERIALIZER.wrap(REUSABLE_SEND_BUFFER);

        Assert.assertEquals(786777, BUFFER_SERIALIZER.readInt());

        REUSABLE_SEND_BUFFER.flip();
        BUFFER_SERIALIZER.wrap(REUSABLE_SEND_BUFFER);

        Assert.assertEquals(REUSABLE_SEND_BUFFER.position(), BUFFER_SERIALIZER.getOffset());
        Assert.assertEquals(REUSABLE_SEND_BUFFER.capacity(), BUFFER_SERIALIZER.getMsgLength());
    }

    @Test
    public void wrap3() throws Exception
    {
        BUFFER_SERIALIZER.writeInt(786777);

        REUSABLE_SEND_BUFFER.flip();
        BUFFER_SERIALIZER.wrap(REUSABLE_SEND_BUFFER, 0, 4);

        Assert.assertEquals(786777, BUFFER_SERIALIZER.readInt());

        REUSABLE_SEND_BUFFER.flip();
        BUFFER_SERIALIZER.wrap(REUSABLE_SEND_BUFFER);

        Assert.assertEquals(REUSABLE_SEND_BUFFER.position(), BUFFER_SERIALIZER.getOffset());
        Assert.assertEquals(REUSABLE_SEND_BUFFER.capacity(), BUFFER_SERIALIZER.getMsgLength());
    }

    @Test
    public void byteSerialization() throws Exception
    {
        BUFFER_SERIALIZER.writeByte((byte)12);
        BUFFER_SERIALIZER.writeByte((byte)24);

        REUSABLE_SEND_BUFFER.flip();
        BUFFER_SERIALIZER.wrap(REUSABLE_SEND_BUFFER);

        Assert.assertTrue(BUFFER_SERIALIZER.readByte() == 12);
        Assert.assertTrue(BUFFER_SERIALIZER.getOffset() == UnsafeBufferSerializer.BYTE_SIZE);
        Assert.assertTrue(BUFFER_SERIALIZER.readByte() == 24);
        Assert.assertTrue(BUFFER_SERIALIZER.getOffset() == UnsafeBufferSerializer.BYTE_SIZE * 2);
    }

    @Test
    public void writeBytes() throws Exception
    {
        final ByteBuffer unsafeInternalBuffer = BUFFER_SERIALIZER.getInternalBuffer().byteBuffer();

        final ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.putInt(786777);

        BUFFER_SERIALIZER.writeBytes(buffer, 0, 4);

        Assert.assertEquals(unsafeInternalBuffer.getInt(), 786777);
    }

    @Test
    public void writeReadBytesArray() throws Exception
    {
        // Write the bytes first
        final ByteBuffer unsafeInternalBuffer = BUFFER_SERIALIZER.getInternalBuffer().byteBuffer();

        final ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.putInt(786777);

        BUFFER_SERIALIZER.writeBytes(buffer.array(), 0, 4);

        Assert.assertEquals(unsafeInternalBuffer.getInt(), 786777);

        // Now read them
        byte[] readedBytes = new byte [4];
        BUFFER_SERIALIZER.readBytes(0, readedBytes);

        Assert.assertEquals(ByteBuffer.wrap(readedBytes).getInt(), 786777);

        // Reset the offset
        BUFFER_SERIALIZER.setOffset(0);
        BUFFER_SERIALIZER.readBytes(readedBytes);
        Assert.assertEquals(ByteBuffer.wrap(readedBytes).getInt(), 786777);
    }

    @Test
    public void writeBytesDirectBuffer() throws Exception
    {
        final ByteBuffer unsafeInternalBuffer = BUFFER_SERIALIZER.getInternalBuffer().byteBuffer();

        final ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.putInt(786777);

        BUFFER_SERIALIZER.writeBytes(new UnsafeBuffer(buffer), 0, 4);

        Assert.assertEquals(unsafeInternalBuffer.getInt(), 786777);
    }

    @Test
    public void boolSerialization() throws Exception
    {
        BUFFER_SERIALIZER.writeBool(true);
        BUFFER_SERIALIZER.writeBool(false);

        REUSABLE_SEND_BUFFER.flip();
        BUFFER_SERIALIZER.wrap(REUSABLE_SEND_BUFFER);

        Assert.assertTrue(BUFFER_SERIALIZER.readBool());
        Assert.assertTrue(!BUFFER_SERIALIZER.readBool());
        Assert.assertTrue(BUFFER_SERIALIZER.getOffset() == UnsafeBufferSerializer.BOOL_SIZE * 2);
    }

    @Test
    public void intSerialization() throws Exception
    {
        BUFFER_SERIALIZER.writeInt(67892);
        BUFFER_SERIALIZER.writeInt(99999);

        REUSABLE_SEND_BUFFER.flip();
        BUFFER_SERIALIZER.wrap(REUSABLE_SEND_BUFFER);

        Assert.assertTrue(BUFFER_SERIALIZER.readInt() == 67892);
        Assert.assertTrue(BUFFER_SERIALIZER.readInt() == 99999);
        Assert.assertTrue(BUFFER_SERIALIZER.getOffset() == UnsafeBufferSerializer.INT_SIZE * 2);
    }

    @Test
    public void int1Serialization() throws Exception
    {
        BUFFER_SERIALIZER.writeInt(67892,0);
        BUFFER_SERIALIZER.writeInt(99999, UnsafeBufferSerializer.INT_SIZE);

        REUSABLE_SEND_BUFFER.flip();
        BUFFER_SERIALIZER.wrap(REUSABLE_SEND_BUFFER);

        Assert.assertTrue(BUFFER_SERIALIZER.readInt() == 67892);
        Assert.assertTrue(BUFFER_SERIALIZER.readInt() == 99999);
        Assert.assertTrue(BUFFER_SERIALIZER.getOffset() == UnsafeBufferSerializer.INT_SIZE * 2);
    }

    @Test
    public void longSerialization() throws Exception
    {
        BUFFER_SERIALIZER.writeLong(67892);
        BUFFER_SERIALIZER.writeLong(99999);

        REUSABLE_SEND_BUFFER.flip();
        BUFFER_SERIALIZER.wrap(REUSABLE_SEND_BUFFER);

        Assert.assertTrue(BUFFER_SERIALIZER.readLong() == 67892);
        Assert.assertTrue(BUFFER_SERIALIZER.readLong() == 99999);
        Assert.assertTrue(BUFFER_SERIALIZER.getOffset() == UnsafeBufferSerializer.LONG_SIZE * 2);
    }

    @Test
    public void uuidSerialization() throws Exception
    {
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();

        BUFFER_SERIALIZER.writeUUID(uuid1);
        BUFFER_SERIALIZER.writeUUID(uuid2);

        REUSABLE_SEND_BUFFER.flip();
        BUFFER_SERIALIZER.wrap(REUSABLE_SEND_BUFFER);

        Assert.assertEquals(uuid1, BUFFER_SERIALIZER.readUUID());
        Assert.assertEquals(uuid2, BUFFER_SERIALIZER.readUUID());
        Assert.assertTrue(BUFFER_SERIALIZER.getOffset() == UnsafeBufferSerializer.UUID_SIZE * 2);
    }

    @Test
    public void stringSerialization() throws Exception
    {
        // Perform several tests
        for(int i = 0; i < 100; i++)
        {
            final String string1 = RandomStringUtils.random(10);
            final String string2 = RandomStringUtils.random(10);

            REUSABLE_SEND_BUFFER.flip();
            BUFFER_SERIALIZER.wrap(REUSABLE_SEND_BUFFER);

            BUFFER_SERIALIZER.writeString(string1);
            BUFFER_SERIALIZER.writeString(string2);

            REUSABLE_SEND_BUFFER.flip();
            BUFFER_SERIALIZER.wrap(REUSABLE_SEND_BUFFER);

            Assert.assertEquals(BUFFER_SERIALIZER.readString(), string1);
            final int firstOffset = BUFFER_SERIALIZER.getOffset();
            Assert.assertEquals(firstOffset, BUFFER_SERIALIZER.serializedSize(string1));
            Assert.assertEquals(BUFFER_SERIALIZER.readString(), string2);
            Assert.assertEquals(BUFFER_SERIALIZER.getOffset(), firstOffset + BUFFER_SERIALIZER.serializedSize(string2));
        }
    }

    @Test
    public void serializedSize() throws Exception
    {
        Assert.assertTrue(BUFFER_SERIALIZER.serializedSize("Test") == "Test".length() + UnsafeBufferSerializer.INT_SIZE);
    }

    @Test
    public void getInternalBuffer() throws Exception
    {
        Assert.assertTrue(BUFFER_SERIALIZER.getInternalBuffer() != null);
    }

    @Test
    public void getOffset() throws Exception
    {
        Assert.assertTrue(BUFFER_SERIALIZER.getOffset() >= 0);
    }

    @Test
    public void getMsgLength() throws Exception
    {
        Assert.assertTrue(BUFFER_SERIALIZER.getMsgLength() >= 0);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testBufferOverflowWritting()
    {
        while(true)
        {
            BUFFER_SERIALIZER.writeString("lkajsfklasf");
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testBufferOverflowReading()
    {
        while(true)
        {
            BUFFER_SERIALIZER.readInt();
        }
    }
}