package com.bbva.kyof.vega.serialization;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Helper class to perform serializations / deserializations using an internal unsafe buffer for maximum performance.<p>
 *
 * If an unsafe buffer is provided it will just wrap it, if any other kind of buffer is provided it will create an internal
 * unsafe buffer that wraps it.<p>
 *
 * The class can be reused to prevent unnecesary memory allocation by using wrap every time there is a new buffer or every time we want to reset the internal counters.<p>
 *
 * It keeps track of the current offset in the internal wrapped buffer and the length that has been written or read.<p>
 *
 * This class is not thread safe
 */
@NoArgsConstructor
public class UnsafeBufferSerializer
{
    /** Byte value serialized size */
    public static final int BYTE_SIZE = 1;
    /** Bool value serialized size */
    public static final int BOOL_SIZE = 1;
    /** Integer value serialized size */
    public static final int INT_SIZE = 4;
    /** Long value serialized size */
    public static final int LONG_SIZE = 8;
    /** UUID value serialized size */
    public static final int UUID_SIZE = 16;

    /** Internal buffer that is being wrapped, it will be reused to avoid memory allocation */
    @Getter private final UnsafeBuffer internalBuffer = new UnsafeBuffer(ByteBuffer.allocate(0));
    /** Current offset of with the position in the internal buffer */
    @Getter @Setter private int offset = 0;
    /** Length of the message in the buffer, only useful for reading binary information */
    @Getter @Setter private int msgLength = 0;

    /**
     * Wrap the given direct buffer entirely
     *
     * @param buffer the buffer to wrap
     */
    public void wrap(final DirectBuffer buffer)
    {
        this.internalBuffer.wrap(buffer);
        this.offset = 0;
        this.msgLength = buffer.capacity();
    }

    /**
     * Wrap the given direct buffer given the offset and length inside the direct buffer
     * @param buffer the buffer to wrap
     * @param offset the offset in the buffer
     * @param length the length of the buffer contents from the offset
     */
    public void wrap(final DirectBuffer buffer, final int offset, final int length)
    {
        this.internalBuffer.wrap(buffer, offset, length);
        this.offset = 0;
        this.msgLength = length;
    }

    /**
     * Wrap the given ByteBuffer entirely
     *
     * @param buffer the buffer to wrap.
     */
    public void wrap(final ByteBuffer buffer)
    {
        this.internalBuffer.wrap(buffer);
        this.offset = 0;
        this.msgLength = buffer.capacity();
    }

    /**
     * Wrap the given ByteBuffer given the offset and length inside the direct buffer
     *
     * @param buffer the buffer to wrap
     * @param offset the offset in the buffer
     * @param length the length of the buffer contents from the offset
     */
    public void wrap(final ByteBuffer buffer, final int offset, final int length)
    {
        this.internalBuffer.wrap(buffer);
        this.offset = offset;
        this.msgLength = length;
    }

    /**
     * Wrtite a byte into the buffer and advance the offset
     *
     * @param value the byte to write
     */
    public void writeByte(final byte value)
    {
        this.internalBuffer.putByte(offset++, value);
    }

    /**
     * Write the whole byte array content from given offset to given position into the internal buffer and advance the offset
     *
     * @param bytes the bytes to write
     * @param bytesOffset the offset in the bytes array
     * @param bytesLength the number of bytes in the byte array to write
     */
    public void writeBytes(final byte[] bytes, final int bytesOffset, final int bytesLength)
    {
        this.internalBuffer.putBytes(offset, bytes, bytesOffset, bytesLength);
        offset += bytesLength;
    }

    /**
     * Write the whole byte buffer content from given offset to given position into the internal buffer and advance the offset
     *
     * @param byteBuffer the buffer to read contents from in order to serialize them in the internal buffer
     * @param bufferOffset the offset in the given buffer
     * @param bufferLength the length of the contents from position in the given buffer
     */
    public void writeBytes(final ByteBuffer byteBuffer, final int bufferOffset, final int bufferLength)
    {
        this.internalBuffer.putBytes(offset, byteBuffer, bufferOffset, bufferLength);
        offset += bufferLength;
    }

    /**
     * Write the whole byte buffer content from given offset to given position into the internal buffer and advance the offset
     *
     * @param buffer the buffer to read contents from in order to serialize them in the internal buffer
     * @param bufferOffset the offset in the given buffer
     * @param bufferLength the length of the contents from position in the given buffer
     */
    public void writeBytes(final DirectBuffer buffer, final int bufferOffset, final int bufferLength)
    {
        this.internalBuffer.putBytes(offset, buffer, bufferOffset, bufferLength);
        offset += bufferLength;
    }

    /**
     * Write a boolean value in the internal buffer and advance the offset
     * @param value the value to write
     */
    public void writeBool(final boolean value)
    {
        if (value)
        {
            internalBuffer.putByte(offset++, (byte) 1);
        }
        else
        {
            internalBuffer.putByte(offset++, (byte) 0);
        }
    }

    /**
     * Write an integer in the internal buffer and advance the offset
     *
     * @param value the interger value to write
     */
    public void writeInt(final int value)
    {
        this.internalBuffer.putInt(offset, value);
        this.offset += INT_SIZE;
    }

    /**
     * Write an integer in the given offset without increasing internal offset
     *
     * @param value the value to write
     * @param offset the position to write it into
     */
    public void writeInt(final int value, final int offset)
    {
        this.internalBuffer.putInt(offset, value);
    }

    /**
     * Write a long value in the internal buffer
     *
     * @param value the value to write
     */
    public void writeLong(final long value)
    {
        this.internalBuffer.putLong(offset, value);
        this.offset += LONG_SIZE;
    }

    /**
     * Write an UUID value in the given buffer and advance the offset
     *
     * @param uniqueId the UUID to write
     */
    public void writeUUID(final UUID uniqueId)
    {
        this.writeLong(uniqueId.getMostSignificantBits());
        this.writeLong(uniqueId.getLeastSignificantBits());
    }

    /**
     * Read a byte value from the internal buffer and advance the offset
     * @return the read byte
     */
    public byte readByte()
    {
        return internalBuffer.getByte(offset++);
    }

    /**
     * Read a byte array
     *
     * @param dest the destination array
     */
    public void readBytes(final byte[] dest)
    {
        this.internalBuffer.getBytes(offset, dest);
        this.offset += dest.length;
    }

    /**
     * Read a byte array starting from the given offset on the contents, won't modify the internal offset
     *
     * @param offset offset in the internal unsafe buffer to start reading from
     * @param dest the destination array
     */
    public void readBytes(final int offset, final byte[] dest)
    {
        this.internalBuffer.getBytes(offset, dest);
    }

    /**
     * Read a boolean value from the internal buffer and advance the offser
     * @return the readed boolean value
     */
    public boolean readBool()
    {
        return internalBuffer.getByte(offset++) == 1;
    }

    /**
     * Read a int value from the internal buffer and advance the offset
     * @return the read int value
     */
    public int readInt()
    {
        final int result = this.internalBuffer.getInt(offset);
        this.offset += INT_SIZE;
        return result;
    }

    /**
     * Read a long value from the internal buffer and advance the offset
     * @return the read long value
     */
    public long readLong()
    {
        final long result = this.internalBuffer.getLong(offset);
        this.offset += LONG_SIZE;
        return result;
    }

    /**
     * Read a UUID value from the internal buffer and advance the offset
     * @return the read UUID value
     */
    public UUID readUUID()
    {
        return new UUID(this.readLong(), this.readLong());
    }

    /**
     * Read a String value from the internal buffer and advance the offset
     * @return the read String value
     */
    public String readString()
    {
        final int stringSize = this.readInt();

        final char[] result = new char[stringSize];

        final int lastToRead = this.offset + stringSize;
        int current = 0;
        while (this.offset < lastToRead)
        {
            final int byte1 = this.readByte();
            if (byte1 >= 0)
            {
                result[current++] = (char) byte1;
            }
            else if ((byte1 >> 5) == -2)
            {
                final int byte2 = this.readByte();
                result[current++] = (char) (((byte1 << 6) ^ byte2) ^ 0x0f80);
            }
            else if ((byte1 >> 4) == -2)
            {
                // 3 bytes, 16 bits: 1110xxxx 10xxxxxx 10xxxxxx
                final int byte2 = this.readByte();
                final int byte3 = this.readByte();

                result[current++] = (char) (((byte1 << 12) ^ (byte2 << 6) ^ byte3) ^ 0x1f80);
            }
            else if ((byte1 >> 3) == -2)
            {
                final int byte2 = this.readByte();
                final int byte3 = this.readByte();
                final int byte4 = this.readByte();
                final int uChar = ((byte1 & 0x07) << 18) | ((byte2 & 0x3f) << 12) | ((byte3 & 0x3f) << 06)
                        | (byte4 & 0x3f);

                final int high = StringSurrogate.high(uChar);
                final int low = StringSurrogate.low(uChar);
                result[current++] = (char) high;
                result[current++] = (char) low;
            }
        }

        return new String(result, 0, current);
    }

    /**
     * Serialize a String value into the internal buffer and advance the offset
     *
     * @param value String to serialize
     */
    public void writeString(final String value)
    {
        // Store original offset
        final int originalOffset = this.offset;

        // Leave some space for the String size
        this.offset += INT_SIZE;

        for (int i = 0; i < value.length(); i++)
        {
            final int currentChar = value.charAt(i);
            if (currentChar < 0x80)
            {
                this.writeByte((byte) currentChar);
            }
            else if (currentChar < 0x800)
            {
                this.writeByte((byte) (0xc0 | (currentChar >> 06)));
                this.writeByte((byte) (0x80 | (currentChar & 0x3f)));
            }
            else if (StringSurrogate.isSurrogate(currentChar))
            {
                // Have a surrogate pair
                final int uChar = StringSurrogate.parse((char) currentChar, value.charAt(++i));

                this.writeByte((byte) (0xf0 | (uChar >> 18)));
                this.writeByte((byte) (0x80 | (uChar >> 12) & 0x3f));
                this.writeByte((byte) (0x80 | (uChar >> 06) & 0x3f));
                this.writeByte((byte) (0x80 | (uChar & 0x3f)));
            }
            else
            {
                this.writeByte((byte) (0xe0 | (currentChar >> 12)));
                this.writeByte((byte) (0x80 | (currentChar >> 06) & 0x3f));
                this.writeByte((byte) (0x80 | (currentChar & 0x3f)));
            }
        }

        // Write the String size at the beginning
        final int stringSize = this.offset - originalOffset - INT_SIZE;
        this.writeInt(stringSize, originalOffset);
    }

    /**
     * Returns serialized size of of the string, it contains also the integer with the string length
     *
     * @param string the String to calculate the size
     * @return serialized String size
     */
    public static int serializedSize(final String string)
    {
        return stringSerializedSize(string) + INT_SIZE;
    }

    /**
     * Returns serialized size of value parameter
     *
     * @param value the String to calculate the size
     * @return serialized String size
     */
    private static int stringSerializedSize(final String value)
    {
        int result = 0;
        for (int i = 0; i < value.length(); i++)
        {
            final int currentChar = value.charAt(i);
            if (currentChar < 0x80)
            {
                result++;
            }
            else if (currentChar < 0x800)
            {
                result += 2;
            }
            else if (StringSurrogate.isSurrogate(currentChar))
            {
                i++;
                result += 4;
            }
            else
            {
                result += 3;
            }
        }

        return result;
    }
}
