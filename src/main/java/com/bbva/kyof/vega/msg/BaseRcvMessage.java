package com.bbva.kyof.vega.msg;


import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Represent the base class for any data received message. Received message, request and response inherits from this one. <p>
 * The message contents are always represented as an UnsafeBuffer, however they may be converted to a byteBuffer if required.
 *
 * This class is not thread safe!
 */
@NoArgsConstructor
class BaseRcvMessage
{
    /** Unique Id of the library instance that sent the message */
    @Getter @Setter private UUID instanceId;

    /** Contents of the message represented as an unsafe buffer */
    @Setter private UnsafeBuffer unsafeBufferContent;

    /** Topic name the message belongs to */
    @Getter @Setter private String topicName;

    /** Offset in the buffer message contents where the user msg starts */
    @Getter @Setter private int contentOffset;

    /** Length of the user message in the buffer contents */
    @Getter @Setter private int contentLength;

    /**
     * Promote the message by cloning all the message contents into the object.
     *
     * It will also clone the internal buffer.
     *
     * @param promotedMsg the message that has been promoted to clone the contents into
     */
    void promote(final BaseRcvMessage promotedMsg)
    {
        // Copy the contents into a new byte buffer, just the user contents, ignore the rest of headers
        final ByteBuffer newBuffer = this.getContentAsNewByteBuffer();

        // Fill promoted message fields
        promotedMsg.instanceId = this.instanceId;
        promotedMsg.topicName = this.topicName;
        promotedMsg.unsafeBufferContent = new UnsafeBuffer(newBuffer);
        promotedMsg.contentLength = newBuffer.capacity();
        promotedMsg.contentOffset = 0;
    }

    /**
     * Return an UnsafeBuffer with the user contents of the message. This is the most optimal way to deal with the message contents.
     *
     * The offset in the buffer and total user msg length can be retrieved using the respective methods, getContentOffset() and getContentLenght()
     *
     * @return the UnsafeBuffer wtih the message contents
     */
    public UnsafeBuffer getContents()
    {
        return this.unsafeBufferContent;
    }

    /**
     * Return an ByteBuffer with the user contents of the message.<p>
     *
     * A new ByteBuffer is created and the message contents copied to it. This is not as optimal as accessing the contents directly!<p>
     * If you need the contents as ByteBuffer try to reuse the buffer by calling "copyContentsIntoByteBuffer" instead.
     *
     * The new buffer will have position 0 and limit at the end of the message.
     *
     * @return the ByteBuffer with the copy of the message contents
     */
    private ByteBuffer getContentAsNewByteBuffer()
    {
        final ByteBuffer result = ByteBuffer.allocate(this.contentLength);
        this.unsafeBufferContent.getBytes(this.contentOffset, result, 0, this.contentLength);

        return result;
    }
}
