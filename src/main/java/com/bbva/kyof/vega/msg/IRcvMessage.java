package com.bbva.kyof.vega.msg;

import org.agrona.concurrent.UnsafeBuffer;

import java.util.UUID;

/**
 * Interface that represents received message.<p>
 *
 * The contents of the message comes in the form of an UnsafeBuffer <p>
 *
 * It is also possible to get the contents as a ByteBuffer <p>
 *
 * The message object and contents are reused by the library to avoid memory allocation, always promote the message
 * if the message contents are going to be accessed on separate user thread!
 */
public interface IRcvMessage
{
    /** @return the topic name of the received message */
    String getTopicName();

    /** @return  the instance id of the message sender */
    UUID getInstanceId();

    /**
     * Return the offset in the message contents where the user msg starts
     *
     * @return the content message offset
     */
    int getContentOffset();

    /**
     * Return the lenght of the user message
     *
     * @return the length of the message
     */

    int getContentLength();

    /**
     * Return an UnsafeBuffer with the user contents of the message. This is the most optimal way to deal with the message contents.
     *
     * The offset in the buffer and total user msg length can be retrieved using the respective methods, getContentOffset() and getContentLenght()
     *
     * @return the UnsafeBuffer with the message contents
     */
    UnsafeBuffer getContents();

    /**
     * Promote the message by cloning all internal fields into a new message, including the content
     *
     * Since the library will try to reuse internal buffers you have to promote the message if it's contents are going
     * to be accessed from a separate thread.
     *
     * @return the new created promoted message
     */
    IRcvMessage promote();
}
