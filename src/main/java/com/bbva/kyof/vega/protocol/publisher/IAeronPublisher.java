package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.msg.IRequestResponder;
import com.bbva.kyof.vega.msg.PublishResult;
import org.agrona.DirectBuffer;

import java.util.UUID;

/**
 * Interface to implement by Aeron Publishers in order to send messages
 */
interface IAeronPublisher extends IRequestResponder
{
    /**
     * Send a message for the given topic with the provided contents
     *
     * @param msgType message type to send
     * @param topicId unique Id of the topic being publisherd
     * @param message the message to sendMsg
     * @param offset offset where the message starts in the buffer
     * @param length length of the message to send starting from the offset
     * @return Enum with the possible results after a publication
     */
    PublishResult sendMessage(byte msgType, UUID topicId, DirectBuffer message, int offset, int length);

    /**
     * Send a message for the given topic with the provided contents
     *
     * @param msgType message type to send
     * @param topicId unique Id of the topic being publisherd
     * @param requestId unique id for the request
     * @param message the message to sendMsg
     * @param offset offset where the message starts in the buffer
     * @param length length of the message to send starting from the offset
     * @return Enum with the possible results after a publication
     */
    PublishResult sendRequest(byte msgType, UUID topicId, UUID requestId, DirectBuffer message, int offset, int length);
}
