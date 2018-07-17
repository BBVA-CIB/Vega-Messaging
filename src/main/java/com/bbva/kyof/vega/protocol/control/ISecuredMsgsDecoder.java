package com.bbva.kyof.vega.protocol.control;

import com.bbva.kyof.vega.util.crypto.AESCrypto;

import java.util.UUID;

/**
 * Interface for a class that is able to decode a secured message.
 *
 * The message contents will be changed by the decoded content transparently. The new buffer used may be reused, the standard
 * rules of
 */
public interface ISecuredMsgsDecoder
{
    /**
     * Return the session AES decoder for the given topic publisher.
     *
     * @param secureTopicPublisherId the topic publisher id to get the deocoder for
     * @return the decoder, null if no decoder available
     */
    AESCrypto getAesCryptoForSecPub(final UUID secureTopicPublisherId);
}
