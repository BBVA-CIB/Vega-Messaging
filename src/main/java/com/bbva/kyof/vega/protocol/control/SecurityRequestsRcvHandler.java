package com.bbva.kyof.vega.protocol.control;

import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.msg.*;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.crypto.RSACrypto;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class that handles security requests reception and processing
 *
 * If the request is valid and the signatures are verified it will respond with the session key for the requested topic.
 * The response is also signed and the session key is encrypted to ensure that only the sender can read the response.
 *
 * If there is any problem with the request it will respond with a signed message with the found error.
 */
@Slf4j
class SecurityRequestsRcvHandler implements ISecurityRequestListener, IOwnSecPubTopicsChangesListener, Closeable
{
    /** Session keys for owned secure topic publishers by topic publisher id */
    private final Map<UUID, OwnSecureTopicPubInfo> ownSecureTopicPubInfo = new ConcurrentHashMap<>();

    /** SEcurity id of the current instance */
    private final int ownSecurityId;

    /** The RSA crypto to sign and verify */
    private final RSACrypto rsaCrypto;

    /** Reference to the control publishers, it will be used to find the right publisher to send the response to */
    private final ControlPublishers controlPublishers;

    /** Reusable buffer serializer to minimize memory creation during responses */
    private final UnsafeBufferSerializer responseBufferSerializer = new UnsafeBufferSerializer();

    /** Reusable security response message */
    private final MsgSecurityResp reusableSecurityResp = new MsgSecurityResp();

    /** Reusable security error response message */
    private final MsgSecurityErrorResp reusableSecurityErrorResp = new MsgSecurityErrorResp();

    /**
     * Create a new security request handler
     *
     * @param vegaContext the vega instance context
     * @param controlPublishers the reference to all the control publishers to send control messages to other instances
     */
    SecurityRequestsRcvHandler(final VegaContext vegaContext, final ControlPublishers controlPublishers)
    {
        this.ownSecurityId = vegaContext.getSecurityContext().getSecurityId();
        this.rsaCrypto = vegaContext.getSecurityContext().getRsaCrypto();
        this.controlPublishers = controlPublishers;

        // Set the instance id for the reusable messages since it won't change
        this.reusableSecurityErrorResp.setInstanceId(vegaContext.getInstanceUniqueId());
        this.reusableSecurityResp.setInstanceId(vegaContext.getInstanceUniqueId());

        // Prepare the send buffer serializer
        this.responseBufferSerializer.wrap(ByteBuffer.allocate(1024));
    }

    @Override
    public void onOwnSecureTopicPublisherAdded(final UUID topicPubId, final byte[] sessionKey, final TopicSecurityTemplateConfig securityConfig)
    {
        // There is a new publisher with security configured, store the publisher information
        this.ownSecureTopicPubInfo.put(topicPubId, new OwnSecureTopicPubInfo(sessionKey, securityConfig));
    }

    @Override
    public void onOwnSecuredTopicPublisherRemoved(final UUID topicPubId)
    {
        // A publisher with security configured has been removed, delete the publisher information
        this.ownSecureTopicPubInfo.remove(topicPubId);
    }

    @Override
    public void onSecurityRequestReceived(final MsgSecurityReq securityReq)
    {
        // Get the control publisher to send the response later one
        final ControlPublisher responsePublisher = this.controlPublishers.getControlPublisherForInstance(securityReq.getInstanceId());

        // If there is no control publisher we cannot send the response back
        if (responsePublisher == null)
        {
            log.warn("Received security request but cannot find a control response publisher for it. Message [{}]", securityReq);
            return;
        }

        // Look the the topic publisher id the request is asking information about
        final OwnSecureTopicPubInfo secureTopicPubInfo = this.ownSecureTopicPubInfo.get(securityReq.getTopicPublisherId());

        // If there is no secure topic publisher information for the topic respond with error
        if (secureTopicPubInfo == null)
        {
            this.sendErrorResponse(securityReq, responsePublisher, MsgSecurityErrorResp.NO_SECURE_PUB_FOUND);
        }
        else if (!secureTopicPubInfo.isAllowedToSubscribe(securityReq.getSenderSecurityId()))
        {
            // Now make sure the security id is in the list of valid id's for the topic
            this.sendErrorResponse(securityReq, responsePublisher, MsgSecurityErrorResp.NOT_ALLOWED_BY_CONFIG);
        }
        else if (!this.rsaCrypto.isSecurityIdRegistered(securityReq.getSenderSecurityId()))
        {
            // Make sure we have the public key of the sender to verify the signature
            this.sendErrorResponse(securityReq, responsePublisher, MsgSecurityErrorResp.PUB_KEY_NOT_FOUND);
        }
        else if (!this.validateRequestSignature(securityReq))
        {
            // Validate the signature of the request
            this.sendErrorResponse(securityReq, responsePublisher, MsgSecurityErrorResp.SIGNATURE_ERROR);
        }
        else
        {
            // Everything is correct! Send the response with the security information requested
            this.sendSecurityResponse(securityReq, responsePublisher, secureTopicPubInfo);
        }
    }

    /**
     * Validate the signature of the security request.
     *
     * @param securityReq the security request whose signature should be validated
     * @return true if valid, false in other case
     */
    private boolean validateRequestSignature(final MsgSecurityReq securityReq)
    {
        try
        {
            if (!securityReq.verifySignature(this.rsaCrypto))
            {
                log.warn("Invalid signature on received security request. [{}]", securityReq);
                return false;
            }
        }
        catch (final VegaException e)
        {
            log.warn("Unexpected error validating signature on received security request. " + securityReq, e);
            return false;
        }

        return true;
    }

    /**
     * Send an error response message with the given error code
     *
     * @param origSecRequest the original request message
     * @param responsePublisher the publisher that is going to be used to send the response
     * @param errorCode the error code of the response
     */
    private void sendErrorResponse(final MsgSecurityReq origSecRequest,
                                   final ControlPublisher responsePublisher,
                                   final byte errorCode)
    {
        if (log.isTraceEnabled())
        {
            log.trace("Sending security error response for original request {}, with error code {}", origSecRequest, errorCode);
        }

        // Set the error message fields
        this.reusableSecurityErrorResp.setSenderSecurityId(ownSecurityId);
        this.reusableSecurityErrorResp.setRequestId(origSecRequest.getRequestId());
        this.reusableSecurityErrorResp.setTopicPublisherId(origSecRequest.getTopicPublisherId());
        this.reusableSecurityErrorResp.setTargetVegaInstanceId(origSecRequest.getInstanceId());
        this.reusableSecurityErrorResp.setErrorCode(errorCode);

        // Reset the unsafe response serializer offset
        this.responseBufferSerializer.setOffset(0);

        try
        {
            // Serialize the message
            this.reusableSecurityErrorResp.signAndSerialize(this.responseBufferSerializer, this.rsaCrypto);
        }
        catch (final VegaException e)
        {
            log.error("Unexpected error creating security error response message", e);
            return;
        }

        // Send security the error response
        final PublishResult result = responsePublisher.sendMessage(
                MsgType.CONTROL_SECURITY_ERROR_RESP,
                this.responseBufferSerializer.getInternalBuffer(),
                0,
                this.responseBufferSerializer.getOffset());

        if (log.isTraceEnabled())
        {
            log.trace("Security error response sent with publish result {}", result);
        }
    }

    /**
     * Send a security response with the encrypted session key and the message signed
     *
     * @param origSecRequest the original request message
     * @param responsePublisher the publisher that is going to be used to send the response
     * @param secureTopicPubInfo the secure topic publisher which security information has been asked about
     */
    private void sendSecurityResponse(final MsgSecurityReq origSecRequest,
                                      final ControlPublisher responsePublisher,
                                      final OwnSecureTopicPubInfo secureTopicPubInfo)
    {
        if (log.isTraceEnabled())
        {
            log.trace("Sending security response for original request {}", origSecRequest);
        }

        // Set the message response fields
        this.reusableSecurityResp.setTargetVegaInstanceId(origSecRequest.getInstanceId());
        this.reusableSecurityResp.setRequestId(origSecRequest.getRequestId());
        this.reusableSecurityResp.setSenderSecurityId(this.ownSecurityId);
        this.reusableSecurityResp.setTopicPublisherId(origSecRequest.getTopicPublisherId());

        // Encode the session key with the requester public key and assign it
        final byte[] encodedSessionKey;
        try
        {
            encodedSessionKey = secureTopicPubInfo.getEncodedSessionKey(this.rsaCrypto, origSecRequest.getSenderSecurityId());
        }
        catch (final VegaException e)
        {
            log.error("Unexpected error encoding the session key to respond to security request message. " + origSecRequest, e);
            return;
        }

        // Set the value of the encoded session key
        this.reusableSecurityResp.setEncodedSessionKey(encodedSessionKey);

        /// Reset the unsafe response serializer offset
        this.responseBufferSerializer.setOffset(0);

        try
        {
            // Serialize the message
            this.reusableSecurityResp.signAndSerialize(this.responseBufferSerializer, this.rsaCrypto);
        }
        catch (final VegaException e)
        {
            log.error("Unexpected error singing and serializing to binary the security response", e);
            return;
        }

        // Send the security response
        final PublishResult result = responsePublisher.sendMessage(
                MsgType.CONTROL_SECURITY_RESP,
                this.responseBufferSerializer.getInternalBuffer(),
                0,
                this.responseBufferSerializer.getOffset());

        if (log.isTraceEnabled())
        {
            log.trace("Security response sent with publish result {}", result);
        }
    }

    @Override
    public void close()
    {
        this.ownSecureTopicPubInfo.clear();
    }

    /**
     * Stores the information of the secure topic publishers that have been registered
     */
    @AllArgsConstructor
    private static class OwnSecureTopicPubInfo
    {
        /** The byte array with the session key */
        private final byte[] sessionKey;

        /** The security configuration for the topic publisher */
        private final TopicSecurityTemplateConfig secureConfig;

        /**
         * Encode the session key given the RSA with the public key to use for encoding and teh security id that represents the key
         * @return the encoded key
         */
        byte[] getEncodedSessionKey(final RSACrypto rsaCrypto, final int securityId) throws VegaException
        {
            return rsaCrypto.encodeWithPubKey(securityId, this.sessionKey);
        }

        /**
         * Returns true if the given security id is allowed to subscribe to the topic.
         *
         * Internally it will check in the topic security configuration for the security id
         *
         * @param subSecurityId the subscriber security id whose permissions are going to be check
         * @return true if allowed, false in other case
         */
        boolean isAllowedToSubscribe(final int subSecurityId)
        {
            return this.secureConfig.getSubSecIds().contains(subSecurityId);
        }
    }
}
