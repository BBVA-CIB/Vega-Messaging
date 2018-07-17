package com.bbva.kyof.vega.protocol.control;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.msg.*;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.collection.HashMapOfHashSet;
import com.bbva.kyof.vega.util.crypto.AESCrypto;
import com.bbva.kyof.vega.util.crypto.RSACrypto;
import com.bbva.kyof.vega.util.threads.BlockCancelTask;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * This class stores all the topic publisher information security retrieved from other instances.
 *
 * Also handles the sending of requests to obtain the security information when a new publisher that has security is discovered
 * with the auto-discovery mechanism.
 */
@Slf4j
class SecurityRequester implements
        ISecurityResponseListener,
        ISecurityRequesterNotifier,
        ISecuredMsgsDecoder,
        Closeable
{
    /** Random number generator to generate request id's */
    private final Random rnd = new Random(System.nanoTime());

    /** Map that contains all the publisher topic security info's stored by topic subscriber id */
    private final HashMapOfHashSet<UUID, TopicPubSecurityInfo> securityInfosByTopicSubId = new HashMapOfHashSet<>();

    /** Map that stores all the publisher topic security info's stored by topic publisher id */
    private final Map<UUID, TopicPubSecurityInfo> securityInfoByTopicPubId = new HashMap<>();

    /** Timer used to schedule the sending of new security requests */
    private final Timer securityRequestsTimer;

    /** Stores all the control publishers to send requests to other instances */
    private final ControlPublishers controlPublishers;

    /** Reusable buffer serializer to minimize memory creation during responses */
    private final UnsafeBufferSerializer requestBufferSerializer = new UnsafeBufferSerializer();

    /** Reusable security request message to save memory space */
    private final MsgSecurityReq reusableSecurityReqMsg = new MsgSecurityReq();

    /** Our own instance security id */
    private final int ownSecurityId;

    /** RSA crypto instance to sing, code, decode and verify messages */
    private final RSACrypto rsaCrypto;

    /** Store our own instance id */
    private final UUID ownInstanceId;

    /** Lock for instance synchronization */
    private final Object lock = new Object();

    /**
     * Create a new security requester instance
     *
     * @param vegaContext the vega instance context
     * @param controlPublishers the list of control publishers
     */
    SecurityRequester(final VegaContext vegaContext, final ControlPublishers controlPublishers)
    {
        // Create the timer
        this.securityRequestsTimer = new Timer("SecurityRequestTimer_" + vegaContext.getInstanceUniqueId());

        this.controlPublishers = controlPublishers;
        this.ownInstanceId = vegaContext.getInstanceUniqueId();
        this.ownSecurityId = vegaContext.getSecurityContext().getSecurityId();
        this.rsaCrypto = vegaContext.getSecurityContext().getRsaCrypto();

        // Set our security id in the reusable message since it will never change
        this.reusableSecurityReqMsg.setSenderSecurityId(this.ownSecurityId);

        // Prepare the serialize buffer
        this.requestBufferSerializer.wrap(ByteBuffer.allocate(1024));
    }

    @Override
    public void removedSecureSubTopic(final UUID subTopicId)
    {
        synchronized (this.lock)
        {
            // Remove remove the security info
            this.securityInfosByTopicSubId.removeAndConsumeIfKeyEquals(subTopicId, pendingSecurityInfo ->
            {
                this.securityInfoByTopicPubId.remove(pendingSecurityInfo.getPublisherTopicId());

                // Cancel the task, it will be ignored if already cancelled
                this.cancelRequestSecurityInfoTask(pendingSecurityInfo);
            });
        }
    }

    @Override
    public void addedPubForSubTopic(final AutoDiscTopicInfo pubTopicInfo, final UUID subTopicId, final TopicSecurityTemplateConfig subSecurityConfig)
    {
        synchronized (this.lock)
        {
            // If we already have it, ignore it, it may be a duplicated event
            if (this.securityInfoByTopicPubId.containsKey(pubTopicInfo.getUniqueId()))
            {
                return;
            }

            // Create the pending security info
            final TopicPubSecurityInfo securityInfo = new TopicPubSecurityInfo(
                    pubTopicInfo.getTopicName(),
                    pubTopicInfo.getInstanceId(),
                    pubTopicInfo.getSecurityId(),
                    pubTopicInfo.getUniqueId());

            // Store the added security info by topic pub id
            this.securityInfoByTopicPubId.put(pubTopicInfo.getUniqueId(), securityInfo);

            // Store the relationship between the topic subscriber and topic publisher security info
            this.securityInfosByTopicSubId.put(subTopicId, securityInfo);

            // Schedule the task to send requests and obtain the security information
            this.securityRequestsTimer.scheduleAtFixedRate(securityInfo, 0, subSecurityConfig.getControlMsgInterval());
        }
    }

    @Override
    public void removedPubForSubTopic(final AutoDiscTopicInfo pubTopicInfo, final UUID subTopicId)
    {
        synchronized (this.lock)
        {
            // Remove the security info
            final TopicPubSecurityInfo securityInfo = this.securityInfoByTopicPubId.remove(pubTopicInfo.getUniqueId());

            // If not there ignore, it may be a duplicated event
            if (securityInfo == null)
            {
                return;
            }

            // Cancel the task, it will ignore it if already canceled
            this.cancelRequestSecurityInfoTask(securityInfo);

            // Remove also from the topic subscriber
            this.securityInfosByTopicSubId.remove(subTopicId, securityInfo);
        }
    }

    /**
     * Cancel the task to try to get the security info of the topic publisher
     * @param securityInfo the security info class with the topic information to retrieve
     */
    private void cancelRequestSecurityInfoTask(final TopicPubSecurityInfo securityInfo)
    {
        securityInfo.cancel();
        this.securityRequestsTimer.purge();
    }

    @Override
    public AESCrypto getAesCryptoForSecPub(final UUID secureTopicPubId)
    {
        final TopicPubSecurityInfo secureInfo = this.securityInfoByTopicPubId.get(secureTopicPubId);

        if(secureInfo != null)
        {
            return secureInfo.getSessionKeyDecoder();
        }

        return null;
    }

    @Override
    public void onSecuirtyResponseReceived(final MsgSecurityResp securityResponse)
    {
        synchronized (this.lock)
        {
            // Find the security info
            final TopicPubSecurityInfo securityInfo = this.securityInfoByTopicPubId.get(securityResponse.getTopicPublisherId());

            // Verify the response
            if (!verifyResponse(securityResponse, securityInfo))
            {
                return;
            }

            // Everything is verified, we can create the AESCrypto for the shared session key and cancel the task

            // First cancel the task to prevent further requests
            this.cancelRequestSecurityInfoTask(securityInfo);

            // Decode the session key
            final byte[] decodedKey;
            try
            {
                decodedKey = this.rsaCrypto.decodeWithOwnPrivKey(securityResponse.getEncodedSessionKey());
            }
            catch (final VegaException e)
            {
                log.error("Error decoding received session key with own private key. " + securityResponse, e);
                return;
            }

            // Create and set the AESCrypto
            final AESCrypto aesCrypto;
            try
            {
                aesCrypto = new AESCrypto(decodedKey);
            }
            catch (final VegaException e)
            {
                log.error("Error creating session AES decoder for security response. " + securityResponse, e);
                return;
            }

            // Set the key in the security info
            securityInfo.setSessionKeyDecoder(aesCrypto);
        }
    }

    @Override
    public void onSecurityErrorResponseReceived(final MsgSecurityErrorResp errorResponse)
    {
        synchronized (this.lock)
        {
            // Find the security info
            final TopicPubSecurityInfo securityInfo = this.securityInfoByTopicPubId.get(errorResponse.getTopicPublisherId());

            // Verify the response
            if (!verifyResponse(errorResponse, securityInfo))
            {
                return;
            }

            // First cancel the task to prevent further requests
            this.cancelRequestSecurityInfoTask(securityInfo);

            // Log the error response code
            switch (errorResponse.getErrorCode())
            {
                case MsgSecurityErrorResp.NO_SECURE_PUB_FOUND:
                    log.error("Cannot retrieve security credentials for topic [{}] with id [{}], the publisher application cannot find the topic", securityInfo.getTopicName(), securityInfo.getPublisherTopicId());
                    break;
                case MsgSecurityErrorResp.NOT_ALLOWED_BY_CONFIG:
                    log.error("Cannot retrieve security credentials for topic [{}] with id [{}], the publisher application don't have our secure id in the list of valid id's for the topic", securityInfo.getTopicName(), securityInfo.getPublisherTopicId());
                    break;
                case MsgSecurityErrorResp.PUB_KEY_NOT_FOUND:
                    log.error("Cannot retrieve security credentials for topic [{}] with id [{}], the publisher application don't have our security id public key", securityInfo.getTopicName(), securityInfo.getPublisherTopicId());
                    break;
                case MsgSecurityErrorResp.SIGNATURE_ERROR:
                    log.error("Cannot retrieve security credentials for topic [{}] with id [{}], the publisher application failed to verify our signature", securityInfo.getTopicName(), securityInfo.getPublisherTopicId());
                    break;
                default:
                    log.error("Unexpected error code on security error response received [{}]", errorResponse);
                    break;
            }
        }
    }

    /**
     * Verify a security response. It will check that the security info for the response exists, and that the requests id's and security id matches.
     *
     * It will also verify the signature to be 100% sure the message has not been modified.
     *
     * @param securityResponse the security response
     * @param securityInfo the original security information for the response
     * @return true if the response is valid, false in other case
     */
    private boolean verifyResponse(final AbstractMsgSecurity securityResponse, final TopicPubSecurityInfo securityInfo)
    {
        // If not found or already canceled, ignore it
        if (securityInfo == null || securityInfo.isCanceled())
        {
            log.info("Security response received but security information is not required anymore. {}", securityResponse);
            return false;
        }

        // Check if the request id match with the last one sent
        if (!securityResponse.getRequestId().equals(securityInfo.getLastRequestIdSent()))
        {
            log.info("Security response received but don't correspond to the last security request id sent. {}", securityResponse);
            return false;
        }

        // Check the security id as well
        if (securityResponse.getSenderSecurityId() != securityInfo.getPublisherSecureId())
        {
            log.info("Security response received but the security ids don't correspond. {}", securityResponse);
            return false;
        }

        // Verify the signature!
        return this.validateResponseSignature(securityResponse);
    }

    /**
     * Validate the signature of the security response.
     *
     * @param securityResp the security response whose signature should be validated
     * @return true if valid, false in other case
     */
    private boolean validateResponseSignature(final AbstractMsgSecurity securityResp)
    {
        try
        {
            if (!securityResp.verifySignature(this.rsaCrypto))
            {
                log.warn("Invalid signature on received security response. [{}]", securityResp);
                return false;
            }
        }
        catch (final VegaException e)
        {
            log.warn("Unexpected error validating signature on received security response. " + securityResp, e);
            return false;
        }

        return true;
    }

    @Override
    public void close()
    {
        synchronized (this.lock)
        {
            // Cancel the timer
            this.securityRequestsTimer.cancel();
            this.securityRequestsTimer.purge();

            // Clean all internal info
            this.securityInfosByTopicSubId.clear();
            this.securityInfoByTopicPubId.clear();
        }
    }

    /**
     * Stores the topic publisher security info and is also used as "task" to try to retrive the information from the external instance
     */
    private class TopicPubSecurityInfo extends BlockCancelTask
    {
        /** Name of the topic represented by the topic publisher */
        @Getter private final String topicName;
        /** The unique instance id the topic publisher belongs to */
        private final UUID publisherInstanceId;
        /** The secure id of the topic publisher */
        @Getter private final int publisherSecureId;
        /** The unique topic id of the topic publisher */
        @Getter private final UUID publisherTopicId;
        /** Stores the id of the last security sent request */
        @Getter private volatile UUID lastRequestIdSent = null;
        /** Stores the session key decoder retrived for the topic publisher, null if don't exists or it couldn't be retrieved */
        @Getter @Setter private volatile AESCrypto sessionKeyDecoder = null;

        /**
         * Create a topip publisher security information
         *
         * @param topicName the name of the topic
         * @param publisherInstanceId the unique instance id of the publisher the topic publisher belongs to
         * @param publisherSecureId the security id of the publisher the topic publisher belongs to
         * @param publisherTopicId the unique topic publisher ID
         */
        TopicPubSecurityInfo(final String topicName, final UUID publisherInstanceId, final int publisherSecureId, final UUID publisherTopicId)
        {
            super();

            this.topicName = topicName;
            this.publisherInstanceId = publisherInstanceId;
            this.publisherSecureId = publisherSecureId;
            this.publisherTopicId = publisherTopicId;
        }

        @Override
        public void action()
        {
            if (log.isTraceEnabled())
            {
                log.trace("Sending security request for pending security information {}", this);
            }

            final ControlPublisher publisher = controlPublishers.getControlPublisherForInstance(this.publisherInstanceId);

            // There is no publisher, maybe the instance information has not arrived yet.
            if (publisher == null)
            {
                log.warn("Trying to send a security request message, but the control publisher is not ready yet for the instance. It will be retried. [{}]", this);
                return;
            }

            /// Reset the unsafe request serializer offset
            requestBufferSerializer.setOffset(0);

            // Prepare the message fields
            this.lastRequestIdSent = new UUID(rnd.nextLong(), rnd.nextLong());
            reusableSecurityReqMsg.setInstanceId(ownInstanceId);
            reusableSecurityReqMsg.setRequestId(this.lastRequestIdSent);
            reusableSecurityReqMsg.setSenderSecurityId(ownSecurityId);
            reusableSecurityReqMsg.setTargetVegaInstanceId(this.publisherInstanceId);
            reusableSecurityReqMsg.setTopicPublisherId(this.publisherTopicId);

            // Sign and serialize the message
            try
            {
                reusableSecurityReqMsg.signAndSerialize(requestBufferSerializer, rsaCrypto);
            }
            catch (final VegaException e)
            {
                log.error("Unexpected error creating security request message", e);
                return;
            }

            // Send the message
            final PublishResult sendResult = publisher.sendMessage(MsgType.CONTROL_SECURITY_REQ, requestBufferSerializer.getInternalBuffer(), 0, requestBufferSerializer.getOffset());

            if (log.isTraceEnabled())
            {
                log.trace("Security request sent with result {}", sendResult);
            }
        }

        @Override
        public boolean equals(final Object target)
        {
            if (this == target)
            {
                return true;
            }
            if (target == null || getClass() != target.getClass())
            {
                return false;
            }

            TopicPubSecurityInfo that = (TopicPubSecurityInfo) target;

            return publisherTopicId != null ? publisherTopicId.equals(that.publisherTopicId) : that.publisherTopicId == null;

        }

        @Override
        public int hashCode()
        {
            return publisherTopicId != null ? publisherTopicId.hashCode() : 0;
        }
    }
}
