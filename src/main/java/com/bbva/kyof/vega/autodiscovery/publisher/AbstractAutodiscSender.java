package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.msg.BaseHeader;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.serialization.IUnsafeSerializable;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import io.aeron.Aeron;
import io.aeron.Publication;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.nio.ByteBuffer;

/**
 * Abstract class that is the base of specific implementations of the publication functionality for auto-discovery.<p>
 *
 * The class handles all publication related features. The information to publish has to be previously registered, once
 * registered every time "sendNextTopicAdverts" is called it will fetch the next element to publish and send it if the last time
 * it was sent is bigger than the refresh interval. <p>
 *
 * It keep track of the last time each element was sent to prevent sending them before the refresh interval time. <p>
 *
 * Some information is also resent when a message arrives to the auto-discovery manager from the receiver to speed up
 * the discovery process. <p>
 *
 * The class is not thread-safe!
 */
@Slf4j
public abstract class AbstractAutodiscSender implements Closeable
{
    /** Send buffer size, with 1024 should be enough for autodiscovery messages */
    private final static int SEND_BUFFER_SIZE = 1024;

    /** Reusable buffer serializer used to serialize the messages into the reusable send buffer */
    private final UnsafeBufferSerializer sendBufferSerializer = new UnsafeBufferSerializer();

    /** Queue containing all the registered information regarding topic-socket pairs */
    private final RegisteredInfoQueue<AutoDiscTopicSocketInfo> registeredTopicSocketInfos;

    /** Queue containing all the registered information regarding topic publishers or subscribers */
    private final RegisteredInfoQueue<AutoDiscTopicInfo> registeredTopicInfos;

    /** Contains the information regarding the vega library instance that has to be periodically published */
    private volatile RegisteredInfo<AutoDiscInstanceInfo> registeredInstanceInfo = null;

    /** Autodiscovery configuration */
    private final AutoDiscoveryConfig config;

    /** Reusable base header for the messages*/
    private final BaseHeader reusableBaseHeader;

    /** Publication aeron socket used to send the messages */
    private final Publication publication;

    /**
     * Constructor to create a new auto-discovery abstract publisher
     *
     * @param aeron the Aeron instance object
     * @param config the autodiscovery configuration
     */
    AbstractAutodiscSender(final Aeron aeron, final AutoDiscoveryConfig config)
    {
        // Initialize the queues of registered info with the refresh interval in the configuration
        this.registeredTopicSocketInfos = new RegisteredInfoQueue<>(config.getRefreshInterval());
        this.registeredTopicInfos = new RegisteredInfoQueue<>(config.getRefreshInterval());

        this.config = config;

        // Initialize the reusable header to serialize the messages
        this.reusableBaseHeader = new BaseHeader(MsgType.AUTO_DISC_TOPIC, Version.LOCAL_VERSION);

        // Create the publication
        this.publication = this.createPublication(aeron, config);

        // Prepare the reusable buffer serializer
        this.sendBufferSerializer.wrap(ByteBuffer.allocate(SEND_BUFFER_SIZE));
    }

    /**
     * Creates the Aeron publication object to send auto-discovery messages.
     * @param aeron the Aeron instance
     * @param config the auto-discovery configuration
     *
     * @return the created Aeron publication
     */
    public abstract Publication createPublication(final Aeron aeron, final AutoDiscoveryConfig config);

    @Override
    public void close()
    {
        log.info("Closing auto discovery sender");

        this.publication.close();
        this.registeredTopicSocketInfos.clear();
        this.registeredTopicInfos.clear();
    }

    /**
     * Register the auto-discovery instance info of the current library instance to be periodically advert.
     *
     * For this type of info there only one instance, but we have keep the methods pattern to be homogeneous
     *
     * @param instanceInfo the instance information
     */
    public void registerInstance(final AutoDiscInstanceInfo instanceInfo)
    {
        log.debug("Registering autodisc instance info [{}]", instanceInfo);

        // Broadcast the new information immediately
        this.sendMessageIfNotNull(MsgType.AUTO_DISC_INSTANCE, instanceInfo);

        // Register the instance info
        this.registeredInstanceInfo = new RegisteredInfo<>(instanceInfo, this.config.getRefreshInterval());
    }

    /**
     * Unregister the auto-discovery instance info of the current library instance to close sending periodical adverts
     */
    public void unRegisterInstance()
    {
        log.debug("Unregistering autodisc instance info ");

        // Remove it
        this.registeredInstanceInfo = null;
    }

    /**
     * Register an auto-discovery topic info with the information of a topic publisher or subscriber.
     *
     * @param topicInfo the topic information
     */
    public void registerTopic(final AutoDiscTopicInfo topicInfo)
    {
        log.debug("Registering autodisc topic info [{}]", topicInfo);

        // Broadcast the new information immediately
        this.sendMessageIfNotNull(MsgType.AUTO_DISC_TOPIC, topicInfo);

        // Register the topic info, it wont add it if already exists
        this.registeredTopicInfos.add(topicInfo);
    }

    /**
     * Unregister an auto-discovery topic info with the information of a topic publisher or subscriber.
     *
     * @param topicInfo the topic information
     */
    public void unregisterTopic(final AutoDiscTopicInfo topicInfo)
    {
        log.debug("Unregistering topic info [{}]", topicInfo);

        // Remove it from que queue
        this.registeredTopicInfos.remove(topicInfo.getUniqueId());
    }

    /**
     * Register an auto-discovery topic-socket pair info
     *
     * @param topicSocketInfo the topic-socket pair information
     */
    public void registerTopicSocket(final AutoDiscTopicSocketInfo topicSocketInfo)
    {
        log.debug("Registering autodisc topic socket info [{}]", topicSocketInfo);

        // Broadcast the new information immediately to speed up the autodiscovery process
        this.sendMessageIfNotNull(MsgType.AUTO_DISC_TOPIC_SOCKET, topicSocketInfo);

        // Register the topic socket, it wont add it if already exists
        this.registeredTopicSocketInfos.add(topicSocketInfo);
    }

    /**
     * Unregister an auto-discovery topic-socket pair info
     *
     * @param topicSocketInfo the topic-socket pair information
     */
    public void unregisterTopicSocket(final AutoDiscTopicSocketInfo topicSocketInfo)
    {
        log.debug("Unregistering topic socket info [{}]", topicSocketInfo);

        // Remove it from the queue
        this.registeredTopicSocketInfos.remove(topicSocketInfo.getUniqueId());
    }

    /**
     * Send the next topic adverts. It will check the last time the advert was sent against the refresh interval configured.
     * If the refresh interval is reached the advert is sent. <p>
     *
     * It will only sent a maximum of 1 advert of each type (topic, topicsocket, instance) per call and return the number of
     * adverts that have been sent.
     *
     * @return the number of messages sent.
     */
    public int sendNextTopicAdverts()
    {
        // Get current time
        final long currentTime = System.currentTimeMillis();

        // Check topic info and topic socket info and send
        int numAdvertsSent =
                this.sendMessageIfNotNull(MsgType.AUTO_DISC_TOPIC, this.registeredTopicInfos.getNextIfShouldSend(currentTime)) +
                this.sendMessageIfNotNull(MsgType.AUTO_DISC_TOPIC_SOCKET, this.registeredTopicSocketInfos.getNextIfShouldSend(currentTime));

        // Check instance info and send
        final RegisteredInfo<AutoDiscInstanceInfo> instanceInfo = this.registeredInstanceInfo;
        if (instanceInfo != null)
        {
            numAdvertsSent += this.sendMessageIfNotNull(MsgType.AUTO_DISC_INSTANCE, instanceInfo.getIfShouldSendAndResetIfRequired(currentTime));
        }

        return numAdvertsSent;
    }

    /**
     * Send the provided message if is not null
     *
     * @param msgType the message type to send
     * @param serializable the message in the form of a serializable object
     * @return 0 if not sent, 1 if sent
     */
    int sendMessageIfNotNull(final byte msgType, final IUnsafeSerializable serializable)
    {
        // Check for null
        if (serializable == null)
        {
            return 0;
        }

        if (log.isTraceEnabled())
        {
            log.trace("Sending auto-discovery advert message [{}]", serializable);
        }

        try
        {
            // Reset the send buffer serializer offset
            this.sendBufferSerializer.setOffset(0);

            // Set msg type and write the base header
            this.reusableBaseHeader.setMsgType(msgType);
            this.reusableBaseHeader.toBinary(this.sendBufferSerializer);

            // Serialize the message
            serializable.toBinary(this.sendBufferSerializer);

            // Send the message
            publication.offer(this.sendBufferSerializer.getInternalBuffer(), 0, this.sendBufferSerializer.getOffset());

            return 1;
        }
        catch (final RuntimeException e)
        {
            log.error("Unexpected error sending auto-discovery advert message", e);
        }

        return 0;
    }

    /**
     * Republish all the information registered about the given topic for an specific transport type. <p>
     *
     * This information includes both topic info and topic socket info for the given topic name. <p>
     *
     * This method is used to speed up the auto-discovery process by resending all topic information when another
     * client is interested on adverts for the topic. <p>
     *
     * @param topicName the name of the topic
     * @param transportType transport type and direction for the topic
     */
    public void republishAllInfoAboutTopic(final String topicName, final AutoDiscTransportType transportType)
    {
        if (log.isTraceEnabled())
        {
            log.trace("Republishing all information about topic [{}] with transport type [{}]", topicName, transportType);
        }

        // Get current time
        final long now = System.currentTimeMillis();

        // Reset timeouts and sendMsgs for topic adverts
        this.registeredTopicInfos.resetNextSendTimeAndConsume(topicName, now,
                info -> info.getTransportType() == transportType,
                info -> sendMessageIfNotNull(MsgType.AUTO_DISC_TOPIC, info));

        // Reset timeouts and sendMsgs for topic socket adverts
        this.registeredTopicSocketInfos.resetNextSendTimeAndConsume(topicName, now,
                info -> info.getTransportType() == transportType,
                info -> sendMessageIfNotNull(MsgType.AUTO_DISC_TOPIC_SOCKET, info));
    }

    /**
     * Republish the stored instance information
     */
    public void republishInstanceInfo()
    {
        if (log.isTraceEnabled())
        {
            log.trace("Republishing current instance information [{}]", this.registeredInstanceInfo);
        }

        // Get the info, we do this to prevent modifications by getting the volatile variable
        final RegisteredInfo<AutoDiscInstanceInfo> instanceInfo = this.registeredInstanceInfo;

        // If settled, reset timeout and send again
        if (instanceInfo != null)
        {
            instanceInfo.resetNextExpectedSent(System.currentTimeMillis());
            this.sendMessageIfNotNull(MsgType.AUTO_DISC_INSTANCE, instanceInfo.getInfo());
        }
    }
}
