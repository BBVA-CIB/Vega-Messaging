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
    private static final int SEND_BUFFER_SIZE = 1024;

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

    /** Variable used to control to print a warning each second when does not exists a valid publication, **/
    private long lastPublicationNullWarn = System.currentTimeMillis();

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

        // Prepare the reusable buffer serializer
        this.sendBufferSerializer.wrap(ByteBuffer.allocate(SEND_BUFFER_SIZE));
    }

    /**
     * Getter for the publication.
     *
     * It is abstract because the unicast and multicast environments are different
     *
     * @return the publication
     */
    public abstract Publication getPublication();

    @Override
    public void close()
    {
        log.info("Closing auto discovery sender: registeredTopicSocketInfos & registeredTopicInfos");

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

            // Send the message if there is an enabled publication
            // In unicast mode, if it does not exist replies from any of the unicast daemon servers,
            // all the publications become disabled until new messages are received
            //In multicast mode, it does exist an enabled publication always
            Publication publication = this.getPublication();

            if (publication == null)
            {
                //Only print one warning each second
                if(System.currentTimeMillis() - lastPublicationNullWarn > 1000)
                {
                    lastPublicationNullWarn = System.currentTimeMillis();
                    log.warn(
                            "It is not possible to send auto-discovery advert message, because it does not exist an enabled publication for msgType = {}", MsgType.toString(msgType) );
                }
            }
            else
            {
                publication.offer(this.sendBufferSerializer.getInternalBuffer(), 0, this.sendBufferSerializer.getOffset());
                return 1;
            }
        }
        catch (final RuntimeException e)
        {
            log.error("Unexpected error sending auto-discovery advert message", e);
        }

        return 0;
    }

    /**
     * Send the provided message if is not null to all the publishers
     *
     * Although this method is for the unicast case, it is placed here for reuse the reusable variables
     *
     * @param msgType the message type to send
     * @param serializable the message in the form of a serializable object
     * @param publicationsInfoArray array with all the publications to send the message
     * @return 0 if not sent, 1 if sent
     */
    int sendMessageIfNotNullToAllPublications(final byte msgType, final IUnsafeSerializable serializable, final PublicationInfo[] publicationsInfoArray)
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

            // Send the same message to all the publishers
            for(int i = 0; i < publicationsInfoArray.length; i++)
            {
                publicationsInfoArray[i].getPublication().offer(this.sendBufferSerializer.getInternalBuffer(), 0, this.sendBufferSerializer.getOffset());
            }
            return 1;
        }
        catch (final RuntimeException e)
        {
            log.error("Unexpected error sending auto-discovery advert message", e);
        }

        return 0;
    }
}
