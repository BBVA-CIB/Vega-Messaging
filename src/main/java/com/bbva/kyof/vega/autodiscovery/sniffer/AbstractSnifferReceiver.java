package com.bbva.kyof.vega.autodiscovery.sniffer;

import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.autodiscovery.advert.ActiveAdvertsQueue;
import com.bbva.kyof.vega.autodiscovery.advert.ActiveTopicAdvertsQueue;
import com.bbva.kyof.vega.msg.BaseHeader;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import lombok.extern.slf4j.Slf4j;
import org.agrona.DirectBuffer;

import java.io.Closeable;
import java.util.UUID;

/**
 * Abstract class that is the base of specific implementations of the subscription functionality for sniffer.
 *
 * The class handles all subscription related features. The information received for subscribed elements is stored when
 * there is a new element with a time out. When there is a timeout the active advert stored is removed and a notification
 * is sent.
 *
 * This class notifies the listener when there is new autodiscovery information or a timeout.
 * It could be InstanceInfo, TopicInfo or SocketInfo and their corresponding timeouts.
 *
 * The class is not thread-safe!
 */
@Slf4j
public abstract class AbstractSnifferReceiver implements Closeable
{
    /**
     * Subscription connection with Aeron to receive auto-discovery messages
     */
    private final Subscription subscription;

    /**
     * Reusable unsafe buffer serializer to wrap incoming messages
     */
    private final UnsafeBufferSerializer bufferSerializer = new UnsafeBufferSerializer();

    /**
     * Reusable base header to deserialize the header of incoming messages
     */
    private final BaseHeader reusableBaseHeader = new BaseHeader();

    /**
     * Reusable auto-discovery instance info used to avoid object creation during deserialization
     */
    private AutoDiscInstanceInfo instanceInfo = new AutoDiscInstanceInfo();

    /**
     * Reusable auto-discovery topic socket info used to avoid object creation during deserialization
     */
    private AutoDiscTopicSocketInfo topicSocketInfo = new AutoDiscTopicSocketInfo();

    /**
     * Reusable auto-discovery topic info used to avoid object creation during deserialization
     */
    private AutoDiscTopicInfo topicInfo = new AutoDiscTopicInfo();

    /**
     * Store the queue with all the active instance information adverts
     */
    private final ActiveAdvertsQueue<AutoDiscInstanceInfo> instanceInfoActiveAdvertsQueue;

    /**
     * Store the queue with all the active topic information adverts
     */
    private final ActiveTopicAdvertsQueue<AutoDiscTopicInfo> topicInfoActiveAdvertsQueue;

    /**
     * Store the queue with all the active topic-socket pair information adverts
     */
    private final ActiveTopicAdvertsQueue<AutoDiscTopicSocketInfo> topicSocketActiveAdvertsQueue;

    /**
     * Sniffer listener that has to receive the events always
     */
    private final ISnifferListener snifferListener;

    /**
     * Create a new auto-discovery subscriber
     *
     * @param instanceId      unique id of the vega library instance this object belongs to
     * @param aeron           the Aeron instance
     * @param parameters      the auto-discovery configuration
     * @param snifferListener Global listener that has to receive the events always
     */
    AbstractSnifferReceiver(final UUID instanceId,
                            final Aeron aeron,
                            final SnifferParameters parameters,
                            final ISnifferListener snifferListener)
    {
        // Create the queues with the timeout of the configuration
        this.instanceInfoActiveAdvertsQueue = new ActiveAdvertsQueue<>(parameters.getTimeout());
        this.topicInfoActiveAdvertsQueue = new ActiveTopicAdvertsQueue<>(parameters.getTimeout());
        this.topicSocketActiveAdvertsQueue = new ActiveTopicAdvertsQueue<>(parameters.getTimeout());
        this.snifferListener = snifferListener;

        // Create the Aeron subscription
        this.subscription = this.createSubscription(instanceId, aeron, parameters, snifferListener);
    }

    /**
     * Create a the subscription for incoming messages.
     *
     * @param instanceId      unique id of the vega library instance this object belongs to
     * @param aeron           aeron instance
     * @param parameters      sniffer parameters
     * @param snifferListener sniffer listener
     * @return the created subscription
     */
    public abstract Subscription createSubscription(final UUID instanceId, final Aeron aeron, final SnifferParameters parameters, ISnifferListener snifferListener);

    @Override
    public void close()
    {
        log.info("Closing auto discovery sniffer receiver");

        // Close subscription
        this.subscription.close();

        // Clear internal queues
        this.topicInfoActiveAdvertsQueue.clear();
        this.topicSocketActiveAdvertsQueue.clear();
        this.instanceInfoActiveAdvertsQueue.clear();
    }

    /**
     * Poll for the next received message in the subscriber
     *
     * @return the number of messages received
     */
    int pollNextMessage()
    {
        return this.subscription.poll(this::processSubscriptionRcvMsg, 1);
    }

    /**
     * Check the next element in the internal list of active adverts for a timeout.<p>
     * <p>
     * It will check for the 3 active advert types, TopicInfo, TopicSocketInfo and InstanceInfo.<p>
     * <p>
     * If the advert has timed out, it will remove the element and notify the listener.<p>
     * <p>
     * The method don't have to go through all the elements stored, since they are always sorted in the internal
     * queues by timeout time. Just checking the oldest element in the queue is enough.<p>
     *
     * @return the number of time outs processed
     */
    int checkNextTimeout()
    {
        // Store the number of timed out elements
        int numTimeOuts = checkTopicInfoTimeouts();
        numTimeOuts += checkTopicSocketInfoTimeouts();
        numTimeOuts += checkInstanceInfoTimeouts();
        return numTimeOuts;
    }

    /**
     * Check for time out on instance info active adverts
     *
     * @return the number of timeouts
     */
    private int checkInstanceInfoTimeouts()
    {
        final AutoDiscInstanceInfo timedOutInstanceInfo = this.instanceInfoActiveAdvertsQueue.returnNextTimedOutElement();
        if (timedOutInstanceInfo != null)
        {
            // Notify about the new removal to all listeners
            this.snifferListener.onTimedOutAutoDiscInstanceInfo(timedOutInstanceInfo);
            return 1;
        }
        return 0;
    }

    /**
     * Check for time out on topic socket info active adverts
     *
     * @return the number of timeouts
     */
    private int checkTopicSocketInfoTimeouts()
    {
        // Check timeout in topic-socket info
        final AutoDiscTopicSocketInfo timedOutTopicSocketInfo = this.topicSocketActiveAdvertsQueue.returnNextTimedOutElement();
        if (timedOutTopicSocketInfo != null)
        {
            // Notify to the subscribed
            this.snifferListener.onTimedOutAutoDiscTopicSocketInfo(timedOutTopicSocketInfo);
            return 1;
        }
        return 0;
    }

    /**
     * Check for time out on topic info active adverts
     *
     * @return the number of timeouts
     */
    private int checkTopicInfoTimeouts()
    {
        // Check timeout in topic infos
        final AutoDiscTopicInfo timedOutTopicInfo = this.topicInfoActiveAdvertsQueue.returnNextTimedOutElement();
        if (timedOutTopicInfo != null)
        {
            this.snifferListener.onTimedOutAutoDiscTopicInfo(timedOutTopicInfo);

            return 1;
        }

        return 0;
    }

    /**
     * Process a received message from the Aeron Subscription. It is called by the fragment handler on message poll.
     *
     * @param buffer      the buffer that contains the received message
     * @param offset      the offset inside the buffer
     * @param length      the length of the message in the buffer
     * @param aeronHeader the header of the Aeron message
     */
    private void processSubscriptionRcvMsg(final DirectBuffer buffer, final int offset, final int length, final Header aeronHeader)
    {
        try
        {
            // Wrap the buffer into the serializer
            this.bufferSerializer.wrap(buffer, offset, length);

            // Read the base header
            this.reusableBaseHeader.fromBinary(this.bufferSerializer);

            // Check the version
            if (!this.reusableBaseHeader.isVersionCompatible())
            {
                log.warn("Autodiscovery message received from incompatible library version [{}]", Version.toStringRep(this.reusableBaseHeader.getVersion()));
                return;
            }

            // Check the message type and process
            switch (this.reusableBaseHeader.getMsgType())
            {
                case MsgType.AUTO_DISC_TOPIC:
                    this.onReceivedTopicInfoMsg();
                    break;
                case MsgType.AUTO_DISC_TOPIC_SOCKET:
                    this.onReceivedTopicSocketInfoMsg();
                    break;
                case MsgType.AUTO_DISC_INSTANCE:
                    this.onReceivedInstanceInfoMsg();
                    break;
                default:
                    log.warn("Wrong message type [{}] received on autodiscovery", this.reusableBaseHeader.getMsgType());
                    break;
            }
        }
        catch (final RuntimeException e)
        {
            log.error("Unexpected error processing received autodiscovery message", e);
        }
    }

    /**
     * Process a received message with information about a pair of topic-socket
     */
    private void onReceivedTopicSocketInfoMsg()
    {
        // Deserialize the message
        this.topicSocketInfo.fromBinary(this.bufferSerializer);

        if (log.isTraceEnabled())
        {
            log.trace("Processing received topic socket pair info message [{}]", this.topicSocketInfo);
        }

        // Add or update, if false is an update and there is nothing else to do
        if (!this.topicSocketActiveAdvertsQueue.addOrUpdateAdvert(this.topicSocketInfo))
        {
            return;
        }

        // Notify about the new addition to the Sniffer listener
        this.snifferListener.onNewAutoDiscTopicSocketInfo(this.topicSocketInfo);

        // Restart the reusable auto-discovery info object since the original has been stored
        this.topicSocketInfo = new AutoDiscTopicSocketInfo();
    }

    /**
     * Process a received message with information about a topic publisher or subscriber
     */
    private void onReceivedTopicInfoMsg()
    {
        // Deserialize the message
        this.topicInfo.fromBinary(this.bufferSerializer);

        if (log.isTraceEnabled())
        {
            log.trace("Processing received topic socket info message [{}]", this.topicInfo);
        }

        // Add or update, if is just an update there is nothing else to do
        if (!this.topicInfoActiveAdvertsQueue.addOrUpdateAdvert(this.topicInfo))
        {
            return;
        }

        //Notify about the new addition to the Sniffer listener
        this.snifferListener.onNewAutoDiscTopicInfo(this.topicInfo);

        // Restart the reusable auto-discovery info object since the original has been stored
        this.topicInfo = new AutoDiscTopicInfo();
    }

    /**
     * Process a received message with information about a library instance
     */
    private void onReceivedInstanceInfoMsg()
    {
        // Deserialize the message
        this.instanceInfo.fromBinary(this.bufferSerializer);

        if (log.isTraceEnabled())
        {
            log.trace("SNIFFER: Processing received instance info message [{}]", this.instanceInfo);
        }

        // If is an update, there is nothing else to do
        if (!this.instanceInfoActiveAdvertsQueue.addOrUpdateAdvert(this.instanceInfo))
        {
            return;
        }

        //Notify about the new addition to the Sniffer listener
        this.snifferListener.onNewAutoDiscInstanceInfo(this.instanceInfo);

        // Restart the reusable auto-discovery info object since the original has been stored
        this.instanceInfo = new AutoDiscInstanceInfo();
    }
}
