package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.msg.MsgReqHeader;
import com.bbva.kyof.vega.msg.RcvMessage;
import com.bbva.kyof.vega.msg.RcvRequest;
import com.bbva.kyof.vega.msg.lost.MsgLostReport;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Represent a subscription to a topic name and keep track of all the AeronSubscribers sockets that are related to the topic name.
 *
 * The class assumes that listeners changes and aeron subscriber changes are always performed in thread-safe mode. It allows concurrent actions
 * of change listeners and receive messages.
 */
@Slf4j
class TopicSubscriber implements Closeable
{
    /** Topic Name the subscriber belongs to */
    @Getter private final String topicName;

    /** Unique id of the topic subscriber */
    @Getter private final UUID uniqueId = UUID.randomUUID();

    /** Listener for incoming messages due to normal subscription */
    private volatile ITopicSubListener normalListener;

    /** Listener for incoming messages due to pattern subscriptions */
    private final ConcurrentMap<String, ITopicSubListener> patternListenersByPattern = new ConcurrentHashMap<>();

    /** Return the topic configuration for this subscriber */
    @Getter private final TopicTemplateConfig topicConfig;

    /** Aeron subscribers related to this topic subscriber */
    private final Set<AeronSubscriber> aeronSubscribers = new HashSet<>();
    
    /** Stores the sequence number for each TopicId */
    @Getter(AccessLevel.PROTECTED)
    private final ConcurrentMap<UUID, AtomicLong> expectedSeqNumByTopicPubId = new ConcurrentHashMap<>();
    
    /**
     * Constructs a new topic subscriber
     *
     * @param topicName Topic name the subscriber is associated to
     * @param topicConfig Topic configuration
     */
    TopicSubscriber(final String topicName, final TopicTemplateConfig topicConfig)
    {
        this.topicName = topicName;
        this.topicConfig = topicConfig;
    }

    /**
     * Method called when a message is received.
     *
     * @param receivedMessage the received message
     */
    void onMessageReceived(final RcvMessage receivedMessage)
    {
        final ITopicSubListener currentNormalListener = this.normalListener;
        final MsgLostReport lostReport = this.checkMessageLoss(receivedMessage);

        if (notDuplicatedData(lostReport) && currentNormalListener != null)
        {
            if (lostReport != null)
            {
                this.normalListener.onMessageLost(lostReport);
            }
                        
            this.normalListener.onMessageReceived(receivedMessage);
        }

        if (notDuplicatedData(lostReport) && !this.patternListenersByPattern.isEmpty())
        {
            if (lostReport != null)
            {
                this.patternListenersByPattern.forEach((key, value) -> value.onMessageLost(lostReport));
            }
                        
            this.patternListenersByPattern.forEach((key, value) -> value.onMessageReceived(receivedMessage));
        }
    }

    /**
     * Method called when a request message is received.
     *
     * @param receivedRequest the received request
     */
    void onRequestReceived(final RcvRequest receivedRequest)
    {
        final MsgLostReport lostReport = this.checkMessageLoss(receivedRequest);

        if (notDuplicatedData(lostReport) && this.normalListener != null)
        {
            if (lostReport != null)
            {
                this.normalListener.onMessageLost(lostReport);
            }

            this.normalListener.onRequestReceived(receivedRequest);
        }

        if (notDuplicatedData(lostReport) && !this.patternListenersByPattern.isEmpty())
        {
            if (lostReport != null)
            {
                this.patternListenersByPattern.forEach((key, value) -> value.onMessageLost(lostReport));
            }

            this.patternListenersByPattern.forEach((key, value) -> value.onRequestReceived(receivedRequest));
        }
    }

    /**
     * Method called when a heartbeat request message is received.
     *
     * @param heartbeatReqMsgHeader the received heartbeat header request
     * @param topicName The name of the target topic
     */
    void onHeartbeatReceived(final MsgReqHeader heartbeatReqMsgHeader, final String topicName)
    {
        final MsgLostReport lostReport = this.checkHeartbeatLoss(heartbeatReqMsgHeader, topicName);

        if (this.normalListener != null && lostReport != null)
        {
                this.normalListener.onMessageLost(lostReport);
        }

        if (!this.patternListenersByPattern.isEmpty() && lostReport != null)
        {
                this.patternListenersByPattern.forEach((key, value) -> value.onMessageLost(lostReport));
        }
    }

    /**
     * Remove the normal listener that was created due to a normal topic subscription for incoming messages and requests
     * @return true if removed, false if it was not settled
     */
    boolean removeNormalListener()
    {
        if (this.normalListener == null)
        {
            return false;
        }

        this.normalListener = null;
        return true;
    }

    /**
     * Set the normal listener created due to a normal topic subscription for incoming messages and requests
     *
     * @param listener the listener
     * @return false if is was already settled
     */
    boolean setNormalListener(final ITopicSubListener listener)
    {
        if (this.normalListener != null)
        {
            return false;
        }

        this.normalListener = listener;
        return true;
    }

    /**
     * Add a listener created due to a pattern subscription for incoming messages and requests
     *
     * @param pattern the pattern of the pattern subscription that has generated the listener
     * @param listener the listenr for messages
     * @return false if there was already a listener for the pattern
     */
    boolean addPatternListener(final String pattern, final ITopicSubListener listener)
    {
        return this.patternListenersByPattern.putIfAbsent(pattern, listener) == null;
    }

    /**
     * Remove a listener created due to a pattern subscription for incoming messages and requests
     *
     * @param pattern the pattern of the pattern subscription that has generated the listener
     * @return false if there was already no listener for the pattern
     */
    boolean removePatternListener(final String pattern)
    {
        return this.patternListenersByPattern.remove(pattern) != null;
    }

    /**
     * Return true if there are no more listeners
     */
    boolean hasNoListeners()
    {
        return this.normalListener == null && this.patternListenersByPattern.isEmpty();
    }

    /**
     * Add a related aeron subscriber that may receive topics for the topic name represented by the topic subscriber
     * @param subscriber the aeron subscriber
     */
    void addAeronSubscriber(final AeronSubscriber subscriber)
    {
        this.aeronSubscribers.add(subscriber);
    }

    /**
     * Remove a related aeron subscriber that may receive topics for the topic name represented by the topic subscriber
     * @param subscriber the aeron subscriber
     */
    void removeAeronSubscriber(final AeronSubscriber subscriber)
    {
        this.aeronSubscribers.remove(subscriber);
    }

    /** Close the topic subscriber cleaning all internal information */
    @Override
    public void close()
    {
        this.aeronSubscribers.clear();
        this.normalListener = null;
        this.patternListenersByPattern.clear();
        this.expectedSeqNumByTopicPubId.clear();
    }

    /**
     * Run the given consumer function for all the internal related aeron subscribers
     * @param consumer the consumer that will be executed for each aeron subscriber
     */
    void runForEachRelatedAeronSubscriber(final Consumer<AeronSubscriber> consumer)
    {
        this.aeronSubscribers.forEach(consumer);
    }

    /**
     * True if the topic is configured to use security
     */
    public boolean hasSecurity()
    {
        return false;
    }

    /**
     * Method called when a message is received, checks for losses between this message and the last received message 
     * of the same topicPublisherId through the use of sequence numbers that are incorporated in the header of the message
     * 
     * @param msg the received message
     * @return MsgLostReport object with the loss information if there is a loss, if not return null
     */
    private MsgLostReport checkMessageLoss(final RcvMessage msg)
    {
        // Result of the loss check
        MsgLostReport lossResult = null;

        // Check if there is an expected sequence number for the topic publisher
        AtomicLong expectedSequenceNumber = this.expectedSeqNumByTopicPubId.get(msg.getTopicPublisherId());

        // There is no number, add a new expected sequence
        if (expectedSequenceNumber == null)
        {
            // Update the expected sequence number to the next one
            expectedSequenceNumber = new AtomicLong(msg.getSequenceNumber() + 1);
            this.expectedSeqNumByTopicPubId.put(msg.getTopicPublisherId(), expectedSequenceNumber);
        }
        else if (expectedSequenceNumber.get() != msg.getSequenceNumber()) // There is an expected sequence number, check for gap
        {
            // There is a gap and therefore a loss, create the loss report
            lossResult = new MsgLostReport(
                    msg.getInstanceId(),
                    msg.getTopicName(),
                    msg.getSequenceNumber() - expectedSequenceNumber.get(),
                    msg.getTopicPublisherId()
            );

            // Update expected sequence number to the next one received
            expectedSequenceNumber.set(msg.getSequenceNumber() + 1);

            log.warn("Message lost detected, sequence number found {}, {}, size {}",
                    msg.getSequenceNumber(), lossResult, msg.getContentLength());
        }
        else // In any other case everything is all right, just increment the expected sequence number
        {
            expectedSequenceNumber.incrementAndGet();
        }

        return lossResult;
    }

    /**
     * Method called when a heartbeat request is received, checks for losses between this message and the last received message
     * of the same topicPublisherId through the use of sequence numbers that are incorporated in the header of the message
     *
     * @param heartbeatReqMsgHeader the heartbeat header received message
     * @return MsgLostReport object with the loss information if there is a loss, if not return null
     */
    private MsgLostReport checkHeartbeatLoss(final MsgReqHeader heartbeatReqMsgHeader, final String topicName)
    {
        // Result of the loss check
        MsgLostReport lossResult = null;

        // Check if there is an expected sequence number for the topic publisher
        AtomicLong expectedSequenceNumber = this.expectedSeqNumByTopicPubId.get(heartbeatReqMsgHeader.getTopicPublisherId());

        // There is no number, add a new expected sequence
        if (expectedSequenceNumber == null)
        {
            // Update the expected sequence number to the next one
            expectedSequenceNumber = new AtomicLong(heartbeatReqMsgHeader.getSequenceNumber() + 1);
            this.expectedSeqNumByTopicPubId.put(heartbeatReqMsgHeader.getTopicPublisherId(), expectedSequenceNumber);
        }
        else if (expectedSequenceNumber.get() != heartbeatReqMsgHeader.getSequenceNumber()) // There is an expected sequence number, check for gap
        {
            // There is a gap and therefore a loss, create the loss report
            lossResult = new MsgLostReport(
                    heartbeatReqMsgHeader.getInstanceId(),
                    topicName,
                    heartbeatReqMsgHeader.getSequenceNumber() - expectedSequenceNumber.get(),
                    heartbeatReqMsgHeader.getTopicPublisherId()
            );

            // Update expected sequence number to the next one received
            expectedSequenceNumber.set(heartbeatReqMsgHeader.getSequenceNumber() + 1);

            log.warn("Message lost detected by heartbeat, sequence number found {}, {}", heartbeatReqMsgHeader.getSequenceNumber(), lossResult);
        }
        else // In any other case everything is all right, just increment the expected sequence number
        {
            expectedSequenceNumber.incrementAndGet();
        }

        return lossResult;
    }

    /**
     * Check if the received message is not a duplicated one
     * @param lostReport report of loss data
     * @return boolean if it is a duplicated message
     */
    private boolean notDuplicatedData(MsgLostReport lostReport)
    {
        return lostReport == null || lostReport.getNumberLostMessages() >= 0;
    }

    /**
     * Notify that a topic publisher has been removed. It will be deleted from the map of expected sequence numbers
     * to keep memory clean
     *
     * @param topicPubId the unique ID of the topic publisher
     */
    void onTopicPublisherRemoved(final UUID topicPubId)
    {
        this.expectedSeqNumByTopicPubId.remove(topicPubId);
    }
}
