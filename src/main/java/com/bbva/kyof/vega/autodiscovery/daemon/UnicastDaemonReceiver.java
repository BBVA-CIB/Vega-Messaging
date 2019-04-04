package com.bbva.kyof.vega.autodiscovery.daemon;

import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscDaemonClientInfo;
import com.bbva.kyof.vega.autodiscovery.advert.ActiveAdvertsQueue;
import com.bbva.kyof.vega.msg.BaseHeader;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import lombok.extern.slf4j.Slf4j;
import org.agrona.DirectBuffer;

import java.io.Closeable;

/**
 * Class that handles all the receiving functionality for the Unicast Daemon.
 *
 * It will create the unicast sockets for reception of messages from the clients and process incoming messages.
 */
@Slf4j
class UnicastDaemonReceiver implements Closeable
{
    /** Aeron Subscription to receive messages from the clients */
    private final Subscription subscription;

    /** Reusable buffer serializer for the incomming messages */
    private final UnsafeBufferSerializer bufferSerializer = new UnsafeBufferSerializer();

    /** Reusable base header for received messages */
    private final BaseHeader reusableBaseHeader = new BaseHeader();

    /** Listener that will receive the events of added and removed clients */
    private final IDaemonReceiverListener listener;

    /** Reusable auto-discovery instance info used to avoid object creation during deserialization */
    private AutoDiscDaemonClientInfo reusableAutoDiscDaemonClientInfo = new AutoDiscDaemonClientInfo();

    /** Queue with all active adverts of daemon clients information */
    private final ActiveAdvertsQueue<AutoDiscDaemonClientInfo> activeDaemonClients;

    /**
     * Create a new receiver class for unicast daemon messages
     *
     * @param aeron the aeron instance that will be used to create the subscription
     * @param parameters the parameters of the daemon
     * @param listener the listener for events related to connected and disconnected clients
     */
    UnicastDaemonReceiver(final Aeron aeron, final DaemonParameters parameters, final IDaemonReceiverListener listener)
    {
        this.listener = listener;

        // Create the queue of active daemons
        this.activeDaemonClients = new ActiveAdvertsQueue<>(parameters.getClientTimeout());

        // Create the subscription channel
        final String channel = AeronChannelHelper.createUnicastChannelString(parameters.getIpAddress(), parameters.getPort(), parameters.getSubnetAddress());

        log.info("Creating UnicastDaemonReceiver Unicast Subscription. Channel {}, Stream {}", channel, DaemonParameters.DEFAULT_STREAM_ID);

        // Create the subscriber to receive from the clients
        this.subscription = aeron.addSubscription(channel, DaemonParameters.DEFAULT_STREAM_ID);

    }

    /**
     * Poll to get the next message in the subscription
     * @return the number of read messages
     */
    int pollForNewMessages()
    {
        return this.subscription.poll(this::processRcvMsg, 1);
    }

    /**
     * Check for the next active client advert timeout
     * @return 1 if there is a timeout, 0 in other case
     */
    int checkNextClientTimeout()
    {
        final AutoDiscDaemonClientInfo timedOutInfo = this.activeDaemonClients.returnNextTimedOutElement();

        // If not null, the active advert has timed out
        if (timedOutInfo != null)
        {
            log.info("Client timed out [{}]", timedOutInfo);

            // Notify about the removal
            this.listener.onRemovedAutoDiscDaemonClientInfo(timedOutInfo);
            return 1;
        }

        return 0;
    }

    @Override
    public void close()
    {
        log.info("Closing UnicastDaemonReceiver");

        this.subscription.close();
        this.activeDaemonClients.clear();
    }

    /**
     * Process a received message from the unicast subscription
     *
     * @param buffer the buffer containing the message contents
     * @param offset the offset of the message in the buffer
     * @param length the length of the message
     * @param aeronHeader the aeron message header
     */
    private void processRcvMsg(final DirectBuffer buffer, final int offset, final int length, final Header aeronHeader)
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

            if (log.isTraceEnabled())
            {
                log.trace("Autodiscovery client message of type [{}] received", this.reusableBaseHeader.getMsgType());
            }

            // Check the message type
            switch (this.reusableBaseHeader.getMsgType())
            {
                case MsgType.AUTO_DISC_DAEMON_CLIENT_INFO:
                    this.reusableAutoDiscDaemonClientInfo.fromBinary(this.bufferSerializer);
                    this.onClientInfoReceived(this.reusableAutoDiscDaemonClientInfo);
                    break;
                case MsgType.AUTO_DISC_INSTANCE:
                case MsgType.AUTO_DISC_TOPIC_SOCKET:
                case MsgType.AUTO_DISC_TOPIC:
                    this.listener.onNewMessageToFordward(buffer, offset, length);
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
     * Called when a message of client information type is received
     *
     * @param msg the client information message
     */
    private void onClientInfoReceived(final AutoDiscDaemonClientInfo msg)
    {
        if (log.isTraceEnabled())
        {
            log.trace("Processing daemon client information message [{}]", msg);
        }

        // If is an addition of new information, store and notify about it
        if (this.activeDaemonClients.addOrUpdateAdvert(msg))
        {
            log.info("Discovered new client [{}]", msg);

            // Restart the reusable autodiscovery info object since the original has been stored
            this.reusableAutoDiscDaemonClientInfo = new AutoDiscDaemonClientInfo();

            // Notify about the new addition
            this.listener.onNewAutoDiscDaemonClientInfo(msg);
        }
    }
}
