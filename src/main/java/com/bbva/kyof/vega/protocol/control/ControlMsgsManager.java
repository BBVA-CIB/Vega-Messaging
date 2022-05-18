package com.bbva.kyof.vega.protocol.control;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.autodiscovery.subscriber.IAutodiscInstanceListener;
import com.bbva.kyof.vega.config.general.ControlRcvConfig;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import com.bbva.kyof.vega.util.net.InetUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;

/**
 * Manager that handles the control messages, creating both the publishers for each instance and the subscriber to get
 * the control messages.
 */
@Slf4j
public class ControlMsgsManager implements Closeable, IAutodiscInstanceListener
{
    /** Vega library isntance context */
    private final VegaContext vegaContext;

    /** Manager for control publishers */
    private final ControlPublishers controlPublishers;

    /** Socket wrapper that can receive control messages */
    private final ControlSubscriber controlSubscriber;

    /** Receive poller for control messages */
    private final ControlMsgsPoller rcvPoller;

    /** Handler for security requests */
    private final SecurityRequestsRcvHandler securityRequestsRcvHandler;

    /** Requester to perform security requests, process the responses and store the security information */
    private final SecurityRequester securityRequester;

    /** Lock for the class */
    private final Object lock = new Object();

    /** True if it has been closed */
    private boolean isClosed = false;


    /**
     * Creates a new manager for control messages
     *
     * @param vegaContext the context of the instance
     */
    public ControlMsgsManager(final VegaContext vegaContext)
    {
        this.vegaContext = vegaContext;

        this.controlPublishers = new ControlPublishers(this.vegaContext);

        // Create the receiver for control messages
        this.controlSubscriber = this.createControlSubscriber();

        // Create the handler for security requests
        this.securityRequestsRcvHandler = new SecurityRequestsRcvHandler(vegaContext, this.controlPublishers);

        // Create the security requester
        this.securityRequester = new SecurityRequester(this.vegaContext, this.controlPublishers);

        // Create the poller for control messages
        this.rcvPoller = new ControlMsgsPoller(this.controlSubscriber, this.securityRequestsRcvHandler, this.securityRequester, this.vegaContext.getInstanceUniqueId());
        this.rcvPoller.start();

        // Subscribe to instance info changes
        this.vegaContext.getAutodiscoveryManager().subscribeToInstances(this);
    }

    @Override
    public void close()
    {
        synchronized (this.lock)
        {
            // Unsubscribe from instances information
            this.vegaContext.getAutodiscoveryManager().unsubscribeFromInstances(this);

            // Stop the control messages poller
            this.rcvPoller.close();

            // Stop security requester and security rcv handler
            this.securityRequestsRcvHandler.close();
            this.securityRequester.close();

            // Destroy the control subscriber
            this.controlSubscriber.close();

            // Destroy control publishers
            this.controlPublishers.close();

            // Set as closed
            this.isClosed = true;
        }
    }

    @Override
    public void onNewAutoDiscInstanceInfo(final AutoDiscInstanceInfo info)
    {
        log.debug("New autodisc inscance info event received {}", info);

        synchronized (this.lock)
        {
            if (this.isClosed)
            {
                return;
            }

            this.controlPublishers.onNewAutoDiscInstanceInfo(info);
        }
    }

    @Override
    public void onTimedOutAutoDiscInstanceInfo(final AutoDiscInstanceInfo info)
    {
        log.debug("New timed out autodisc instance info event received {}", info);

        synchronized (this.lock)
        {
            if (this.isClosed)
            {
                return;
            }

            this.controlPublishers.onTimedOutAutoDiscInstanceInfo(info);
        }
    }

    /**
     * Create the aeron control subscriber that will listen for control messages
     * @return the created subscriber
     */
    private ControlSubscriber createControlSubscriber()
    {
        // Create a hash for the instance id. We will use it to select a random port and stream
        final int instanceIdHash = this.vegaContext.getInstanceUniqueId().hashCode();

        // Get the configuration for control messages receiver
        final ControlRcvConfig controlRcvConfig = this.vegaContext.getInstanceConfig().getControlRcvConfig();

        // Select the Stream ID
        final int streamId = AeronChannelHelper.selectStreamFromRange(instanceIdHash, controlRcvConfig.getNumStreams());

        // Select the ip address using the subnet address, since we are using 32 bit mask subnets we can use that address directly
        final String ipAddress = controlRcvConfig.getSubnetAddress().getIpAddres().getHostAddress();

        // Select the port
        final int portNumber = AeronChannelHelper.selectPortFromRange(instanceIdHash, controlRcvConfig.getMinPort(), controlRcvConfig.getMaxPort());

        // Create the parameters
        final ControlSubscriberParams params = new ControlSubscriberParams(InetUtil.convertIpAddressToInt(ipAddress), portNumber, streamId, controlRcvConfig.getSubnetAddress(),
                controlRcvConfig.getHostname());

        // Create the subscriber
        return new ControlSubscriber(this.vegaContext, params);
    }

    /**
     * Return the parameters used to create the control messages subsriber
     * @return the parameters used to create the control messages subsriber
     */
    public ControlSubscriberParams getControlMsgsSubscriberParams()
    {
        return this.controlSubscriber.getParams();
    }

    /**
     * Return the own notifier for secure changes on owned secure topic publishers
     * @return the own notifier for secure changes on owned secure topic publishers
     */
    public IOwnSecPubTopicsChangesListener getOwnPubSecureChangesNotifier()
    {
        return this.securityRequestsRcvHandler;
    }

    /**
     * Return the the decoder for security messages
     * @return the the decoder for security messages
     */
    public ISecuredMsgsDecoder getSecureMessagesDecoder()
    {
        return this.securityRequester;
    }

    /**
     * Return the the request notifier to obtain the security information of publisher topics
     * @return the the request notifier to obtain the security information of publisher topics
     */
    public ISecurityRequesterNotifier getRecurityRequestsNotifier()
    {
         return this.securityRequester;
    }
}
