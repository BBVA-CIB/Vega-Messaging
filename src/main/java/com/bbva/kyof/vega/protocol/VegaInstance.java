package com.bbva.kyof.vega.protocol;

import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.autodiscovery.AutodiscManager;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.config.general.AeronDriverType;
import com.bbva.kyof.vega.config.general.ConfigReader;
import com.bbva.kyof.vega.config.general.GlobalConfiguration;
import com.bbva.kyof.vega.driver.EmbeddedMediaDriver;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.protocol.common.AsyncRequestManager;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.common.VegaInstanceParams;
import com.bbva.kyof.vega.protocol.control.ControlMsgsManager;
import com.bbva.kyof.vega.protocol.control.ControlSubscriberParams;
import com.bbva.kyof.vega.protocol.publisher.ITopicPublisher;
import com.bbva.kyof.vega.protocol.publisher.SendManager;
import com.bbva.kyof.vega.protocol.subscriber.AeronSubscriberParams;
import com.bbva.kyof.vega.protocol.subscriber.ITopicSubListener;
import com.bbva.kyof.vega.protocol.subscriber.ReceiveManager;
import io.aeron.*;
import io.aeron.driver.MediaDriver;
import io.aeron.exceptions.DriverTimeoutException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.agrona.ErrorHandler;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main framework class that represents an instance of the messaging library
 */
@Slf4j
public final class VegaInstance implements IVegaInstance, AvailableImageHandler, UnavailableImageHandler, ErrorHandler
{
    /** Context of the instance with all the common information */
    private final VegaContext vegaContext;

    /** Manager for publications */
    private final SendManager sendManager;

    /** Manager for reception */
    private final ReceiveManager receiveManager;

    /** Manager for control messages */
    private final ControlMsgsManager controlMsgsManager;

    /** Embedded media driver, null if the daemon is using an stand alone media driver or unmanaged media driver */
    private final EmbeddedMediaDriver embeddedMediaDriver;

    /** True if the manager has been stopped or is currently stopped */
    private final AtomicBoolean stoppedOrStopping = new AtomicBoolean(false);


    /**
     * Constructs the manager class, it will also creates and initializes the Aeron context
     *
     * @param parameters the parameters to initialize the instance
     * @throws VegaException exception thrown if there a configuration problem
     */
    private VegaInstance(final VegaInstanceParams parameters) throws VegaException
    {
        log.info("Creating a new Vega manager instance. Version [{}] Parameters [{}]", Version.LOCAL_VERSION, parameters);

        // Validate the parameters before continue
        parameters.validateParams();

        // Get the unmanaged media driver if any
        final MediaDriver unmanagedMediaDriver = parameters.getUnmanagedMediaDriver();

        // Load the configuration
        final GlobalConfiguration config = ConfigReader.readConfiguration(parameters.getConfigurationFile());

        // Make sure that if there are secure configured topics in configuration, the parameters also contains the security information
        if (config.hasAnySecureTopic() && parameters.getSecurityParams() == null)
        {
            log.error("Secure configured topics found in configuration but no security parameters have been provided for the instance");
            throw new VegaException("Security parameters have to be provided if there are secure topics configured");
        }

        // Create the aeron context
        final Aeron.Context aeronContext = new Aeron.Context();

        // Set the image handler to print new available aeron images on the log
        aeronContext.availableImageHandler(this);
        aeronContext.unavailableImageHandler(this);

        // Set the error handler for Aeron Errors, use the provided one or the default
        if (parameters.getAeronErrorHandler() != null)
        {
            aeronContext.errorHandler(parameters.getAeronErrorHandler());
        }
        else
        {
            aeronContext.errorHandler(this);
        }

        // Start the embedded media driver if required, use the unmanaged one or the external
        if (unmanagedMediaDriver != null)
        {
            this.embeddedMediaDriver = null;
            aeronContext.aeronDirectoryName(unmanagedMediaDriver.aeronDirectoryName());
        }
        else if (config.getDriverType() == AeronDriverType.EXTERNAL)
        {
            this.embeddedMediaDriver = null;

            // If a driver directory has been settled, configure it
            if (config.getExternalDriverDir() != null)
            {
                aeronContext.aeronDirectoryName(config.getExternalDriverDir());
            }
        }
        else
        {
            this.embeddedMediaDriver = new EmbeddedMediaDriver(config.getEmbeddedDriverConfigFile(), config.getDriverType());
            aeronContext.aeronDirectoryName(this.embeddedMediaDriver.getDriverDirectoryName());
        }

        // Create the Aeron client connection
        final Aeron aeron = Aeron.connect(aeronContext);

        // Create the instance context
        this.vegaContext = new VegaContext(aeron, config);

        // Initialize the security information
        this.vegaContext.initializeSecurity(parameters.getSecurityParams());

        // Create autodiscovery manager
        final AutodiscManager autodiscoManager = new AutodiscManager(aeron, config.getAutodiscConfig(), this.vegaContext.getInstanceUniqueId());

        // Set the autodiscovery manager
        this.vegaContext.setAutodiscoveryManager(autodiscoManager);

        // Create the asynchronous request manager and start it
        final AsyncRequestManager asyncRequestManager = new AsyncRequestManager(this.vegaContext.getInstanceUniqueId());
        this.vegaContext.setAsyncRequestManager(asyncRequestManager);

        // Initialize the control messages manager
        this.controlMsgsManager = new ControlMsgsManager(this.vegaContext);

        // Initialize the managers to sendMsg and receive
        this.sendManager = new SendManager(this.vegaContext, this.controlMsgsManager.getOwnPubSecureChangesNotifier());
        this.receiveManager = new ReceiveManager(this.vegaContext, this.controlMsgsManager.getSecureMessagesDecoder(), this.controlMsgsManager.getRecurityRequestsNotifier());

        // Start auto-discovery
        autodiscoManager.start();

        // Get the parameters of the subscriber for responses
        final AeronSubscriberParams responseSubscriberParams = this.receiveManager.getResponseSubscriberParams();

        // Get the parameters of the subscriber for control messages
        final ControlSubscriberParams controlMsgsSubscriberParams = this.controlMsgsManager.getControlMsgsSubscriberParams();

        // Create the information of the instance
        AutoDiscInstanceInfo autoDiscInstanceInfo = new AutoDiscInstanceInfo(
                parameters.getInstanceName(),
                vegaContext.getInstanceUniqueId(),
                responseSubscriberParams.getIpAddress(),
                responseSubscriberParams.getPort(),
                responseSubscriberParams.getStreamId(),
                controlMsgsSubscriberParams.getIpAddress(),
                controlMsgsSubscriberParams.getPort(),
                controlMsgsSubscriberParams.getStreamId());

        autodiscoManager.registerInstanceInfo(autoDiscInstanceInfo);
    }

    /**
     * Create a new instance of the library
     *
     * @param parameters the parameters to initialize the instance
     * @return the instance created
     * @throws VegaException exception thrown if there a configuration problem
     */
    public static IVegaInstance createNewInstance(final VegaInstanceParams parameters) throws VegaException
    {
        return new VegaInstance(parameters);
    }

    @Override
    public UUID getInstanceId()
    {
        return this.vegaContext.getInstanceUniqueId();
    }

    @Override
    public ITopicPublisher createPublisher(@NonNull final String topicName) throws VegaException
    {
        log.info("Creating publisher for topic [{}]", topicName);
        return this.sendManager.createTopicPublisher(topicName);
    }

    @Override
    public void destroyPublisher(@NonNull final String topicName) throws VegaException
    {
        log.info("Destroying publisher for topic [{}]", topicName);
        this.sendManager.destroyTopicPublisher(topicName);
    }

    @Override
    public void subscribeToTopic(@NonNull final String topicName, @NonNull final ITopicSubListener listener) throws VegaException
    {
        log.info("Subscribing to topic [{}]", topicName);
        this.receiveManager.subscribeToTopic(topicName, listener);
    }

    @Override
    public void unsubscribeFromTopic(@NonNull final String topicName) throws VegaException
    {
        log.info("Unsubscribing from topic [{}]", topicName);
        this.receiveManager.unsubscribeFromTopic(topicName);
    }

    @Override
    public void subscribeToPattern(@NonNull final String topicPattern, @NonNull final ITopicSubListener listener) throws VegaException
    {
        log.info("Subscribing to topic pattern [{}]", topicPattern);
        this.receiveManager.subscribeToPattern(topicPattern, listener);
    }

    @Override
    public void unsubscribeFromPattern(@NonNull final String topicPattern) throws VegaException
    {
        log.info("Unsubscribing from topic pattern [{}]", topicPattern);
        this.receiveManager.unsubscribefromPattern(topicPattern);
    }

    @Override
    public void close()
    {
        log.info("Stopping the Manager ID [{}]", this.vegaContext.getInstanceUniqueId());

        // Make sure it is not stopped or stopping already
        if (!this.stoppedOrStopping.compareAndSet(false, true))
        {
            return;
        }

        // Stop the async request manager
        this.vegaContext.getAsyncRequestManager().close();

        // Close the internal managers
        this.sendManager.close();
        this.receiveManager.close();
        this.controlMsgsManager.close();

        // Stop the auto discovery mechanism
        this.vegaContext.getAutodiscoveryManager().unregisterInstanceInfo();
        this.vegaContext.getAutodiscoveryManager().close();

        // Stop the heartbeats timer
        this.vegaContext.stopHeartsbeatTimer();

        // Stop the Aeron connection
        this.vegaContext.getAeron().close();

        if (this.embeddedMediaDriver != null)
        {
            this.embeddedMediaDriver.close();
        }

        log.info("Managers stopped successfully");
    }

    @Override
    public void onAvailableImage(final Image image)
    {
        final Subscription subscription = image.subscription();
        log.info("Available Aeron image: channel={} streamId={} session={}",
                subscription.channel(), subscription.streamId(), image.sessionId());
    }

    @Override
    public void onUnavailableImage(final Image image)
    {
        final Subscription subscription = image.subscription();
        log.info("Unavailable Aeron image: channel={} streamId={} session={}",
                subscription.channel(), subscription.streamId(), image.sessionId());
    }

    @Override
    public void onError(final Throwable throwable)
    {
        // Print the error on the log
        log.error("Unexpected Internal Aeron Error", throwable);

        if(throwable instanceof DriverTimeoutException)
        {
            log.error("Timeout from media driver, the system will exit...");
        }

        // Call the default implementation
        Aeron.Configuration.DEFAULT_ERROR_HANDLER.onError(throwable);
    }
}
