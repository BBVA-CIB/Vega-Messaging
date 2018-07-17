package com.bbva.kyof.vega.autodiscovery.daemon;

import com.bbva.kyof.vega.driver.AbstractEmbeddedMediaDriver;
import com.bbva.kyof.vega.driver.EmbeddedLowLatencyMediaDriver;
import com.bbva.kyof.vega.driver.EmbeddedMediaDriver;
import com.bbva.kyof.vega.util.threads.RecurrentTask;
import io.aeron.Aeron;
import lombok.extern.slf4j.Slf4j;
import org.agrona.concurrent.BackoffIdleStrategy;

/**
 * Unicast Daemon that acts as a central point for auto-discovery communication routing.<p>
 *
 * The Daemon will create an unicast reception socket to receive messages from the clients.<p>
 *
 * The client messages contains the socket where the client want's to receive the adverts from the
 * daemon. For each client the Daemon will create a socket to connect directly to the client.<p>
 *
 * The threading model follows a single thread pattern with multiple actions to reduce CPU usage and simplify synchronization.
 */
@Slf4j
public class UnicastDaemon extends RecurrentTask
{
    /** Object that will handle all the publication of messages for the Daemon */
    private final UnicastDaemonSender daemonPublisher;
    /** Object that will handle all the subscription of messages for the Daemon */
    private final UnicastDaemonReceiver daemonReceiver;
    /** Aeron instance, stored to use the same instance when publishers are created for clients */
    private final Aeron aeron;
    /** Embedded media driver, null if the daemon is using an stand alone media driver */
    private final AbstractEmbeddedMediaDriver embeddedMediaDriver;

    /**
     * Create a unicast daemon for the given parameters
     * @param parameters parameters for the daemon
     */
    public UnicastDaemon(final DaemonParameters parameters)
    {
        // Use sleep idle between iterations
        super(new BackoffIdleStrategy(1, 1, 1, 1));

        log.info("Creating Unicast Daemon with Parameters {}", parameters);

        // Create the aeron context
        final Aeron.Context aeronContext = new Aeron.Context();

        // Start the embedded daemon if required
        switch (parameters.getAeronDriverType())
        {
            case EMBEDDED:
                this.embeddedMediaDriver = new EmbeddedMediaDriver(parameters.getEmbeddedDriverConfigFile());
                aeronContext.aeronDirectoryName(this.embeddedMediaDriver.getDriverDirectoryName());
                break;
            case LOWLATENCY_EMBEDDED:
                this.embeddedMediaDriver = new EmbeddedLowLatencyMediaDriver(parameters.getEmbeddedDriverConfigFile());
                aeronContext.aeronDirectoryName(this.embeddedMediaDriver.getDriverDirectoryName());
                break;
            default:

                // Set external driver directory if settled
                if (parameters.getExternalDriverDir() != null)
                {
                    aeronContext.aeronDirectoryName(parameters.getExternalDriverDir());
                }

                this.embeddedMediaDriver = null;
                break;
        }

        // Create the aeron client
        this.aeron = Aeron.connect(aeronContext);

        // Start senders and receivers
        this.daemonPublisher = new UnicastDaemonSender(this.aeron, parameters);
        this.daemonReceiver = new UnicastDaemonReceiver(this.aeron, parameters, this.daemonPublisher);
    }

    @Override
    public int action()
    {
        // Poll for new messages and check for client timeouts
        return this.daemonReceiver.pollForNewMessages() + this.daemonReceiver.checkNextClientTimeout();
    }

    @Override
    public void cleanUp()
    {
        log.info("Stopping publishers and receivers and cleaning up");

        // Close sender and receiver
        this.daemonReceiver.close();
        this.daemonPublisher.close();

        // Close aeron instance
        this.aeron.close();

        // Stop the embedded media driver if required
        if (this.embeddedMediaDriver != null)
        {
            this.embeddedMediaDriver.close();
        }
    }
}
