package com.bbva.kyof.vega.autodiscovery.sniffer;

import com.bbva.kyof.vega.util.threads.RecurrentTask;
import io.aeron.Aeron;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.agrona.concurrent.SleepingIdleStrategy;

import java.util.UUID;

/**
 * Main class that manages all the auto-discovery for a sniffer. It handles the reception of messages
 * and the related events .
 *
 * The implementation follows a single thread model in which all the required actions are performed in sequence. This approach reduces
 * the amount of CPU consumed and simplify the synchronization process.
 *
 */
@Slf4j
public class AutodiscManagerSniffer extends RecurrentTask
{
    /**
     * Instance unique identifier
     */
    @Getter
    private final UUID instanceId = UUID.randomUUID();

    /**
     * Handler for receiving functionality on autodiscovery
     */
    private final AbstractSnifferReceiver autodiscSub;

    /**
     * Create a new multicast receiver for the sniffer
     *
     * @param aeron      the Aeron instance
     * @param parameters the parameters from the sniffer
     * @param listener   listener for events
     */
    public AutodiscManagerSniffer(final Aeron aeron, final SnifferParameters parameters, final ISnifferListener listener)
    {
        // Idle strategy will be sleep to use as less CPU as possible
        super(new SleepingIdleStrategy(1000));

        log.info("Creando Autodisc Manager Sniffer " + instanceId);
        this.autodiscSub = new SnifferMcastReceiver(instanceId, aeron, parameters, listener);
    }

    /**
     * Start the sniffer
     */
    public void start()
    {
        super.start("AutodiscManagerSniffer_" + this.instanceId);
    }


    @Override
    public int action()
    {
        // Apply pending user actions
        int actionsApplied = this.autodiscSub.pollNextMessage();

        // Check next timeout
        actionsApplied += this.autodiscSub.checkNextTimeout();

        // Return the number of actions taken
        return actionsApplied;
    }

    @Override
    public void cleanUp()
    {
        log.info("Cleaning up after being stopped");

        //Close autodiscSub
        this.autodiscSub.close();
    }
}
