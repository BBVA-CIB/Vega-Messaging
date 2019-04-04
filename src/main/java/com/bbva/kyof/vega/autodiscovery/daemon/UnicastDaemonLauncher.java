package com.bbva.kyof.vega.autodiscovery.daemon;

import com.bbva.kyof.vega.autodiscovery.exception.AutodiscException;
import lombok.extern.slf4j.Slf4j;
import org.agrona.concurrent.SigIntBarrier;

/**
 * Launcher to run the unicast daemon in stand alone mode
 */
@Slf4j
public final class UnicastDaemonLauncher
{
    /** Private constructor to avoid instantiation */
    private UnicastDaemonLauncher()
    {
        // Nothing to do
    }

    /**
     * Run the daemon in stand alone mode using the provided arguments
     * @param args arguments for the command line daemon launch
     * @throws AutodiscException exception thrown if there is any problem launching the daemon
     */
    public static void main(final String [] args) throws AutodiscException
    {
        // Create a command line parser, parse and validate the parameters
        final CommandLineParser parser = new CommandLineParser();
        final DaemonParameters parameters = parser.parseCommandLine(args);

        log.info("Launching unicast daemon with parameters [{}]", parameters);

        try(final UnicastDaemon daemon = new UnicastDaemon(parameters))
        {
            daemon.start("ResolverUnicastDaeemon");

            log.info("Resolver unicast daemon started");

            new SigIntBarrier().await();

            log.info("Resolver unicast daemon stopped");
        }
    }
}
