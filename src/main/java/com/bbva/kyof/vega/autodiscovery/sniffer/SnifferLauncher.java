package com.bbva.kyof.vega.autodiscovery.sniffer;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Launcher to run the sniffer in stand alone mode
 */
@Slf4j
public class SnifferLauncher
{
    /** Singletone instance of the sniffer */
    private static final AtomicReference<AutodiscManagerSniffer> SNIFFER_SINGLETONE = new AtomicReference<>();

    /**
     * Embedded media driver
     **/
    private static MediaDriver mediaDriver;

    /**
     * Aeron context
     **/
    private static Aeron aeron;

    /**
     * Private constructor to avoid instantiation
     */
    private SnifferLauncher()
    {
        // Nothing to do here
    }

    /**
     * Run the sniffer in stand alone mode using the provided arguments
     *
     * @param args arguments for the command line sniffer launch
     * @throws SnifferException exception thrown if there is any problem launching the sniffer
     */
    public static void main(final String[] args) throws SnifferException, InterruptedException
    {
        // Create a command line parser, parse and validate the parameters
        final SnifferCommandLineParser parser = new SnifferCommandLineParser();
        final SnifferParameters parameters = parser.parseCommandLine(args);
        //System.out.println(parameters);

        //media driver embebido
        mediaDriver = MediaDriver.launchEmbedded();

        //Aeron context
        final Aeron.Context ctx = new Aeron.Context();
        ctx.aeronDirectoryName(mediaDriver.aeronDirectoryName());

        //Aeron conntection
        aeron = Aeron.connect(ctx);

        //Sniffer manager, allows to receive the autodiscovery info from vega instances
        if (!SNIFFER_SINGLETONE.compareAndSet(null, new AutodiscManagerSniffer(aeron, parameters, new SnifferListener())))
        {
            log.error("Sniffer already running");
            throw new SnifferException("Sniffer already running");
        }

        SNIFFER_SINGLETONE.get().start();

        // Control loop, indicates that the sniffer is still alive
        while (SNIFFER_SINGLETONE.get() != null)
        {
            log.info("Sniffer running...");
            Thread.sleep(10000);
        }
    }

    /**
     * Stop the sniffer
     * @throws SnifferException if there is any issue
     */
    public static void stop() throws SnifferException
    {
        final AutodiscManagerSniffer sniffer = SNIFFER_SINGLETONE.getAndSet(null);

        if (sniffer == null)
        {
            log.error("Snifer is not running");
            throw new SnifferException("The sniffer is not running");
        }

        sniffer.close();

        mediaDriver.close();
        aeron.close();
    }

    /**
     * Implements the listener interface that handles the reception of all autodiscovery info
     */
    private static class SnifferListener implements ISnifferListener
    {
        @Override
        public void onNewAutoDiscTopicInfo(final AutoDiscTopicInfo info)
        {
            log.info("SNIFFER: New topic info event received from auto-discovery {}", info);
        }

        @Override
        public void onTimedOutAutoDiscTopicInfo(final AutoDiscTopicInfo info)
        {
            log.info("SNIFFER: Topic info event timed out in auto-discovery {}", info);
        }

        @Override
        public void onNewAutoDiscTopicSocketInfo(final AutoDiscTopicSocketInfo info)
        {
            log.info("SNIFFER: New topic socket info event received from auto-discovery {}", info);
        }

        @Override
        public void onTimedOutAutoDiscTopicSocketInfo(final AutoDiscTopicSocketInfo info)
        {
            log.info("SNIFFER: Topic socket info event timed out in auto-discovery {}", info);
        }

        @Override
        public void onNewAutoDiscInstanceInfo(final AutoDiscInstanceInfo info)
        {
            log.info("SNIFFER: New autodisc instance info event received {}", info);
        }

        @Override
        public void onTimedOutAutoDiscInstanceInfo(final AutoDiscInstanceInfo info)
        {
            log.debug("SNIFFER: New timed out autodisc instance info event received {}", info);
        }
    }
}
