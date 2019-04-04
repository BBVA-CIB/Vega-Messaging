package com.bbva.kyof.vega.autodiscovery.daemon;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscDaemonClientInfo;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import com.bbva.kyof.vega.util.collection.HashMapOfHashSet;
import com.bbva.kyof.vega.util.collection.NativeArraySet;
import io.aeron.Aeron;
import io.aeron.Publication;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.agrona.DirectBuffer;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Class that handles all the sending functionality for the Unicast Daemon.
 *
 * It will create the unicast sockets for any active client and fodward the received autodiscovery information in each one of the clients.
 */
@Slf4j
@AllArgsConstructor
class UnicastDaemonSender implements IDaemonReceiverListener, Closeable
{
    /** Store all the AeronPublishers by the parameters used to create it */
    private final Map<PublicationParams, Publication> publicationsByParams = new HashMap<>();

    /** Store all clients for a publication parameters */
    private final HashMapOfHashSet<PublicationParams, UUID> clientsByPublicationParams = new HashMapOfHashSet<>();

    /** Store all the created Publications for quick iteration using native array list */
    private final NativeArraySet<Publication> publications = new NativeArraySet<>(Publication.class, 10);

    /** The Aeron instance used to create the publications */
    private final Aeron aeron;

    /** Parameters used to create the unicast daemon instance */
    private final DaemonParameters parameters;

    @Override
    public void onNewAutoDiscDaemonClientInfo(final AutoDiscDaemonClientInfo msg)
    {
        log.info("New autodiscovery client info message received {}", msg);

        // Create the parameters for the publication
        final PublicationParams publicationParams = this.createPublicationParams(msg);

        // Security check
        if (this.clientsByPublicationParams.containsValue(publicationParams, msg.getUniqueId()))
        {
            log.warn("There is already a client registered with the same information {}", msg);
            return;
        }

        // Get the publication for the parameters or create a new one if it doesn't exists
        Publication publication = this.publicationsByParams.get(publicationParams);
        if (publication == null)
        {
            log.info("Creating new aeron publication to connect with client id [{}], publication parameters {}", msg.getUniqueId(), publicationParams);

            publication = this.createPublication(publicationParams);
            this.publicationsByParams.put(publicationParams, publication);
            this.publications.addElement(publication);
        }
        else
        {
            log.info("Client id [{}] match existing publication, reusing publication with parameters {}", msg.getUniqueId(), publicationParams);
        }

        this.clientsByPublicationParams.put(publicationParams, msg.getUniqueId());
    }

    @Override
    public void onRemovedAutoDiscDaemonClientInfo(final AutoDiscDaemonClientInfo timedOutInfo)
    {
        log.info("Removed autodiscovery client info message received {}", timedOutInfo);

        // Create the parameters for the publication
        final PublicationParams publicationParams = this.createPublicationParams(timedOutInfo);

        // Remove the client
        if (!this.clientsByPublicationParams.remove(publicationParams, timedOutInfo.getUniqueId()))
        {
            log.warn("Cannot find any registered client that match the timed out information {}", timedOutInfo);
            return;
        }

        // If no more clients for the parameters, close the publication
        if (!this.clientsByPublicationParams.containsKey(publicationParams))
        {
            log.info("Aeron publication with parameters [{}] has no more related clients, closing it", publicationParams);

            // Get the publication and remove from the map of params
            final Publication publicationToRemove = this.publicationsByParams.remove(publicationParams);
            // Remove from the list of publications
            this.publications.removeElement(publicationToRemove);
            // Close it
            publicationToRemove.close();
        }
    }

    @Override
    public void onNewMessageToFordward(final DirectBuffer buffer, final int offset, final int length)
    {
        if (log.isTraceEnabled())
        {
            log.trace("Fordwarding message to {} publishers", publications.getNumElements());
        }

        // Forward the message to all clients
        publications.consumeAll(publication -> publication.offer(buffer, offset, length));
    }

    @Override
    public void close()
    {
        log.info("Closing unicast daemon sender");

        // Close all publications
        this.publicationsByParams.values().forEach(Publication::close);

        // Clear the maps
        this.publicationsByParams.clear();
        this.clientsByPublicationParams.clear();
    }

    /**
     * Create the publication parameters that match the client connection received in the client info message
     *
     * @param clientInfo the message with the information of the client
     * @return the parameters por the aeron publication
     */
    private PublicationParams createPublicationParams(final AutoDiscDaemonClientInfo clientInfo)
    {
        return new PublicationParams(
                clientInfo.getUnicastResolverClientIp(),
                clientInfo.getUnicastResolverClientPort(),
                clientInfo.getUnicastResolverClientStreamId());
    }

    /**
     * Create a new Aeron Publication given the parameters for the publication
     *
     * @param params the publication parameters
     * @return the created aeron publication
     */
    private Publication createPublication(final PublicationParams params)
    {
        final String channel = AeronChannelHelper.createUnicastChannelString(params.getIpAddress(), params.getPort(), this.parameters.getSubnetAddress());
        final int streamId = params.getStreamId();

        log.info("Creating publication for channel {} and stream {}", channel, streamId);

        return this.aeron.addPublication(channel, streamId);
    }

    /**
     * Class to store all the parameters required to define an aeron publication that connects to a client
     */
    @Data
    private static class PublicationParams
    {
        /** Ip used for the publication (0 for ipc)*/
        private final int ipAddress;

        /** Port used for the publication (0 for ipc)*/
        private final int port;

        /** StreamId used by the publication */
        private final int streamId;
    }
}
