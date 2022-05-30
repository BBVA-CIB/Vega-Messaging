package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscDaemonServerInfo;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.config.general.UnicastInfo;
import com.bbva.kyof.vega.util.collection.NativeArraySet;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import com.bbva.kyof.vega.util.net.InetUtil;
import io.aeron.Aeron;
import io.aeron.Publication;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * Implementation of IPublicationsManager to manage all the publications of the unicast daemon servers
 */
@Slf4j
public class PublicationsManager implements IPublicationsManager
{

    /**
     * All the Publication aeron sockets used to send the messages to the unicast daemons (high availability).
     *
     * This attribute enabledPublicationsInfo is used for obtaining a random publication quickly.
     * */
    private volatile NativeArraySet<PublicationInfo> enabledPublicationsInfo;

    /**
     * Map with all the Publication aeron socket by UUID as key that has been sent a discovery message
     *
     * When the client receives the first AUTO_DISC_DAEMON_SERVER_INFO from a unicast server,
     * it saves his Publication by UUID into this map.
     *
     * When the client receives the next AUTO_DISC_DAEMON_SERVER_INFO, it enables the daemon as enable using
     * this structure to access quickly to their data.
     * */
    private volatile Map<UUID, PublicationInfo> publicationsInfoByUUID;

    /**
     * Array with all the Publication aeron socket
     *
     * It is used:
     * - for sending all the DAEMON CLIENT INFO msgs to all the unicast daemon servers
     * - for getting the servers info when the first message is received, because at this moment, the UUID is unknown
     *  After receiving the first message, the data is saved into the map publicationsInfoByUUID by key UUID and
     *  after that, the servers info will be always searched from the map (because is quickly).
     */
    @Getter private final PublicationInfo[] publicationsInfoArray;

    /**
     * Constructor to configure and save all the necessary structures to monitoring the publications
     * It must be called at starting time to configure the PublicationsManager
     * @param aeron the Aeron instance
     * @param config with the unicast daemon IPs and Ports configuration
     */
    public PublicationsManager(final Aeron aeron, final AutoDiscoveryConfig config)
    {
        log.debug("Creating {} uniscast publications", config.getUnicastInfoArray().size());

        //Create all the publications for unicast daemons for High Availability embedded in PublicationInfo class
        //Initialize structures
        this.publicationsInfoByUUID = new LinkedHashMap<>();
        this.enabledPublicationsInfo = new NativeArraySet<>(PublicationInfo.class, config.getUnicastInfoArray().size());
        this.publicationsInfoArray = new PublicationInfo[config.getUnicastInfoArray().size()];

        for(int i=0; i < config.getUnicastInfoArray().size(); i++)
        {
            //Get the next configuration
            UnicastInfo unicastInfo = config.getUnicastInfoArray().get(i);

            // Create the aeron channel
            final String channel = AeronChannelHelper.createUnicastChannelString(
                    unicastInfo.getResolverDaemonAddress(),
                    unicastInfo.getResolverDaemonPort(),
                    config.getSubnetAddress()
            );

            if(log.isInfoEnabled())
            {
                log.info("Creating uniscast publication with channel [{}] and stream [{}] in position [{}]", channel,
                        config.getDefaultStreamId(), i);
            }

            // Create the publication. At first, it is supposed that the daemon is down, so the publication is disabled
            Publication publication = aeron.addPublication(channel, config.getDefaultStreamId());
            PublicationInfo publicationInfo = new PublicationInfo(
                    publication,
                    null,
                    InetUtil.convertIpAddressToInt( unicastInfo.getResolverDaemonAddress() ),
                    unicastInfo.getResolverDaemonPort(),
                    Boolean.FALSE);

            //Add the publicationInfo to the structures
            this.publicationsInfoArray[i] = publicationInfo;
        }
    }

    @Override
    public PublicationInfo getRandomPublicationInfo()
    {
        PublicationInfo publicationInfo= this.enabledPublicationsInfo.getRandomElement();
        if (publicationInfo != null && log.isInfoEnabled())
        {
            log.info("Returning a new Random publication to unicast discovery daemon server with uuid: {} ip:{} port:{}",
                    publicationInfo.getUniqueId(), publicationInfo.getUnicastResolverServerIp(),
                    publicationInfo.getUnicastResolverServerPort());
        }
        return publicationInfo;
    }

    @Override
    public void disablePublication(final AutoDiscDaemonServerInfo autoDiscDaemonServerInfo)
    {
        if (log.isDebugEnabled())
        {
            log.debug("Disabling uniscast publication for timeout of daemon server info msg: {}", autoDiscDaemonServerInfo);
        }

        //Get the publicationInfo with UUID. If the msg timeouts is because it was been received in the past,
        // and the publicationInfo was updated with UUID and inserted into the map.
        PublicationInfo publicationInfo = this.publicationsInfoByUUID.get(autoDiscDaemonServerInfo.getUniqueId());

        //If the publication is enabled, disable it.
        if (publicationInfo != null && publicationInfo.getEnabled())
        {
            publicationInfo.setEnabled(Boolean.FALSE);

            //And insert the publication into the enabledPublicationsInfo structure
            this.enabledPublicationsInfo.removeElement(publicationInfo);

            if(log.isInfoEnabled())
            {
                log.info("Disabled by timeout the uniscast publication with daemon server info msg: {}", autoDiscDaemonServerInfo);
            }
        }
    }

    @Override
    public void enablePublication(final AutoDiscDaemonServerInfo autoDiscDaemonServerInfo)
    {
        if(log.isDebugEnabled())
        {
            log.debug("Enabling uniscast publication with daemon server info msg: {}", autoDiscDaemonServerInfo);
        }

        //Obtain the publicationInfo from map publicationsInfoByUUID
        //If the data is not present (if this is the first message from the unicast daemon server),
        // get the data from the publicationsInfoArray and add the new data into the map
        PublicationInfo publicationInfo = this.publicationsInfoByUUID.computeIfAbsent(
                autoDiscDaemonServerInfo.getUniqueId(),
                mappingFunction -> searchPublicationInfoInAllConfigured(autoDiscDaemonServerInfo)
        );

        //If the publication is disabled, enable it.
        if (!publicationInfo.getEnabled())
        {
            publicationInfo.setEnabled(Boolean.TRUE);

            //And insert the publication into the enabledPublicationsInfo structure
            this.enabledPublicationsInfo.addElement(publicationInfo);

            if(log.isInfoEnabled())
            {
                log.info("Enabled uniscast publication with daemon server info msg: {}", autoDiscDaemonServerInfo);
            }
        }
    }

    /**
     *
     * Search the PublicationInfo inside the publicationsInfoArray that match with the IP and Port of the
     * message autoDiscDaemonServerInfo.
     *
     * If this method is called, it is because the data is not saved into the publicationsInfoByUUID,
     * because autoDiscDaemonServerInfo is the first message received from this server, and the UUID is not set. So,
     * the UUID is set.
     *
     * @param autoDiscDaemonServerInfo message from the unicast discovery server
     * @return PublicationInfo that match IP and Port
     */
    private PublicationInfo searchPublicationInfoInAllConfigured(final AutoDiscDaemonServerInfo autoDiscDaemonServerInfo){
        //Search the publication info
        PublicationInfo publicationInfo = Arrays.stream(publicationsInfoArray)
                .filter(publicationInfoParam -> validatePublicationInfo(publicationInfoParam, autoDiscDaemonServerInfo))
                .findFirst().get();
        publicationInfo.setUniqueId(autoDiscDaemonServerInfo.getUniqueId());
        if(log.isInfoEnabled())
        {
            log.info("Initialized publicationInfo with data: {}", autoDiscDaemonServerInfo);
        }
        return publicationInfo;
    }

    @Override
    public boolean hasEnabledPublications()
    {
        return !this.enabledPublicationsInfo.isEmpty();
    }

    @Override
    public void checkOldDaemonServerInfo()
    {
        //The number of publications configured is the publicationsInfoArray.length
        if(publicationsInfoByUUID.size() > publicationsInfoArray.length){

            //Make cleaning deleting all the entries that key != value.getUniqueId
            publicationsInfoByUUID.entrySet()
                    .removeIf(entry -> !entry.getKey().equals(entry.getValue().getUniqueId()) );
        }
    }

    /**
     * Validate if the publicationInfo match with the IP and Port of the message autoDiscDaemonServerInfo
     *
     * @param publicationInfoParam publicationInfo to validate
     * @param autoDiscDaemonServerInfo message from the unicast discovery server
     * @return true if the publicationInfo match with the IP and Port of the message autoDiscDaemonServerInfo
     */
    private boolean validatePublicationInfo(final PublicationInfo publicationInfoParam, final AutoDiscDaemonServerInfo autoDiscDaemonServerInfo)
    {
        if(publicationInfoParam.getUnicastResolverServerPort() != autoDiscDaemonServerInfo.getUnicastResolverServerPort())
        {
            //if Port does not match with the publicationInfo, is not the same daemon
            return false;
        }

        boolean isValid = false;
        if (publicationInfoParam.getUnicastResolverServerIp() == autoDiscDaemonServerInfo.getUnicastResolverServerIp() )
        {
            isValid = true;
        }
        else if (autoDiscDaemonServerInfo.getUnicastResolverHostname() != null)
        {
            int alternativeIp = InetUtil.getIpAddressAsIntByHostnameOrDefault(autoDiscDaemonServerInfo.getUnicastResolverHostname(), autoDiscDaemonServerInfo.getUnicastResolverServerIp());
            isValid = alternativeIp == publicationInfoParam.getUnicastResolverServerIp();
        }

        return isValid;
    }
}
