package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscDaemonClientInfo;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.msg.MsgType;
import io.aeron.Aeron;
import io.aeron.Publication;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;

/**
 * Implementation of auto-discovery sender handler for unicast auto-discovery type
 */
@Slf4j
public class AutodiscUnicastSender extends AbstractAutodiscSender implements Closeable
{
    /** Client info min send interval */
    private static final long CLIENT_INFO_MIN_SEND_INTERVAL = 10;
    /** Client info max send interval */
    private static final long CLIENT_INFO_MAX_SEND_INTERVAL = 500;
    /** Client info send interval increment factor */
    private static final int CLIENT_INFO_SEND_INC_FACTOR = 2;

    /**
     * Information of the daemon client represented by the auto-discovery instance of the library and
     * that has to be periodically sent with the reception socket info of the client.
     * The daemon will use this information to send the adverts to the client.
     */
    private volatile VariableSendRegisteredInfo<AutoDiscDaemonClientInfo> registeredDaemonClientInfo = null;

    /** Publication aeron socket used to send the messages */
    private PublicationInfo publicationInfo;

    /** Manager for all the publishers of unicast daemon servers*/
    private final IPublicationsManager publicationsManager;

    /**
     * Creates a new auto-discovery unicast sender
     * @param aeron the aeron instance
     * @param config the configuration for autodiscovery
     * @param daemonClientInfo the information of the daemon client with the reception socket information
     * @param pPublicationsManager PublicationsManager instance
     */
    public AutodiscUnicastSender(
            final Aeron aeron,
            final AutoDiscoveryConfig config,
            final AutoDiscDaemonClientInfo daemonClientInfo,
            final IPublicationsManager pPublicationsManager)
    {
        super(aeron, config);
        this.registeredDaemonClientInfo = new VariableSendRegisteredInfo<>(daemonClientInfo, CLIENT_INFO_MIN_SEND_INTERVAL, CLIENT_INFO_MAX_SEND_INTERVAL, CLIENT_INFO_SEND_INC_FACTOR);
        this.publicationsManager = pPublicationsManager;

        // Create the unicast publication
        this.publicationInfo = this.getPublicationInfo();
    }

    /**
     * Creates the Aeron publication object to send auto-discovery messages.
     *
     * @return the created Aeron publication
     */
    private PublicationInfo getPublicationInfo()
    {
        return this.publicationsManager.getRandomPublicationInfo();
    }

    @Override
    public Publication getPublication(){
        //If the actual selected publicationInfo is null or it becomes disable,
        // and does exists another publication enabled, change it
        // It it does not exists another enabled, maintains the old disabled one
        // (to maintain the old environment)
        if( (this.publicationInfo == null || !this.publicationInfo.getEnabled()) &&
                this.publicationsManager.hasEnabledPublications() )
        {
            this.publicationInfo = publicationsManager.getRandomPublicationInfo();
        }

        //If the publicationInfo is not null, return the publication
        if(this.publicationInfo != null)
        {
            return this.publicationInfo.getPublication();
        }

        //In this case, it does not exists a valid publication, so return null
        return null;
    }

    @Override
    public int sendNextTopicAdverts()
    {
        // Get the daemon client info it if should be sent
        final AutoDiscDaemonClientInfo daemonClientInfo = this.registeredDaemonClientInfo.getIfShouldSendAndResetIfRequired(System.currentTimeMillis());

        // Send the daemon client info to ALL the publications (to know if any disabled daemon is now enabled)
        // and to the rest of topic adverts
        return super.sendMessageIfNotNullToAllPublications
                (MsgType.AUTO_DISC_DAEMON_CLIENT_INFO, daemonClientInfo, this.publicationsManager.getPublicationsInfoArray())
                + super.sendNextTopicAdverts();
    }

    @Override
    public void close()
    {
        log.info("Closing auto discovery sender: publications");

        for(int i = 0; i < this.publicationsManager.getPublicationsInfoArray().length; i++)
        {
            this.publicationsManager.getPublicationsInfoArray()[i].getPublication().close();
        }
        super.close();
    }
}
