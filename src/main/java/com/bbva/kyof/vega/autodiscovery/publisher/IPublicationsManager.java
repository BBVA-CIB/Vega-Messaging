package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscDaemonServerInfo;

/**
 * Interface to implement the functionality High Availability to Unicast Discovery mechanism
 *
 * For this purpose, the client will manage all the publications to all the Unicast Daemon Servers,
 * and will monitoring their status
 *
 */
public interface IPublicationsManager
{
    /**
     * Getter for PublicationsInfoArray (array with all the publications)
     * @return Array with all the PublicationsInfo
     */
    PublicationInfo[] getPublicationsInfoArray();

    /**
     * Method than returns a ramdom Publication for implementing Load Balance functionality
     * @return an aleatory PublicationInfo
     */
    PublicationInfo getRandomPublicationInfo();

    /**
     * Method to disable a publication
     * @param autoDiscDaemonServerInfo Message with the unicast daemon information to disable it
     */
    void disablePublication(AutoDiscDaemonServerInfo autoDiscDaemonServerInfo);

    /**
     * Method to enable a publication
     * @param autoDiscDaemonServerInfo Message with the unicast daemon information to enable it
     */
    void enablePublication(AutoDiscDaemonServerInfo autoDiscDaemonServerInfo);

    /**
     * Method to test if there is an enabled publication of an Unicast Discovery Daemon Server
     * @return true if it exists any enabled publication
     */
    boolean hasEnabledPublications();

    /**
     * When a publication was enabled, is because the client received a msg AUTO_DISC_DAEMON_SERVER_INFO.
     * This can happen because the server is just started, or it is restarted.
     * If it is started, the publication has been saved using their UUID.
     * But if it is restarted, then the server will start with a new UUID. So, it will be necessary to
     * remove the old data from the publicationsInfoByUUID
     * This method performs that clean
     */
    void checkOldDaemonServerInfo();
}
