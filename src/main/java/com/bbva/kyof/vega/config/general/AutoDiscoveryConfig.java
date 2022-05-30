
package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.config.IConfiguration;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains the configuration for hte auto-discovery process. The process can be unicast using a centralized daemon router or
 * distributed using multicast. <p>
 *
 * For the optional parameters a default value will be provided if the optional parameter is missing.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AutoDiscoveryConfig")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AutoDiscoveryConfig implements IConfiguration
{
    //---------------------- Constant values ------------------------
    /** Default topic advert sendMsg interval in milliseconds */
    static final long DEFAULT_REFRESH_INTERVAL = 1000;
    /** Default received end-point expiration in milliseconds */
    static final long DEFAULT_EXPIRATION_TIMEOUT = 10000;
    /** Default multicast address for autodiscovery */
    static final String DEFAULT_MULTICAST_ADDRESS = "225.0.0.1";
    /** Default multicast port autodiscovery */
    static final int DEFAULT_MULTICAST_PORT = 35000;
    /** Default resolver daemon port */
    static final int DEFAULT_RESOLVER_DAEMON_PORT = 1;
    /** Default resolver receiver min port */
    static final int DEFAULT_UNI_RSV_RCV_MIN_PORT = 35002;
    /** Default resolver receiver max port */
    static final int DEFAULT_UNI_RSV_RCV_MAX_PORT = 35003;
    /** Default resolver receiver num streams */
    static final int DEFAULT_UNI_RSV_RCV_NUM_STREAMS = 10;
    /** Default stream id for auto-discovery messages */
    static final int DEFAULT_STREAM_ID = 10;
    //---------------------------------------------------------------

    /** (Compulsory) The autoDiscoType of Autodiscovery */
    @XmlElement(name = "autodisc_type", required = true)
    @Getter private AutoDiscoType autoDiscoType;

    /** (Optional) Defines in milliseconds the refresh interval to send advert messages */
    @XmlElement(name = "refresh_interval")
    @Getter private Long refreshInterval;

    /** (Optional) Defines in milliseconds the time out for received adverts */
    @XmlElement(name = "timeout")
    @Getter private Long timeout;

    /** (Optional, only unicast) The resolver daemon address (backward compatibility)
     * This data will be inserted into unicastInfoArray and it will not used any more
     */
    @XmlElement(name = "resolver_daemon_address")
    @Deprecated
    private String resolverDaemonAddress;

    /** (Optional, only unicast) The resolver daemon port  (backward compatibility)
     * This data will be inserted into unicastInfoArray and it will not used any more
     */
    @XmlElement(name = "resolver_daemon_port")
    @Deprecated
    private Integer resolverDaemonPort;

    /** (Optional, only unicast) Address and port for all the resolver daemons */
    @XmlElement(name = "unicast_info")
    @Getter private List<UnicastInfo> unicastInfoArray;

    /** (Optional, only unicast) Min port for unicast resolver subscription */
    @XmlElement(name = "unicast_resolver_port_min")
    @Getter private Integer unicastResolverRcvPortMin;

    /** (Optional, only unicast) Max port for unicast resolver subscription */
    @XmlElement(name = "unicast_resolver_port_max")
    @Getter private Integer unicastResolverRcvPortMax;

    /** (Optional, only unicast) Num streams for unicast resolver subscription */
    @XmlElement(name = "unicast_resolver_num_streams")
    @Getter private Integer unicastResolverRcvNumStreams;

    /** (Optional, only multicast) The multicast address */
    @XmlElement(name = "multicast_address")
    @Getter private String multicastAddress;

    /** (Optional, only multicast) The multicast port */
    @XmlElement(name = "multicast_port")
    @Getter private Integer multicastPort;

    /** (Optional) The subnet to use */
    @XmlElement(name = "subnet")
    private String subnet;

    /** SubnetAddress to use, obtained from the provided multicast interface or the default interface */
    @XmlTransient
    @Getter private SubnetAddress subnetAddress;

    /** (Optional) The hostname  to use */
    @XmlElement(name = "unicast_alternative_hostname")
    @Getter private String hostname;

    /** (Optional) Resolve hostname from clients to get ip address */
    @XmlElement(name = "resolve_unicast_hostname")
    @Getter private Boolean isResolveHostname;

    @Override
    public void completeAndValidateConfig() throws VegaException
    {
        this.checkAutoDiscoveryType();
        this.checkRefreshInterval();
        this.checkTimeout();
        this.checkSubnet();

        // Behaviour is different in multicast and in unicast daemon for the rest of fields
        if (this.autoDiscoType == AutoDiscoType.UNICAST_DAEMON)
        {
            this.validateUnicastDaemonConfig();
        }
        else if (this.autoDiscoType == AutoDiscoType.MULTICAST)
        {
            this.validateMulticastConfig();
        }

        this.checkHostname();
    }


    /** @return the default stream id for auto-discovery communication */
    public int getDefaultStreamId()
    {
        return DEFAULT_STREAM_ID;
    }

    /**
     * Checks and assigns a correct value to the subnet
     *
     * @throws VegaException if any error happens checking the subnet
     */
    private void checkSubnet() throws VegaException
    {
        // Create the subnet address
        this.subnetAddress = ConfigUtils.getFullMaskSubnetFromStringOrDefault(this.subnet);
    }

    /** Checks and assigns a correct value to the timeout */
    private void checkTimeout()
    {
        // Timeout check
        if (this.timeout == null)
        {
            this.timeout = DEFAULT_EXPIRATION_TIMEOUT;
        }
    }

    /** Checks and assigns a correct value to the refresh interval */
    private void checkRefreshInterval()
    {
        // Refresh interval check
        if (this.refreshInterval == null)
        {
            this.refreshInterval = DEFAULT_REFRESH_INTERVAL;
        }
    }

    /**
     * Checks if compulsory value of auto-disco type is missing
     *
     * @throws VegaException if compulsory field autoDiscoType is missing
     */
    private void checkAutoDiscoveryType() throws VegaException
    {
        // Configuration autoDiscoType is compulsory
        if (this.autoDiscoType == null)
        {
            throw new VegaException("Missing compulsory field autoDiscoType");
        }
    }

    /**
     * Validate parameters for multicast configuration
     *
     * @throws VegaException if any parameter is not valid
     */
    private void validateMulticastConfig() throws VegaException
    {
        // Check the multicast address
        if (this.multicastAddress == null)
        {
            this.multicastAddress = DEFAULT_MULTICAST_ADDRESS;
        }
        ConfigUtils.validateMulticastAddress(this.multicastAddress);

        // Check the multicast port
        if (this.multicastPort == null)
        {
            this.multicastPort = DEFAULT_MULTICAST_PORT;
        }
        ConfigUtils.validatePortNumber(this.multicastPort);
    }

    /**
     * Validate parameters for unicast configuration
     *
     * @throws VegaException if any parameter is not valid
     */
    private void validateUnicastDaemonConfig() throws VegaException
    {
        //Move the old style unicast daemon address and port to the new structure.
        saveOldStyleDaemonAddressAndPort();

        //Check all the IPs and ports configurations
        for(UnicastInfo unicastInfo: unicastInfoArray){

            // Check the unicast address
            if (unicastInfo.getResolverDaemonAddress() == null)
            {
                throw new VegaException("The resolver daemon address is missing");
            }
            ConfigUtils.validateUnicastAddress(unicastInfo.getResolverDaemonAddress());

            // Check the unicast port
            if (unicastInfo.getResolverDaemonPort() == null)
            {
                unicastInfo.setResolverDaemonPort(DEFAULT_RESOLVER_DAEMON_PORT);
            }
            ConfigUtils.validatePortNumber(unicastInfo.getResolverDaemonPort());
        }

        //Check if is it some unicastInfo configured
        if(unicastInfoArray.isEmpty()){
            throw new VegaException("The resolver daemon address is missing");
        }

        // Check reception port range
        if (this.unicastResolverRcvPortMin == null)
        {
            this.unicastResolverRcvPortMin = DEFAULT_UNI_RSV_RCV_MIN_PORT;
        }

        // Check reception port range
        if (this.unicastResolverRcvPortMax == null)
        {
            this.unicastResolverRcvPortMax = DEFAULT_UNI_RSV_RCV_MAX_PORT;
        }

        // Validate the port range
        ConfigUtils.validatePortRange(this.unicastResolverRcvPortMin, this.unicastResolverRcvPortMax);

        // Finally check the streams
        if (this.unicastResolverRcvNumStreams == null)
        {
            this.unicastResolverRcvNumStreams = DEFAULT_UNI_RSV_RCV_NUM_STREAMS;
        }
    }

    /**
     * With the new functionality of adding various unicast daemons, the configuration has change, adding an array of
     * IPs and ports. But to maintain backward compatibility, the fields this.resolverDaemonAddress
     * and this.resolverDaemonPort are still supported.
     * This function is used to convert the old style configuration params to the new structure
     */
    private void saveOldStyleDaemonAddressAndPort()
    {
    	//If there are only an old configuration, unicastInfoArray is null
    	if(unicastInfoArray == null)
		{
			unicastInfoArray = new ArrayList<>();
		}

        //If old style address is not null, configure into the new structure
        if (this.resolverDaemonAddress != null)
        {
            //If existe an old style address configured but there is not port, use default port
            if (this.resolverDaemonPort == null)
            {
                this.resolverDaemonPort = DEFAULT_RESOLVER_DAEMON_PORT;
            }

            //Insert the IP and port (old configuration style) into unicastInfoArray (new functionality)
            this.unicastInfoArray.add( new UnicastInfo(this.resolverDaemonAddress, this.resolverDaemonPort) );

            //Initialize both fields to ensure that them are not used
            this.resolverDaemonAddress = null;
            this.resolverDaemonPort=null;
        }
    }

    /**
     * Checks if the hostname is configured. If is not configured, the hostname is set by subnet by default
     */
    private void checkHostname()
    {
        if(isResolveHostname == null)
        {
            //by default, hostname is not desired
            this.isResolveHostname = Boolean.FALSE;
        }

        // if hostname is not configured, check isResolveHostname flag to get by subnet.
        // if hostname is not wanted to be resolved (but resolved client hostname is wanted), set it to empty string by configuration
        if(this.hostname == null)
        {
            //avoid null
            this.hostname = this.isResolveHostname ? subnetAddress.getIpAddres().getHostName() : ConfigUtils.EMPTY_HOSTNAME;
        }
    }
}
