package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.config.IConfiguration;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.*;

/**
 * Represent the configuration of a topic template
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "TopicTemplateConfig")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TopicTemplateConfig implements IConfiguration
{
    /** Default min port for multicast */
    static final int DEFAULT_MCAST_MIN_PORT = 35006;

    /** Default max port for multicast */
    static final int DEFAULT_MCAST_MAX_PORT = 35007;

    /** Default min port for unicast */
    static final int DEFAULT_UCAST_MIN_PORT = 35008;

    /** Default max port for unicast */
    static final int DEFAULT_UCAST_MAX_PORT = 35009;

    /** Default max streams per port */
    static final int DEFAULT_STREAMS_PER_PORT = 10;

    /** Default Multicast Address Low */
    static final String DEFAULT_MULTICAST_LOW = "225.0.0.9";

    /** Default Multicast Address High */
    static final String DEFAULT_MULTICAST_HIGH = "225.0.0.20";

    /** Name of the topic template */
    @XmlAttribute(name = "name", required = true)
    @Getter private String name;

    /** Transport media type */
    @XmlElement(name = "transport_type", required = true)
    @Getter private TransportMediaType transportType;
    
    /** Name of the receive poller */
    @XmlElement(name = "rcv_poller", required = true)
    @Getter private String rcvPoller;
    
    /** Minimun port */
    @XmlElement(name = "min_port")
    @Getter private Integer minPort;
    
    /** Maximum port */
    @XmlElement(name = "max_port")
    @Getter private Integer maxPort;
    
    /** Number of streams per port */
    @XmlElement(name = "num_streams_per_port")
    @Getter private Integer numStreamsPerPort;
    
    /** Multicast address low */
    @XmlElement(name = "multicast_address_low")
    @Getter private String multicastAddressLow;
    
    /** Multicast address high */
    @XmlElement(name = "multicast_address_high")
    @Getter private String multicastAddressHigh;
    
    /** Subnet of the topic template */
    @XmlElement(name = "subnet")
    private String subnet;

    /** Subnet address calculated using the provided subnet, or the default if the subnet provided was null.
     *  The subnet address is a full 32 bit mask */
    @XmlTransient
    @Getter private SubnetAddress subnetAddress;

    /** Alternative hostname to publish in unicast mode */
    @XmlElement(name = "unicast_alternative_hostname")
    @Getter private String hostname;

    /** (Optional) Resolve hostname from clients to get ip address */
    @XmlElement(name = "resolve_unicast_hostname")
    @Getter private Boolean isResolveHostname;

    @Override
    public void completeAndValidateConfig() throws VegaException
    {
        this.checkName();
        this.checkRcvPoller();
        this.checkTransportType();

        if (this.transportType == TransportMediaType.UNICAST)
        {
            checkUnicastParameters();
        }
        else if (this.transportType == TransportMediaType.MULTICAST)
        {
            checkMulticastParameters();
        }

        this.checkNumStreams();

        if (transportType != TransportMediaType.IPC)
        {
            this.checkSubnet();
            this.checkHostname();
        }
    }

    /**
     * Check the multicast parameters
     */
    private void checkMulticastParameters() throws VegaException
    {
        // Set default ports if null
        if (this.minPort == null)
        {
            this.minPort = DEFAULT_MCAST_MIN_PORT;
        }

        if (this.maxPort == null)
        {
            this.maxPort = DEFAULT_MCAST_MAX_PORT;
        }

        // Validate the port range
        ConfigUtils.validatePortRange(this.minPort, this.maxPort);

        // Set default multicast address if null
        if (this.multicastAddressLow == null)
        {
            this.multicastAddressLow = DEFAULT_MULTICAST_LOW;
        }

        if (this.multicastAddressHigh == null)
        {
            this.multicastAddressHigh = DEFAULT_MULTICAST_HIGH;
        }

        // Validate the address range
        ConfigUtils.validateMulticastAddressRange(this.multicastAddressLow, this.multicastAddressHigh);
    }

    /**
     * Check the unicast parameters
     */
    private void checkUnicastParameters() throws VegaException
    {
        if (this.minPort == null)
        {
            this.minPort = DEFAULT_UCAST_MIN_PORT;
        }

        if (this.maxPort == null)
        {
            this.maxPort = DEFAULT_UCAST_MAX_PORT;
        }

        ConfigUtils.validatePortRange(this.minPort, this.maxPort);
    }

    /**
     * Check the number of streams
     */
    private void checkNumStreams()
    {
        if (this.numStreamsPerPort == null)
        {
            this.numStreamsPerPort = DEFAULT_STREAMS_PER_PORT;
        }
    }

    /**
     * Check the transport type, it has to be present
     */
    private void checkTransportType() throws VegaException
    {
        if (this.transportType == null)
        {
            throw new VegaException("Missing parameter transport type in topic template configuration");
        }
    }

    /**
     * Check the template name, it has to be present
     */
    private void checkName() throws VegaException
    {
        if (this.name == null)
        {
            throw new VegaException("Missing parameter name in topic template configuration");
        }
    }

    /**
     * Check the receive poller name, it has to be present
     */
    private void checkRcvPoller() throws VegaException
    {
        if (this.rcvPoller == null)
        {
            throw new VegaException("Missing parameter rcvPoller in topic template configuration");
        }
    }

    /**
     * Check and calculate the subnet address
     */
    private void checkSubnet() throws VegaException
    {
        this.subnetAddress = ConfigUtils.getFullMaskSubnetFromStringOrDefault(this.subnet);
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
            this.hostname = this.isResolveHostname ? subnetAddress.getIpAddres().getCanonicalHostName() : ConfigUtils.EMPTY_HOSTNAME;
        }
    }
}
