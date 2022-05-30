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
 * Configuration for responses, contains the parameters required to create the response unicast reception socket
 * and assign the right poller to get incoming responses.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ResponsesConfig")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ResponsesConfig implements IConfiguration
{
    /** Default min port for multicast */
    static final int DEFAULT_MIN_PORT = 35004;

    /** Default max port for multicast */
    static final int DEFAULT_MAX_PORT = 35005;

    /** Default max streams per port */
    static final int DEFAULT_NUM_STREAMS = 10;

    /** (Optional) Minimum value of port range for responses socket */
    @XmlElement(name = "min_port")
    @Getter private Integer minPort;
    
    /** (Optional) Maximun value of port range for responses socket*/
    @XmlElement(name = "max_port")
    @Getter private Integer maxPort;
    
    /** (Optional) Number of streams */
    @XmlElement(name = "num_streams")
    @Getter private Integer numStreams;
    
    /** Name of the receive poller */
    @XmlElement(name = "rcv_poller")
    @Getter private String rcvPoller;
    
    /** (Optional) Subnet address */
    @XmlElement(name = "subnet")
    private String subnet;

    /** Subnet address calculated using the provided subnet, or the default if the subnet provided was null.
     *  The subnet address is a full 32 bit mask */
    @XmlTransient
    @Getter private SubnetAddress subnetAddress;

    /**  Alternative hostname for Unicast*/
    @XmlElement(name = "unicast_alternative_hostname")
    @Getter private String hostname;

    /** (Optional) Resolve hostname from clients to get ip address */
    @XmlElement(name = "resolve_unicast_hostname")
    @Getter private Boolean isResolveHostname;

    @Override
    public void completeAndValidateConfig() throws VegaException
    {
        this.checkPorts();
        this.checkNumStreams();
        this.checkSubnet();
        this.checkRcvPoller();
        this.checkHostname();
    }

    /** Check name of the receiver poller */
    private void checkRcvPoller() throws VegaException
    {
        if (this.rcvPoller == null)
        {
            throw new VegaException("Missing parameter rcvPoller in responses configuration");
        }
    }

    /** Check the ports */
    private void checkPorts() throws VegaException
    {
        // Check reception port range
        if (this.minPort == null)
        {
            this.minPort = DEFAULT_MIN_PORT;
        }

        // Check reception port range
        if (this.maxPort == null)
        {
            this.maxPort = DEFAULT_MAX_PORT;
        }

        // Validate the port range
        ConfigUtils.validatePortRange(this.minPort, this.maxPort);
    }

    /** Check the number of streams */
    private void checkNumStreams()
    {
        if (this.numStreams == null)
        {
            this.numStreams = DEFAULT_NUM_STREAMS;
        }
    }

    /** Check the subnet */
    private void checkSubnet() throws VegaException
    {
        // Create the subnet address
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
            this.hostname = this.isResolveHostname ? subnetAddress.getIpAddres().getHostName() : ConfigUtils.EMPTY_HOSTNAME;
        }
    }
}
