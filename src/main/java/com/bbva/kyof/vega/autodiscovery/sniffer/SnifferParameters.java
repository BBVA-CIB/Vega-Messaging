package com.bbva.kyof.vega.autodiscovery.sniffer;

import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import javax.xml.bind.annotation.XmlTransient;

/**
 * Contains the configuration for the multicast sniffer.
 * <p>
 * For the optional parameters a default value will be provided if the optional parameter is missing.
 */
@ToString
@Builder
public class SnifferParameters
{
    /**
     * Default stream id
     */
    public static final int DEFAULT_STREAM_ID = 10;
    /**
     * Default timeout of the instances
     */
    static final long DEFAULT_CLIENT_TIMEOUT = 10000;
    /**
     * Default multicast address
     */
    static final String DEFAULT_MULTICAST_ADDRESS = "225.0.0.1";
    /**
     * Default multicast port
     */
    static final int DEFAULT_MULTICAST_PORT = 35000;

    /**
     * Timeout for connections, the element will be considered disconnected if no message is received after the timeout period
     */
    @Getter
    private Long timeout;
    /**
     * Ip address for the reception socket
     */
    @Getter
    private String ipAddress;
    /**
     * Subnet address, will be used to calculate the proper ip from the machine interfaces
     */
    private String subnet;
    /**
     * Port for the reception connection
     */
    @Getter
    private Integer port;
    /**
     * SubnetAddress to use, obtained from the provided multicast interface or the default interface
     */
    @XmlTransient
    @Getter
    private SubnetAddress subnetAddress;

    /**
     * Complete the null parameters that are optional using the default parameters. It will also validate the parameters and
     * perform any required internal calculation
     *
     * @throws SnifferException exception thrown if there is any problem in the validation or there are missing parameters
     */
    void validateSnifferParameters() throws SnifferException
    {

        this.validateMulticastConfig();
        this.checkSubnet();
        if (this.timeout == null)
        {
            this.timeout = DEFAULT_CLIENT_TIMEOUT;
        }

    }

    /**
     * Validate parameters for multicast configuration
     *
     * @throws SnifferException if any parameter is not valid
     */
    private void validateMulticastConfig() throws SnifferException
    {
        // Check the multicast address
        if (this.ipAddress == null)
        {
            this.ipAddress = DEFAULT_MULTICAST_ADDRESS;
        }

        validateMulticastAddress(this.ipAddress);

        // Check the multicast port
        if (this.port == null)
        {
            this.port = DEFAULT_MULTICAST_PORT;
        }
        validatePortNumber(this.port);
    }

    /**
     * Checks and assigns a correct value to the subnet
     *
     * @throws SnifferException if any error happens checking the subnet
     */
    private void checkSubnet() throws SnifferException
    {
        // Create the subnet address
        this.subnetAddress = getFullMaskSubnetFromStringOrDefault(this.subnet);
    }

    /**
     * Validate the given port
     *
     * @param port the port to validate
     * @throws SnifferException exception thrown if the port is not valid
     */
    static void validatePortNumber(final int port) throws SnifferException
    {
        if (!InetUtil.isValidPortNumber(port))
        {
            throw new SnifferException(String.format("The port %d is not a valid port number", port));
        }
    }

    /**
     * Validate the given multicast address
     *
     * @param address the address to validate
     * @throws SnifferException exception thrown if the address is invalid
     */
    static void validateMulticastAddress(final String address) throws SnifferException
    {
        if (!InetUtil.isValidMulticastAddress(address))
        {
            throw new SnifferException(String.format("Invalid multicast address %s", address));
        }

        final int intAddress = InetUtil.convertIpAddressToInt(address);

        // It has to be an ODD address
        if (intAddress % 2 == 0)
        {
            throw new SnifferException(String.format("The sniffer multicast address has to be odd. Provided address %s is invalid", address));
        }
    }

    /**
     * Given a subnet in String format it returns a SubnetAddress object with full mask. <p>
     * <p>
     * The SubnetAddress returned will always match the IP and FullMask of the fist interface address in the machine that match the
     * provided subnet. <p>
     * <p>
     * If the provided subnet is null, it will use the first interface address on the machine. <p>
     *
     * @param subnet subnet address in the form IP/Mask, ej: 192.168.1.0/24. Null to use the address of the first interface.
     * @return the subnet
     * @throws SnifferException if any error happens
     */
    static SubnetAddress getFullMaskSubnetFromStringOrDefault(final String subnet) throws SnifferException
    {
        // If subnet is null, find the default subnet
        if (subnet == null)
        {
            // Find a suitable interface address
            final SubnetAddress defaultSubnet = InetUtil.getDefaultSubnet();

            if (defaultSubnet == null)
            {
                throw new SnifferException("Cannot find a suitable interface for multicast autodiscovery");
            }
            else
            {
                return defaultSubnet;
            }
        }
        else
        {
            // Check that the provided interface address is valid and convert to full mask
            try
            {
                // Find the first interface address that matches the subnet
                final SubnetAddress fullMaskSubnet = InetUtil.validateSubnetAndConvertToFullMask(new SubnetAddress(subnet));

                if (fullMaskSubnet == null)
                {
                    throw new SnifferException("The provided subnet " + subnet + " don't match any valid interface address on the machine");
                }

                return fullMaskSubnet;
            }
            catch (final IllegalArgumentException e)
            {
                throw new SnifferException("The provided subnet " + subnet + " is malformed", e);
            }
        }
    }
}
