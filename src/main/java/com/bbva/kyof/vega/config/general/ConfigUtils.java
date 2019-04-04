package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;

/**
 * Contains utility methods to validate the configuration read
 */
final class ConfigUtils
{
    /** Private constructor to avoid instantiation */
    private ConfigUtils()
    {
        // Nothing to do
    }

    /**
     * Validate the given port range, it will if the ports are valid and if the range is valid
     * @param minPort minimum port in the range
     * @param maxPort maximum port in the range
     * @throws VegaException exception thrown if the range is not valid
     */
    static void validatePortRange(final int minPort, final int maxPort) throws VegaException
    {
        validatePortNumber(minPort);
        validatePortNumber(maxPort);

        // Make sure about range
        if (maxPort < minPort)
        {
            throw new VegaException(String.format("The max port %d cannot be smaller than min port %d", maxPort, minPort));
        }
    }

    /**
     * Validate the given port
     *
     * @param port the port to validate
     * @throws VegaException exception thrown if the port is not valid
     */
    static void validatePortNumber(final int port) throws VegaException
    {
        if (!InetUtil.isValidPortNumber(port))
        {
            throw new VegaException(String.format("The port %d is not a valid port number", port));
        }
    }

    /**
     * Validate the given unicast address
     * @param address the address to validate
     * @throws VegaException exception thrown if the address is invalid
     */
    static void validateUnicastAddress(final String address) throws VegaException
    {
        if (!InetUtil.isValidUnicastAddress(address))
        {
            throw new VegaException(String.format("Invalid unicast address %s", address));
        }
    }

    /**
     * Validate the given multicast address
     * @param address the address to validate
     * @throws VegaException exception thrown if the address is invalid
     */
    static void validateMulticastAddress(final String address) throws VegaException
    {
        if (!InetUtil.isValidMulticastAddress(address))
        {
            throw new VegaException(String.format("Invalid multicast address %s", address));
        }

        final int intAddress = InetUtil.convertIpAddressToInt(address);

        // It has to be an ODD address
        if (intAddress % 2 == 0)
        {
            throw new VegaException(String.format("The auto-discovery multicast address has to be odd. Provided address %s is invalid", address));
        }
    }

    /**
     * Validate the given multicast controll address
     * @param address the address to validate
     * @throws VegaException exception thrown if the address is invalid
     */
    static void validateControlMulticastAddress(final String address) throws VegaException
    {
        if (!InetUtil.isValidMulticastAddress(address))
        {
            throw new VegaException(String.format("Invalid multicast address %s", address));
        }

        final int intAddress = InetUtil.convertIpAddressToInt(address);

        // It has to be an ODD address
        if (intAddress % 2 != 0)
        {
            throw new VegaException(String.format("The auto-discovery control multicast address has to be even. Provided address %s is invalid", address));
        }
    }

    /**
     * Validate teh given multicast address range. It will check both address and also the range
     * @param minRange minimum multicast address in the range
     * @param maxRange maximum multicast address in the range
     * @throws VegaException exception thrown if there is a problem
     */
    static void validateMulticastAddressRange(final String minRange, final String maxRange) throws VegaException
    {
        validateMulticastAddress(minRange);
        validateControlMulticastAddress(maxRange);

        // Convert ip addresses to int
        final int minIp = InetUtil.convertIpAddressToInt(minRange);
        final int maxIp = InetUtil.convertIpAddressToInt(maxRange);

        // Check that the addresses range is correct
        if (minIp >= maxIp)
        {
            throw new VegaException(String.format("The multicast addres high %s cannot be lower than the multicast address low %s", maxRange, minRange));
        }
    }

    /**
     * Given a subnet in String format it returns a SubnetAddress object with full mask. <p>
     *
     * The SubnetAddress returned will always match the IP and FullMask of the fist interface address in the machine that match the
     * provided subnet. <p>
     *
     * If the provided subnet is null, it will use the first interface address on the machine. <p>
     *
     * @param subnet subnet address in the form IP/Mask, ej: 192.168.1.0/24. Null to use the address of the first interface.
     * @return the subnet
     * @throws VegaException if any error happens
     */
    static SubnetAddress getFullMaskSubnetFromStringOrDefault(final String subnet) throws VegaException
    {
        // If subnet is null, find the default subnet
        if (subnet == null)
        {
            // Find a suitable interface address
            final SubnetAddress defaultSubnet = InetUtil.getDefaultSubnet();

            if (defaultSubnet == null)
            {
                throw new VegaException("Cannot find a suitable interface for multicast autodiscovery");
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
                    throw new VegaException("The provided subnet " + subnet + " don't match any valid interface address on the machine");
                }

                return fullMaskSubnet;
            }
            catch (final IllegalArgumentException e)
            {
                throw new VegaException("The provided subnet " + subnet + " is malformed", e);
            }
        }
    }
}
