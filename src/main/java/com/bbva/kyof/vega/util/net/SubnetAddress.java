package com.bbva.kyof.vega.util.net;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.regex.Pattern;

/**
 * Wrapper class for a framework subnet address
 *
 * It will extract and store internally the Inet4Address and the mask.
 *
 * The address format contains the IP and optionally the mask separated by "/". Example: 192.168.4.5 or 192.168.3.0/24
 */
public class SubnetAddress
{
    /** 32 bits full ip mask constant */
    public static final int FULL_MASK = 32;

    /** String pattern for valid interfaces */
    private static final String INTERFACE_PATTERN_STRING = new StringBuilder().
            append("^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.").
            append("([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.").
            append("([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.").
            append("([01]?\\d\\d?|2[0-4]\\d|25[0-5])").
            append("(/([0-9]|[1-2][0-9]|3[0-2]))?$").toString();

    /** Compiled pattern */
    private static final Pattern INTERFACE_PATTERN = Pattern.compile(INTERFACE_PATTERN_STRING);

    /** String value of the subnet */
    private final String stringValue;

    /** The mask of the interface in int format, from 0 to 32 indicating the number of bites of the mask */
    private final int mask;

    /** The IP address of the interface */
    private final Inet4Address ipAddres;

    /**
     * Create a new interface address given the String representation.
     *
     * The address format contains the IP and optionally the mask separated by "/". Example: 192.168.4.5 or 192.168.3.0/24
     *
     * @param stringAddress the interface in string format
     * @throws IllegalArgumentException exception thrown if the provided address is not valid
     */
    public SubnetAddress(final String stringAddress)
    {
        // Make sure it matches the valid patter
        if (!matchPattern(stringAddress))
        {
            throw new IllegalArgumentException("Invalid interface address " + stringAddress);
        }

        // Split the address
        final String[] splitAddress = stringAddress.split("/");

        // Extract the IP
        this.ipAddres = InetUtil.getInetAddressFromString(splitAddress[0]);

        // If there is mask, get the mask, if not use full mask of 32 bits.
        if (splitAddress.length > 1)
        {
            this.mask = Integer.parseInt(splitAddress[1]);
        }
        else
        {
            this.mask = FULL_MASK;
        }

        this.stringValue = this.ipAddres.getHostAddress() + "/" + this.mask;
    }

    /**
     * Create a subnet address given the ip and the networkMask
     * @param ipAddress address of the subnet
     * @param networkMask networkMask of the subnet
     */
    public SubnetAddress(final String ipAddress, final int networkMask)
    {
        this(ipAddress + "/" + networkMask);
    }

    /** @return  the interface mask in bites. Number of bites that forms the mask. Ej: 255.255.255.0 = 24 */
    public int getMask()
    {
        return this.mask;
    }

    /** @return the IP address of the interface */
    public Inet4Address getIpAddres()
    {
        return this.ipAddres;
    }

    @Override
    public String toString()
    {
        return this.stringValue;
    }

    @Override
    public boolean equals(final Object target)
    {
        if (this == target)
        {
            return true;
        }
        if (target == null || getClass() != target.getClass())
        {
            return false;
        }

        SubnetAddress that = (SubnetAddress) target;

        return stringValue.equals(that.stringValue);

    }

    @Override
    public int hashCode()
    {
        return stringValue.hashCode();
    }

    /**
     * Check if a given address matches the subnet represented by this instance
     *
     * @param address the address to check
     * @return true if the given address matches the subnet represented by this instance
     */
    public boolean checkIfMatch(final InetAddress address)
    {
        // Check only Ip4 addresses
        if (!(address instanceof Inet4Address))
        {
            return false;
        }

        final int networkMask = -1 << (FULL_MASK - this.mask);
        final int addressInt = InetUtil.convertIpAddressToInt(address);
        final int subnetInt = InetUtil.convertIpAddressToInt(this.ipAddres);

        return (subnetInt & networkMask) == (addressInt & networkMask);
    }

    /**
     * Check if the provided interface address matches the pattern
     *
     * @param interfaceAddress the address to check
     * @return true if it matches the pattern
     */
    private static boolean matchPattern(final String interfaceAddress)
    {
        synchronized (INTERFACE_PATTERN)
        {
            return INTERFACE_PATTERN.matcher(interfaceAddress).matches();
        }
    }
}
