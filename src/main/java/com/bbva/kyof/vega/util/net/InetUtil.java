package com.bbva.kyof.vega.util.net;

import lombok.extern.slf4j.Slf4j;

import java.net.*;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class to obtain network addresses and perform subnet filters
 */
@Slf4j
public final class InetUtil
{
    /** Stores all the ip4, non loopback, up interface addresses */
    private static List<InterfaceAddress> interfaceAddresses = null;

    /** Lock to access the static stored interface addresses */
    private static final Object STORED_ADDRESSES_LOCK = new Object();

    /** Private constructor to avoid utility class instantiation */
    private InetUtil()
    {
        // Nothing to do here
    }

    /**
     * Return the Inet4Address corresponding to the given ip addres in format "192.168.0.1".
     * @param ipString the Ip address
     * @return the Inet4Address
     */
    public static Inet4Address getInetAddressFromString(final String ipString)
    {
        final InetAddress result;
        try
        {
            result = InetAddress.getByName(ipString);
        }
        catch (final UnknownHostException e)
        {
            throw new IllegalArgumentException("The provided address is not a valid ip", e);
        }

        if (!(result instanceof Inet4Address))
        {
            throw new IllegalArgumentException("The provided address is not a valid ip4 address");
        }

        return (Inet4Address)result;
    }

    /**
     * Converts a given address to int
     * @param address to convert
     * @return the converted address
     */
    public static int convertIpAddressToInt(final InetAddress address)
    {
        int result = 0;
        for (int i = 0; i < address.getAddress().length; i++)
        {
            result <<= 8;
            result |= address.getAddress()[i] & 0xff;
        }
        return result;
    }

    /**
     * Converts a given address in the form 255.255.255.255 to int
     * @param address the address to convert
     * @return the converted address
     */
    public static int convertIpAddressToInt(final String address)
    {
        // Convert to InetAddress
        final InetAddress inetAddress = InetUtil.getInetAddressFromString(address);

        return InetUtil.convertIpAddressToInt(inetAddress);
    }

    /**
     * Converts a given int address into an InetAddress
     * @param address to convert
     * @return the converted address
     */
    public static InetAddress convertIntToInetAddress(final int address)
    {
        final byte[] addressInBytes =  new byte[] {
                (byte)((address >>> 24) & 0xff),
                (byte)((address >>> 16) & 0xff),
                (byte)((address >>>  8) & 0xff),
                (byte)(address & 0xff)
        };

        try
        {
            return InetAddress.getByAddress(addressInBytes);
        }
        catch (final UnknownHostException e)
        {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Converts a given int address into an String address
     * @param address to convert
     * @return the converted address in 255.255.255.255 format
     */
    public static String convertIntToIpAddress(final int address)
    {
        return InetUtil.convertIntToInetAddress(address).getHostAddress();
    }

    /**
     *
     * @param ipAddress given IP address
     * @return true if the given IP address is a valid ip4 multicast address
     */
    public static boolean isValidMulticastAddress(final String ipAddress)
    {
        try
        {
            final InetAddress address = InetAddress.getByName(ipAddress);
            return address instanceof Inet4Address && address.isMulticastAddress();
        }
        catch (final UnknownHostException e)
        {
            log.error("Unexpected exception checking valid multicast address", e);
            return false;
        }
    }


    /**
     * Test if a given Unicast address is valid
     *
     * @param ipAddress to test
     * @return true if the given IP address is a valid ip4 unicast address
     */
    public static boolean isValidUnicastAddress(final String ipAddress)
    {
        try
        {
            final InetAddress address = InetAddress.getByName(ipAddress);
            return address instanceof Inet4Address && !address.isMulticastAddress();
        }
        catch (final UnknownHostException e)
        {
            log.error("Unexpected exception checking valid unicast address", e);
            return false;
        }
    }

    /**
     * Tests if a given port is a valid port number
     *
     * @param port port to test
     * @return true if the port is a valid port number
     */
    public static boolean isValidPortNumber(final int port)
    {
        return port > 0 && port < 65536;
    }

    /**
     * Get all the interface addresses of the machine, it will perform a lazy search the first time this method is called.
     *
     * @return the list of addresses. It can be empty if no interface is valid or there has been a problem.
     */
    public static List<InterfaceAddress> getAllInterfaceAddresses()
    {
        synchronized (STORED_ADDRESSES_LOCK)
        {
            if (interfaceAddresses == null)
            {
                interfaceAddresses = InetUtil.findAllInterfaceAddresses();
            }

            return interfaceAddresses;
        }
    }

    /**
     * Get the default interface of the machine, it will perform a lazy search the first time this method is called.
     *
     * @return the default interface found. Null if no interface or if there has been an error.
     */
    public static InterfaceAddress getDefaultInterfaceAddress()
    {
        final List<InterfaceAddress> allAddresses = InetUtil.getAllInterfaceAddresses();

        if (allAddresses.isEmpty())
        {
            return null;
        }
        else
        {
            return allAddresses.get(0);
        }
    }

    /** @return a created SubnetAddress using the default interface address. Return null if there is no default */
    public static SubnetAddress getDefaultSubnet()
    {
        // Find the default address
        final InterfaceAddress defaultAddress = InetUtil.getDefaultInterfaceAddress();

        if (defaultAddress == null)
        {
            return null;
        }

        // Create the subnet using the interface address and the 32 bit full mask
        return new SubnetAddress(defaultAddress.getAddress().getHostAddress(), SubnetAddress.FULL_MASK);
    }

    /**
     * Return the first ip 4 interface address found in the machine.
     * It will also check that the interface is in the given subnet.
     *
     * @param subnetAddress subnet address
     * @return the first interface address that matches the given subnet, null if none match
     */
    public static InterfaceAddress findFirstInterfaceAddressForSubnet(final SubnetAddress subnetAddress)
    {
        for (InterfaceAddress interfaceAddress : InetUtil.getAllInterfaceAddresses())
        {
            if (subnetAddress.checkIfMatch(interfaceAddress.getAddress()))
            {
                return interfaceAddress;
            }
        }

        return null;
    }

    /**
     * Check if the provided subnet match any of the valid interfaces on the machine.
     * If there is a match it will return the full mask subnet for the match interface.
     *
     * @param subnetAddress subnet address
     * @return the subnet address with full mask of the interface that match the subnet
     */
    public static SubnetAddress validateSubnetAndConvertToFullMask(final SubnetAddress subnetAddress)
    {
        final InterfaceAddress matchInterface = InetUtil.findFirstInterfaceAddressForSubnet(subnetAddress);

        if (matchInterface == null)
        {
            return null;
        }
        else
        {
            return new SubnetAddress(matchInterface.getAddress().getHostAddress(), SubnetAddress.FULL_MASK);
        }
    }

    /**
     * Return all ip 4 interface address.
     *
     * @return all interface address that matches the given subnet
     */
    private static List<InterfaceAddress> findAllInterfaceAddresses()
    {
        // Result with all the found inet addresses, it may end empty
        final List<InterfaceAddress> result = new LinkedList<>();

        try
        {
            // Stream all the network interfaces
            Collections.list(NetworkInterface.getNetworkInterfaces()).stream().
                    // Sort by interface index
                    sorted((interface1, interface2) -> Integer.valueOf(interface1.getIndex()).compareTo(interface2.getIndex())).
                    // Filter only valid interface addresses (active and not lookup)
                    filter(InetUtil::isInterfaceValidAndUp).
                    // Get all addresses in the interfaces that are ip4
                    forEach(networkInterface -> result.addAll(
                            filterIp4Addresses(networkInterface)));
        }
        catch (final SocketException e)
        {
            log.error("Unexpected exception accessing network interfaces information", e);

            // Clear in order to return an empty list
            result.clear();
        }

        return result;
    }

    /**
     * Return all ip 4 addresses of the given interface.
     *
     * @param networkInterface the network interface containing the addresses to check
     */
    private static List<InterfaceAddress> filterIp4Addresses(final NetworkInterface networkInterface)
    {
        return networkInterface.getInterfaceAddresses().stream().
                filter(address -> address.getAddress() instanceof Inet4Address).collect(Collectors.toList());
    }

    /**
     * Return true if the interface is not loopback and is up
     */
    private static boolean isInterfaceValidAndUp(final NetworkInterface networkInterface)
    {
        try
        {
            return !networkInterface.isLoopback() && networkInterface.isUp();
        }
        catch (final SocketException e)
        {
            log.warn("Unexpected exception checking interface availability", e);
            return false;
        }
    }
}
