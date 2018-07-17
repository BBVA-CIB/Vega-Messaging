package com.bbva.kyof.vega.util.net;

/**
 * Class that helps with the process of creating Aeron Channel Strings
 */
public final class AeronChannelHelper
{
    /** Represents an Aeron IPC channel*/
    private static final String CHANNEL_IPC = "aeron:ipc";
    
    /** Reliability of Aeron channel */
    private static boolean reliable = true;

    /** Private constructor to avoid instantiation of utility class */
    private AeronChannelHelper()
    {
        // Nothing to do
    }

    /** @return the string with the IPC channel */
    public static String createIpcChannelString()
    {
        return CHANNEL_IPC;
    }

    /**
     * Create a string that represents an Aeron multicast channel
     *
     * @param ipAddress Ip address of the channel
     * @param port the port of the channel
     * @param subnetAddress wrapper for a framework subnet address
     * @return the string with the multicast channel
     */
    public static String createMulticastChannelString(final int ipAddress,
                                                      final int port,
                                                      final SubnetAddress subnetAddress)
    {
        return createMulticastChannelString(InetUtil.convertIntToIpAddress(ipAddress), port, subnetAddress);
    }

    /**
     * Create a string that represents an Aeron multicast channel
     *
     * @param ipAddress Ip address of the channel
     * @param port the port of the channel
     * @param subnetAddress wrapper for a framework subnet address
     * @return the string with the multicast channel
     */
    public static String createMulticastChannelString(final String ipAddress,
                                                      final int port,
                                                      final SubnetAddress subnetAddress)
    {
        if (reliable)
        {
            return String.format("aeron:udp?endpoint=%s:%d|interface=%s", ipAddress, port, subnetAddress.toString());
        }
        else
        {
            return String.format("aeron:udp?endpoint=%s:%d|interface=%s|reliable=false", ipAddress, port, subnetAddress.toString());
        }

    }

    /**
     * Create a string that represents an Aeron unicast channel
     *
     * @param ipAddress Ip address of the channel
     * @param port the port of the channel
     * @param subnetAddress wrapper for a framework subnet address
     * @return the string with the unicast channel
     */
    public static String createUnicastChannelString(final int ipAddress, final Integer port, final SubnetAddress subnetAddress)
    {
        return createUnicastChannelString(InetUtil.convertIntToIpAddress(ipAddress), port, subnetAddress);
    }

    /**
     * Create a string that represents an Aeron unicast channel
     *
     * @param ipAddress Ip address of the channel
     * @param port the port of the channel
     * @param subnetAddress wrapper for a framework subnet address
     * @return the string with the unicast channel
     */
    public static String createUnicastChannelString(final String ipAddress, final Integer port, final SubnetAddress subnetAddress)
    {
        if (reliable)
        {
            return String.format("aeron:udp?endpoint=%s:%d|interface=%s", ipAddress, port, subnetAddress.toString());
        }
        else
        {
            return String.format("aeron:udp?endpoint=%s:%d|interface=%s|reliable=false", ipAddress, port, subnetAddress.toString());
        }
    }

    /**
     * Selects an IP address from a range of IPs. The selected address is always ODD and the next EVEN address is reserved for
     * control messages. This means that it will return an ODD address in the range that allows the next address to be in the range
     * as well. <p>
     *
     * The function is deterministic, it always return the same address for the same topic and range.
     *
     * @param topicName the name of the topic
     * @param minIp the minimun value of the range
     * @param maxIp the maximum value of the range
     * @return the IP selected
     */
    public static String selectMcastIpFromRange(final String topicName, final String minIp, final String maxIp)
    {
        return selectMcastIpFromRange(topicName.hashCode(), minIp, maxIp);
    }

    /**
     * Selects an IP address from a range of IPs. The selected address is always ODD and the next EVEN address is reserved for
     * control messages. This means that it will return an ODD address in the range that allows the next address to be in the range
     * as well.
     *
     * The function is deterministic, it always return the same address for the same hash and range.
     *
     * @param hash the hash function to select the Ip address
     * @param minIp the minimun value of the range, should be ODD
     * @param maxIp the maximum value of the range, should be EVEN
     * @return the IP selected
     */
    private static String selectMcastIpFromRange(final int hash, final String minIp, final String maxIp)
    {
        // Convert the ip addresses to a 32 bit integer
        final int intMinIp = InetUtil.convertIpAddressToInt(minIp);
        final int intMaxIp = InetUtil.convertIpAddressToInt(maxIp);

        // Security check, min Ip should be ODD
        if (intMinIp % 2 == 0)
        {
            throw new IllegalArgumentException(String.format("The min multicast ip in range should be odd, found %s", intMinIp));
        }

        // Security check, max Ip should be Even
        if (intMaxIp % 2 != 0)
        {
            throw new IllegalArgumentException(String.format("The max multicast ip in range should be even, found %s", intMaxIp));
        }

        // Find the number of ODD ip's in the range
        final int oddIpsInRange = (intMaxIp - intMinIp + 1) / 2;

        // Select the ODD in the range to use
        final int intIpToUse = intMinIp + Math.abs(hash % oddIpsInRange) * 2;

        // Convert the Ip back to String
        return InetUtil.convertIntToIpAddress(intIpToUse);
    }

    /**
     * Selects a port from a range of ports
     *
     * @param topicName the name of the topic
     * @param minPort the minimun value of the range
     * @param maxPort the maximum value of the range
     * @return the port selected
     */
    public static int selectPortFromRange(final String topicName, final int minPort, final int maxPort)
    {
        return selectPortFromRange(topicName.hashCode(), minPort, maxPort);
    }

    /**
     * Selects a port from a range of ports
     *
     * @param hash the hash function to select the port
     * @param minPort the minimun value of the range
     * @param maxPort the maximum value of the range
     * @return the port selected
     */
    public static int selectPortFromRange(final int hash, final int minPort, final int maxPort)
    {
        // Get the range of ports from configuration
        final int numPorts = maxPort - minPort + 1;

        // Find the specific port to use inside teh range
        return Math.abs(hash % numPorts) + minPort;
    }

    /**
     * Selects a stream Id from a range of streams
     *
     * @param topicName the name of the topic
     * @param maxStreams the maximum value of the range
     * @return the stream Id selected
     */
    public static int selectStreamFromRange(final String topicName, final int maxStreams)
    {
        return selectStreamFromRange(topicName.hashCode(), maxStreams);
    }

    /**
     * Selects a stream Id from a range of streams
     *
     * @param hash the hash function to select the stream Id
     * @param maxStreams the maximum value of the range
     * @return the stream Id selected
     */
    public static int selectStreamFromRange(final int hash, final int maxStreams)
    {
        return Math.abs(hash % maxStreams) + 2;
    }
}
