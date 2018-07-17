package com.bbva.kyof.vega.autodiscovery.daemon;

import com.bbva.kyof.vega.autodiscovery.exception.AutodiscException;
import com.bbva.kyof.vega.util.file.FilePathUtil;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.IOException;

/**
 * Parameters for the unicast daemon
 */
@ToString
@Builder
public class DaemonParameters
{
    /** Default stream id for the unicast daemon reception socket */
    public static final int DEFAULT_STREAM_ID = 10;
    /** Default port for the unicast daemon reception socket */
    static final int DEFAULT_PORT = 40300;
    /** Default timeout for the clients before considering them disconnected */
    static final long DEFAULT_CLIENT_TIMEOUT = 10000;

    /** Timeout for client connections, the client will be considered disconnected if no message is received after the timeout period */
    @Getter private Long clientTimeout;
    /** Aeron Driver Type to use */
    @Getter private final AeronDriverType aeronDriverType;
    /** External driver directory */
    @Getter private final String externalDriverDir;
    /** Embedded driver configuration file */
    @Getter private final String embeddedDriverConfigFile;
    /** Ip address for the reception socket */
    @Getter private String ipAddress;
    /** Subnet address, will be used to calculate the proper ip from the machine interfaces */
    private String subnet;
    /** Port for the reception connection */
    @Getter private Integer port;
    /** Subnet address calculated using the given subnet string, if null it will use the ip of the first ip4 interface of the machine */
    @Getter private SubnetAddress subnetAddress;

    /**
     * Complete the null parameters that are optional using the default parameters. It will also validate the parameters and
     * perform any required internal calculation
     *
     * @throws AutodiscException exception thrown if there is any problem in the validation or there are missing parameters
     */
    public void completeAndValidateParameters() throws AutodiscException
    {
        this.checkMediaDriverConfig();

        if (this.port == null)
        {
            this.port = DEFAULT_PORT;
        }

        if (this.clientTimeout == null)
        {
            this.clientTimeout = DEFAULT_CLIENT_TIMEOUT;
        }

        this.validateSubnet();

        this.ipAddress = this.subnetAddress.getIpAddres().getHostAddress();
    }

    /**
     * Check the configuration of the media driver
     *
     * @throws AutodiscException exception thrown if there is a problem checking the configuration
     */
    private void checkMediaDriverConfig() throws AutodiscException
    {
        // If the driver directory is selected, check that it exists and that is a directory
        if (this.aeronDriverType == AeronDriverType.EXTERNAL && this.externalDriverDir != null)
        {
            try
            {
                FilePathUtil.verifyDirPath(this.externalDriverDir);
            }
            catch (final IOException e)
            {
                throw new AutodiscException("Exception trying to verify external driver directory " + this.externalDriverDir, e);
            }
        }

        // If the driver is embedded and the configuration is provided check that the file exists
        if (this.aeronDriverType != AeronDriverType.EXTERNAL && this.embeddedDriverConfigFile != null)
        {
            try
            {
                FilePathUtil.verifyFilePath(this.embeddedDriverConfigFile);
            }
            catch (final IOException e)
            {
                throw new AutodiscException("Exception trying to verify embedded driver configuration file " + this.externalDriverDir, e);
            }
        }
    }

    /**
     * Validate the provided subnet address and calculate a SubnetAddress object from it.
     *
     * If the subnet provided is null, it will look for the first ip4 interface of the machine.
     *
     * The created subnet always use 32 bit mask to avoid multiple matches.
     *
     * @throws AutodiscException exception thrown if there is any problem in the validation or there are missing parameters
     */
    private void validateSubnet() throws AutodiscException
    {
        // If the provided subnet is null, try to find a suitable interface in the machine
        if (this.subnet == null)
        {
            // Find a suitable interface address
            final SubnetAddress defaultSubnet = InetUtil.getDefaultSubnet();

            if (defaultSubnet == null)
            {
                throw new AutodiscException("Cannot find a suitable interface for communication");
            }

            this.subnet = defaultSubnet.toString();
            this.subnetAddress = defaultSubnet;
        }
        else
        {
            // Check that the provided interface address is valid and convert to full mask
            try
            {
                // Find the first interface address that matches the subnet
                final SubnetAddress fullMaskSubnet = InetUtil.validateSubnetAndConvertToFullMask(new SubnetAddress(this.subnet));

                if (fullMaskSubnet == null)
                {
                    throw new AutodiscException("The provided subnet " + this.subnet + " don't match any valid interface address on the machine");
                }

                this.subnetAddress = fullMaskSubnet;
            }
            catch (final IllegalArgumentException e)
            {
                throw new AutodiscException("The provided subnet " + this.subnet + " is malformed", e);
            }
        }
    }

    /** Aeron driver type to use */
    public enum AeronDriverType
    {
        /** External driver */
        EXTERNAL,
        /** Driver embedded in the application */
        EMBEDDED,
        /** Low latency embedded driver in the instance */
        LOWLATENCY_EMBEDDED
    }
}
