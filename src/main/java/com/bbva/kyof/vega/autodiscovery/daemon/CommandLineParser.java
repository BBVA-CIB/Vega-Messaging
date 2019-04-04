package com.bbva.kyof.vega.autodiscovery.daemon;

import com.bbva.kyof.vega.autodiscovery.exception.AutodiscException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;

/**
 * Parser for the command line arguments of the Daemon
 */
@Slf4j
class CommandLineParser
{
    /** Port option for the unicast connection */
    private final Option commPortOption    = new Option("p", "port", true, "(Optional) Communications port. Default Value: " + DaemonParameters.DEFAULT_PORT);
    /** Subnet option for the unicast connection */
    private final Option subnetOption 	   = new Option("sn", "subnet", true, "(Optional) Subnet with format Ip/Mask. Ej: 192.168.1.0/24. By default will use the first IP4 interface");
    /** External driver directory path */
    private final Option externalDriverDir 	   = new Option("exdd", "driverDirectory", true, "(Optional) External driver directory path, use only if no embedded driver is used and driver is not using the default directory");
    /** True to use an embedded aeron driver */
    private final Option embeddedDriverOption  = new Option("ed", "embeddedDriver", false, "(Optional) Set to true to use an embedded driver");
    /** External driver directory path */
    private final Option embeddedDriverConfigFileOption  = new Option("edcf", "embeddedDriverConfigFile", true, "(Optional) Embedded driver configuration file");
    /** Timeout for client adverts before considering them disconnected */
    private final Option clientTimeoutOption  = new Option("ct", "clientTimeout", true, "(Optional) Client timeout in milliseconds. Default value: " + DaemonParameters.DEFAULT_CLIENT_TIMEOUT);

    /** The command line with all the values parsed */
    private CommandLine commandLine = null;

    /**
     * Add the common command line options to the options list, it will be used to parse the command line later
     * @param options options object to add the common options to
     */
    private void addCommonCommandLineOptions(final Options options)
    {
        options.addOption(this.commPortOption);
        options.addOption(this.subnetOption);
        options.addOption(this.clientTimeoutOption);
        options.addOption(this.embeddedDriverOption);
        options.addOption(this.externalDriverDir);
        options.addOption(this.embeddedDriverConfigFileOption);
    }

    /**
     * Add the command line options including the specific parser ones, parse the command line and validate the found options
     * @param args command line arguments
     *
     * @throws AutodiscException exception thrown if there is a problem reading or validating the command line
     */
    DaemonParameters parseCommandLine(final String[] args) throws AutodiscException
    {
        log.info("Parsing command line arguments: {}", (Object)args);

        // Create the options
        final Options commandLineOptions = new Options();

        // Add the common command line options
        this.addCommonCommandLineOptions(commandLineOptions);

        // Parse the command line
        final org.apache.commons.cli.CommandLineParser commandLineParser = new PosixParser();
        try
        {
            this.commandLine = commandLineParser.parse(commandLineOptions, args);
        }
        catch (final ParseException e)
        {
            log.error("Error parsing command line arguments", e);
            throw new AutodiscException(e);
        }

        // Verify the command line arguments, both the common and the additional ones
        return this.parseAndValidateCommandLine();
    }

    /**
     * Validate the input command line arguments parsed from the command line
     *
     * @throws AutodiscException exception thrown if the validation is not correct
     */
    private DaemonParameters parseAndValidateCommandLine() throws AutodiscException
    {
        final String subnetAddress = this.getCmdStringOption(this.subnetOption);
        final String externalDriverDirString = this.getCmdStringOption(this.externalDriverDir);
        final Integer port = this.getCmdIntegerOption(this.commPortOption);
        final Long clientTimeout = this.getCmdLongOption(this.clientTimeoutOption);
        final boolean useEmbeddedDriver = this.hasOption(this.embeddedDriverOption);
        final String embeddedDriverConfigString = this.getCmdStringOption(this.embeddedDriverConfigFileOption);

        DaemonParameters.AeronDriverType aeronDriverType;

        if (useEmbeddedDriver)
        {
            aeronDriverType = DaemonParameters.AeronDriverType.EMBEDDED;
        }
        else
        {
            aeronDriverType = DaemonParameters.AeronDriverType.EXTERNAL;
        }

        // Validate the parameters
        final DaemonParameters result = DaemonParameters.builder().
                subnet(subnetAddress).
                port(port).
                clientTimeout(clientTimeout).
                aeronDriverType(aeronDriverType).
                externalDriverDir(externalDriverDirString).
                embeddedDriverConfigFile(embeddedDriverConfigString).build();

        result.completeAndValidateParameters();

        return result;
    }

    /**
     * Return true if the given option is pressent in the command line
     * @param option the option to check
     * @return true if the option is present
     */
    private boolean hasOption(final Option option)
    {
        return commandLine.hasOption(option.getOpt());
    }

    /**
     * Return the String option value from the command line given the representing option
     *
     * @param option the representing option of the command line
     * @return the value of the option, exception if unsettled
     */
    private String getCmdStringOption(final Option option)
    {
        if(commandLine.hasOption(option.getOpt()))
        {
            return commandLine.getOptionValue(option.getOpt()).trim();
        }

        return null;
    }

    /**
     * Return the Integer option value from the command line given the representing option
     *
     * @param option the representing option of the command line
     * @return the value of the option, exception if unsettled
     */
    private Integer getCmdIntegerOption(final Option option)
    {
        if(commandLine.hasOption(option.getOpt()))
        {
            return Integer.parseInt(commandLine.getOptionValue(option.getOpt()).trim());
        }

        return null;
    }

    /**
     * Return the Long option value from the command line given the representing option
     *
     * @param option the representing option of the command line
     * @return the value of the option, exception if unsettled
     */
    private Long getCmdLongOption(final Option option)
    {
        if(commandLine.hasOption(option.getOpt()))
        {
            return Long.parseLong(commandLine.getOptionValue(option.getOpt()).trim());
        }

        return null;
    }
}
