package com.bbva.kyof.vega.autodiscovery.sniffer;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * Parser for the command line arguments of the sniffer
 */
@Slf4j
class SnifferCommandLineParser
{
    /**
     * Port option for the multicast connection
     */
    private final Option commPortOption = new Option("p", "port", true, "(Optional) Communications port. Default Value: " + SnifferParameters.DEFAULT_MULTICAST_PORT);
    /**
     * Subnet option for the multicast connection
     */
    private final Option subnetOption = new Option("sn", "subnet", true, "(Optional) Subnet with format Ip/Mask. Ej: 15.30.174.241/32.");
    /**
     * IP option for the multicast connection
     */
    private final Option ipAddressOption = new Option("ip", "ipAddress", true, "(Optional) Ip Address. Default value: " + SnifferParameters.DEFAULT_MULTICAST_ADDRESS);

    /**
     * Timeout for client adverts before considering them disconnected
     */
    private final Option timeoutOption = new Option("t", "timeout", true, "(Optional) Timeout in milliseconds. Default value: " + SnifferParameters.DEFAULT_CLIENT_TIMEOUT);

    /**
     * The command line with all the values parsed
     */
    private CommandLine commandLine = null;

    /**
     * Add the common command line options to the options list, it will be used to parse the command line later
     *
     * @param options options object to add the common options to
     */
    private void addCommonCommandLineOptions(final Options options)
    {
        options.addOption(this.commPortOption);
        options.addOption(this.subnetOption);
        options.addOption(this.ipAddressOption);
        options.addOption(this.timeoutOption);
    }

    /**
     * Add the command line options including the specific parser ones, parse the command line and validate the found options
     *
     * @param args command line arguments
     * @throws SnifferException exception thrown if there is a problem reading or validating the command line
     */
    SnifferParameters parseCommandLine(final String[] args) throws SnifferException
    {
        log.info("Parsing command line arguments: {}", (Object) args);

        // Create the options
        final Options commandLineOptions = new Options();

        // Add the common command line options
        this.addCommonCommandLineOptions(commandLineOptions);

        // Parse the command line
        final CommandLineParser commandLineParser = new PosixParser();
        try
        {
            this.commandLine = commandLineParser.parse(commandLineOptions, args);
        }
        catch (final ParseException e)
        {
            log.error("Error parsing command line arguments", e);
            throw new SnifferException(e);
        }

        // Verify the command line arguments, both the common and the additional ones
        return this.parseAndValidateCommandLine();
    }

    /**
     * Validate the input command line arguments parsed from the command line
     *
     * @throws SnifferException exception thrown if the validation is not correct
     */
    private SnifferParameters parseAndValidateCommandLine() throws SnifferException
    {
        final String subnetAddress = this.getCmdStringOption(this.subnetOption);
        final String ipAddress = this.getCmdStringOption(this.ipAddressOption);
        final Integer port = this.getCmdIntegerOption(this.commPortOption);
        final Long timeout = this.getCmdLongOption(this.timeoutOption);

        // Validate the parameters
        final SnifferParameters result = SnifferParameters.builder().
                subnet(subnetAddress).
                port(port).
                ipAddress(ipAddress).timeout(timeout).build();
        result.validateSnifferParameters();

        return result;
    }

    /**
     * Return the String option value from the command line given the representing option
     *
     * @param option the representing option of the command line
     * @return the value of the option, exception if unsettled
     */
    private String getCmdStringOption(final Option option)
    {
        if (commandLine.hasOption(option.getOpt()))
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
        if (commandLine.hasOption(option.getOpt()))
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
        if (commandLine.hasOption(option.getOpt()))
        {
            return Long.parseLong(commandLine.getOptionValue(option.getOpt()).trim());
        }

        return null;
    }
}