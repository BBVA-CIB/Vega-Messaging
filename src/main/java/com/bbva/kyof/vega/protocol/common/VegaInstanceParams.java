package com.bbva.kyof.vega.protocol.common;

import com.bbva.kyof.vega.config.general.GlobalConfiguration;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.file.FilePathUtil;
import io.aeron.driver.MediaDriver;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.agrona.ErrorHandler;

import java.io.IOException;

/**
 * Parameters for the Vega messaging library initialization
 */
@Builder
@ToString
public final class VegaInstanceParams
{
    /** Name of the instance */
    @Getter private final String instanceName;

    /** path + name of the configuration xml file */
    @Getter private final String configurationFile;

    /** Configuration loaded by code. It is overwritten by the configurationFile if the file is not null*/
    @Getter private final GlobalConfiguration globalConfiguration;

    /** (Optional) Media driver in case we want to handle ourselfs the media driver lifecycle */
    @Getter private final MediaDriver unmanagedMediaDriver;

    /** (Optional) Instance parameters for secure connections. Only required if any of the topics has "encryption" configured */
    @Getter private final SecurityParams securityParams;

    /**
     * (Optional) Error handler for Aeron internal errors, if not settled it will use the default Aeron behaviour.
     *  The default behaviour for a DriverTimeoutException is to close the application¡
     */
    @Getter private final ErrorHandler aeronErrorHandler;

    /**
     * Validate the parameters checking for compulsory parameters and verifying file access
     *
     * @throws VegaException exception thrown if there is a problem with the parameters
     */
    public void validateParams() throws VegaException
    {
        // Validate instance name
        if (this.instanceName == null)
        {
            throw new VegaException("Missing required parameter [instanceName]");
        }

        // Validate configuration file
        if (this.configurationFile == null && this.globalConfiguration == null)
        {
            throw new VegaException("Missing required parameter [configuration file name or programmatic configuration]");
        }

        // Check the configuration
        if (this.configurationFile == null)
        {
            // If the configuration file does not exist, check the programmatic configuration
            globalConfiguration.completeAndValidateConfig();
        }
        else
        {
            // If the configuration file exists, check it
            try
            {
                FilePathUtil.verifyFilePath(this.configurationFile);
            }
            catch (final IOException e)
            {
                throw new VegaException("Error trying to access the configuration file " + this.configurationFile, e);
            }
        }

        // Validate security parameters
        if (this.securityParams != null)
        {
            this.securityParams.validateParams();
        }
    }
}
