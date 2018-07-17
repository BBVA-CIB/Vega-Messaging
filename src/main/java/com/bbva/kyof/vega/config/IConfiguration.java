package com.bbva.kyof.vega.config;

import com.bbva.kyof.vega.exception.VegaException;

/**
 * Instance to implement by all configuration classes
 */
@FunctionalInterface
public interface IConfiguration
{
    /** Validates the configuration instance and complete the optional values with default values when required.
     *
     * @throws VegaException if there is a problem in the validation or something is missing
     */
     void completeAndValidateConfig() throws VegaException;
}
