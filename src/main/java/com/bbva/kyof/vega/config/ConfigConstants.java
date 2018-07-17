package com.bbva.kyof.vega.config;

/**
 * Class with configuration constants
 */
public final class ConfigConstants
{
    /** Schema used to validate the xsd for the jaxb */
    public static final String W3_SCHEMA = "http://www.w3.org/2001/XMLSchema";

    /** Prefix of public key files */
    public static final String PUB_KEY_FILE_PREFIX = "VEGA_PUB_KEY";

    /** Prefix of private key files */
    public static final String PRIV_KEY_FILE_PREFIX = "VEGA_PRIV_KEY";

    /** Location of the XSD describing the general configuraton */
    public static final String XSD_GENERAL_CONFIG_FILE = "/xsd/config_schema.xsd";

    /** Location of the XSD describing the XML private key configuration file */
    public static final String XSD_PRIV_KEY_CONFIG_FILE = "/xsd/private_key_schema.xsd";

    /** Location of the XSD describing the XML trusted keys configuration file */
    public static final String XSD_PUBLIC_KEY_CONFIG_FILE = "/xsd/public_key_schema.xsd";

    /** Private constructor to prevent instantiation of constants class */
    private ConfigConstants()
    {
        // Nothing to do here
    }
}
