package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.config.ConfigConstants;
import com.bbva.kyof.vega.exception.VegaException;
import lombok.extern.slf4j.Slf4j;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;

/**
 * This class helps to read the instance configuration
 */
@Slf4j
public final class ConfigReader
{
    /** Private constructor to avoid instantiation fo utility class */
    private ConfigReader()
    {
        // Nothing to do
    }

    /**
     * Read the configuration given the path to the configuration file
     * @param configFile the path to the xml configuration file to read
     * @return the loaded and validated configuration
     * @throws VegaException exception thrown if there is any problem loading the configuration
     */
    public static GlobalConfiguration readConfiguration(final String configFile) throws VegaException
    {
        log.info("Loading the xml configuration file [{}] ", configFile);

        // First unmarshall the configuration
        final GlobalConfiguration result = unmarshallConfiguration(configFile);

        // Complete the default values and validate the contents
        result.completeAndValidateConfig();

        // Return the loaded configuration
        return result;
    }

    /**
     * Unmarshall the configuration using JAXB
     *
     * @param configFile path to the file
     * @return the unmarshalled configuration
     *
     * @throws VegaException exception thrown if there is any problem
     */
    private static GlobalConfiguration unmarshallConfiguration(final String configFile) throws VegaException
    {
        try
        {
            final File file = new File(configFile);
            final JAXBContext jaxbContext = JAXBContext.newInstance(GlobalConfiguration.class);
            final Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            final SchemaFactory factory = SchemaFactory.newInstance(ConfigConstants.W3_SCHEMA);
            final Schema schema = factory.newSchema(new StreamSource(ConfigReader.class.getResourceAsStream(ConfigConstants.XSD_GENERAL_CONFIG_FILE)));

            jaxbUnmarshaller.setSchema(schema);

            return (GlobalConfiguration) jaxbUnmarshaller.unmarshal(file);
        }
        catch (final SAXException | JAXBException e)
        {
            log.error("Error parsing the xml file [{}]", configFile, e);

            throw new VegaException("Error loading the xml configuration file passed.", e);
        }
    }
}
