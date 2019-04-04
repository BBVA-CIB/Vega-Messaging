package com.bbva.kyof.vega.config.privkey;


import com.bbva.kyof.vega.config.ConfigConstants;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.file.FilePathUtil;
import lombok.extern.slf4j.Slf4j;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.IOException;

/**
 * This class reads and validate a private key configuration
 */
@Slf4j
public final class PrivateKeyConfigReader
{
    /** Private constructor to avoid instantiation */
    private PrivateKeyConfigReader()
    {
        // Nothing to do here
    }

    /**
     * Read the configuration given the path to the configuration file
     *
     * @param directoryPath the path to the xml configuration file to read
     * @param securityId application security identifier
     * @return the loaded and validated configuration
     * @throws VegaException exception thrown if there is any problem loading the configuration
     */
    public static PrivateKeyConfig readConfiguration(final String directoryPath, final long securityId) throws VegaException
    {
        // Construct the file full path
        final String privateKeyFullPath = PrivateKeyConfigReader.createPrivKeyFileFullPath(directoryPath, securityId);

        log.info("Loading and validating the xml private key file [{}]", privateKeyFullPath);

        // Verify the private file
        try
        {
            FilePathUtil.verifyFilePath(privateKeyFullPath);
        }
        catch (final IOException e)
        {
            throw new VegaException("Error trying to access the private key file " + privateKeyFullPath, e);
        }

        // First unmarshall the private key and load it
        final PrivateKeyConfig privKeyConfiguration = unmarshallPrivKey(privateKeyFullPath);

        // Make sure the application security ID of the private key is correct
        if (securityId != privKeyConfiguration.getAppSecurityId())
        {
            throw new VegaException("Unique security ID in the private key file don't correspond to the loaded security id");
        }

        privKeyConfiguration.completeAndValidateConfig();

        return privKeyConfiguration;
    }

    /**
     * Unmarshall the private key file using jaxb
     *
     * @param privKeysFile path to the file
     * @return the unmarshalled key
     * @throws VegaException exception thrown if there is any problem
     */
    private static PrivateKeyConfig unmarshallPrivKey(final String privKeysFile) throws VegaException
    {
        try
        {
            final File file = new File(privKeysFile);
            final JAXBContext jaxbContext = JAXBContext.newInstance(PrivateKeyConfig.class);
            final Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            final SchemaFactory factory = SchemaFactory.newInstance(ConfigConstants.W3_SCHEMA);
            final Schema schema = factory.newSchema(new StreamSource(PrivateKeyConfigReader.class.getResourceAsStream(ConfigConstants.XSD_PRIV_KEY_CONFIG_FILE)));

            jaxbUnmarshaller.setSchema(schema);

            return (PrivateKeyConfig) jaxbUnmarshaller.unmarshal(file);
        }
        catch (final SAXException | JAXBException e)
        {
            log.error("Error parsing the private key xml file [{}]", privKeysFile, e);

            throw new VegaException("Error loading the xml private key configuration file passed.", e);
        }
    }

    /**
     * Marshall the private key file using jaxb and store it in the given path and name
     *
     * @param privKeyConfig object to marshall
     * @param outputFilePath path to the output file for the key
     * @throws VegaException exception thrown if there is any problem
     */
    public static void marshallPrivKey(final PrivateKeyConfig privKeyConfig, final String outputFilePath) throws VegaException
    {
        try
        {
            final JAXBContext jaxbContext = JAXBContext.newInstance(PrivateKeyConfig.class);
            final Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
            final SchemaFactory factory = SchemaFactory.newInstance(ConfigConstants.W3_SCHEMA);
            final Schema schema = factory.newSchema(new StreamSource(PrivateKeyConfigReader.class.getResourceAsStream(ConfigConstants.XSD_PRIV_KEY_CONFIG_FILE)));

            jaxbMarshaller.setSchema(schema);
            final String fileName = createPrivKeyFileFullPath(outputFilePath, privKeyConfig.getAppSecurityId());

            jaxbMarshaller.marshal(privKeyConfig, new File(fileName));
        }
        catch (final SAXException | JAXBException e)
        {
            log.error("Error storing the private key xml file [{}]", outputFilePath, e);
            throw new VegaException("Error storing the private keys xml file", e);
        }
    }

    /**
     * Create a full path to the private key file given the base path and the security ID for the key
     *
     * @param dirPath the base path for the file
     * @param securityId the unique security identifier
     * @return the created path
     */
    public static String createPrivKeyFileFullPath(final String dirPath, final long securityId)
    {
        return dirPath + File.separator + ConfigConstants.PRIV_KEY_FILE_PREFIX + "_" + securityId + ".xml";
    }
}
