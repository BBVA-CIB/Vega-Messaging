package com.bbva.kyof.vega.config.pubkey;


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
public final class PublicKeyConfigReader
{
    /** Private constructor to avoid instantiation */
    private PublicKeyConfigReader()
    {
        // Nothing to do here
    }

    /**
     * Read the configuration given the path to the directory that contains the file and the security id
     * The full file path will be formed using the security id
     *
     * @param directoryPath directory path to the xml configuration file to read
     * @param securityId unique security id of the configuration to load
     * @return the loaded and validated configuration
     * @throws VegaException exception thrown if there is any problem loading the configuration
     */
    public static PublicKeyConfig readConfiguration(final String directoryPath, final long securityId) throws VegaException
    {
        // Construct the file full path
        final String publicKeyFullPath = PublicKeyConfigReader.createPubKeyFileFullPath(directoryPath, securityId);

        log.info("Loading and validating the xml public key file [{}]", publicKeyFullPath);

        // Verify the private file
        try
        {
            FilePathUtil.verifyFilePath(publicKeyFullPath);
        }
        catch (final IOException e)
        {
            throw new VegaException("Error trying to access the public key file " + publicKeyFullPath, e);
        }

        // First unmarshall the private key and load it
        final PublicKeyConfig pubKeyConfiguration = unmarshallPubKey(publicKeyFullPath);

        // Make sure the application security ID of the public key is correct
        if (securityId != pubKeyConfiguration.getAppSecurityId())
        {
            throw new VegaException("Unique security ID in the public key file don't correspond to the loaded security id");
        }

        pubKeyConfiguration.completeAndValidateConfig();

        return pubKeyConfiguration;
    }

    /**
     * Unmarshall the public key file using jaxb
     *
     * @param pubKeysFile path to the file
     * @return the unmarshalled key
     * @throws VegaException exception thrown if there is any problem
     */
    private static PublicKeyConfig unmarshallPubKey(final String pubKeysFile) throws VegaException
    {
        try
        {
            final File file = new File(pubKeysFile);
            final JAXBContext jaxbContext = JAXBContext.newInstance(PublicKeyConfig.class);
            final Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            final SchemaFactory factory = SchemaFactory.newInstance(ConfigConstants.W3_SCHEMA);
            final Schema schema = factory.newSchema(new StreamSource(PublicKeyConfigReader.class.getResourceAsStream(ConfigConstants.XSD_PUBLIC_KEY_CONFIG_FILE)));

            jaxbUnmarshaller.setSchema(schema);

            return (PublicKeyConfig) jaxbUnmarshaller.unmarshal(file);
        }
        catch (final SAXException | JAXBException e)
        {
            log.error("Error parsing the private key xml file [{}]", pubKeysFile, e);

            throw new VegaException("Error loading the xml private key configuration file passed.", e);
        }
    }

    /**
     * Marshall the public key file using jaxb and store it in the given path and name
     *
     * @param pubKeyConfig object to marshall
     * @param outputFilePath path where the key file will be written
     * @throws VegaException exception thrown if there is any problem
     */
    public static void marshallPubKey(final PublicKeyConfig pubKeyConfig, final String outputFilePath) throws VegaException
    {
        try
        {
            final JAXBContext jaxbContext = JAXBContext.newInstance(PublicKeyConfig.class);
            final Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
            final SchemaFactory factory = SchemaFactory.newInstance(ConfigConstants.W3_SCHEMA);
            final Schema schema = factory.newSchema(new StreamSource(PublicKeyConfigReader.class.getResourceAsStream(ConfigConstants.XSD_PUBLIC_KEY_CONFIG_FILE)));

            jaxbMarshaller.setSchema(schema);

            final String fileName = createPubKeyFileFullPath(outputFilePath, pubKeyConfig.getAppSecurityId());

            jaxbMarshaller.marshal(pubKeyConfig, new File(fileName));
        }
        catch (final SAXException | JAXBException e)
        {
            log.error("Error storing the public key xml file [{}]", outputFilePath, e);
            throw new VegaException("Error storing the public keys xml file", e);
        }
    }

    /**
     * Create a full path to the public key file given the base path and the security ID for the key
     *
     * @param dirPath the base path for the file
     * @param securityId the unique security identifier
     * @return the created path
     */
    public static String createPubKeyFileFullPath(final String dirPath, final long securityId)
    {
        return dirPath + File.separator + ConfigConstants.PUB_KEY_FILE_PREFIX + "_" + securityId + ".xml";
    }
}
