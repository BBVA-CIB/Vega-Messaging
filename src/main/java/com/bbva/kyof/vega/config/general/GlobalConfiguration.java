package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.config.IConfiguration;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.PatternEquals;
import com.bbva.kyof.vega.util.file.FilePathUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.*;
import java.io.IOException;
import java.util.*;

/**
 * Contains the whole configuration for a library instance, including auto-discovery, request/response, topics, pollers, etc. <p>
 *
 * The class support Jaxb to read from an XML and also contains methods to validate and complete teh configuration with he default values
 * for optional parameters. Also check information about subnet and interfaces.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "vega_config")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class GlobalConfiguration implements IConfiguration
{
    /** Stores the Aeron driver type to use */
    @XmlElement(name = "driver_type", required = true)
    @Getter private AeronDriverType driverType;

    /** (Optional) Stores the embedded driver configuration file to use if embedded driver is selected */
    @XmlElement(name = "embedded_driver_config_file")
    @Getter private String embeddedDriverConfigFile;

    /** (Optional) Stores the Aeron driver directory for external driver.
     * Only required if external driver is used and we don't want to use the default value */
    @XmlElement(name = "external_driver_directory")
    @Getter private String externalDriverDir;

    /** Stores all receive poller configurations */
    @XmlElement(name = "rcv_poller_config", required = true)
    private List<RcvPollerConfig> rcvPollerConfig;

    /** (Optional) Unicast control channel receiver for control messages configuration */
    @XmlElement(name = "control_rcv_config")
    @Getter private ControlRcvConfig controlRcvConfig;

    /** Response configuration */
    @XmlElement(name = "responses_config", required = true)
    @Getter private ResponsesConfig responsesConfig;
    
    /** Auto-discovery configuration */
    @XmlElement(name = "autodisc_config", required = true)
    @Getter private AutoDiscoveryConfig autodiscConfig;
    
    /** Stores all topic template configurations */
    @XmlElement(name = "topic_template", required = true)
    private List<TopicTemplateConfig> topicTemplate;
    
    /** Stores all topic configurations */
    @XmlElement(name = "topic_pattern", required = true)
    private List<TopicConfig> topic;

    /** (Optional) Stores all topic security template configurations */
    @XmlElement(name = "topic_security_template")
    private List<TopicSecurityTemplateConfig> topicSecurityTemplate;

    /** (Optional) Stores all topic security configurations */
    @XmlElement(name = "topic_security_pattern")
    private List<TopicSecurityConfig> topicSecurity;

    /** Stores all configured receiver pollers by pool name */
    @XmlTransient
    private final Map<String, RcvPollerConfig> rcvPollersByName = new HashMap<>();

    /** Stores all configured topic templates by template name */
    @XmlTransient
    private final Map<String, TopicTemplateConfig> topicTemplatesByName = new HashMap<>();

    /** Stores all the configured topics by pattern */
    @XmlTransient
    private final Map<PatternEquals, TopicConfig> topicConfigByPattern = new LinkedHashMap<>();

    /** Stores all configured topic security templates by template name */
    @XmlTransient
    private final Map<String, TopicSecurityTemplateConfig> topicSecurityTemplatesByName = new HashMap<>();

    /** Stores all the configured secure topics by pattern */
    @XmlTransient
    private final Map<PatternEquals, TopicSecurityConfig> topicSecurityConfigByPattern = new LinkedHashMap<>();

    @Override
    public void completeAndValidateConfig() throws VegaException
    {
        this.checkMediaDriverConfig();
        this.checkRcvPollerConfig();
        this.checkAutodiscConfig();
        this.checkControlRcvConfig();
        this.checkResponsesConfig();
        this.checkTopicTemplateConfig();
        this.checkTopicConfig();
        this.checkTopicSecurityTemplateConfig();
        this.checkTopicSecurityConfig();
    }

    /**
     * Return true if there are configurations for secure topics
     * @return true if any of the topics has security configured
     */
    public boolean hasAnySecureTopic()
    {
        return this.topicSecurity != null && !this.topicSecurity.isEmpty();
    }

    /**
     * Check the configuration of the media driver
     *
     * @throws VegaException exception thrown if there is a problem checking the configuration
     */
    private void checkMediaDriverConfig() throws VegaException
    {
        // Ensure the driver type to use is selected
        if (this.driverType == null)
        {
            throw new VegaException("Missing compulsory parameter driver_type");
        }

        // If the driver directory is selected, check that it exists and that is a directory
        if (this.driverType == AeronDriverType.EXTERNAL && this.externalDriverDir != null)
        {
            try
            {
                FilePathUtil.verifyDirPath(this.externalDriverDir);
            }
            catch (final IOException e)
            {
                throw new VegaException("Exception trying to verify external driver directory " + this.externalDriverDir, e);
            }
        }

        // If the driver is embedded and the configuration is provided check that the file exists
        if (this.driverType != AeronDriverType.EXTERNAL && this.embeddedDriverConfigFile != null)
        {
            try
            {
                FilePathUtil.verifyFilePath(this.embeddedDriverConfigFile);
            }
            catch (final IOException e)
            {
                throw new VegaException("Exception trying to verify embedded driver configuration file directory " + this.externalDriverDir, e);
            }
        }
    }

    /**
     * Check and validate the configuration of reception pollers
     * @throws VegaException exception thrown if there is a problem in the configuration
     */
    private void checkRcvPollerConfig() throws VegaException
    {
        if (this.rcvPollerConfig == null || this.rcvPollerConfig.isEmpty())
        {
            throw new VegaException("No receiver poller configuration found");
        }

        for (final RcvPollerConfig pollerConfig : this.rcvPollerConfig)
        {
            pollerConfig.completeAndValidateConfig();

            // Name of the poller
            final String pollerName = pollerConfig.getName();

            // Look for duplicates
            if (this.rcvPollersByName.containsKey(pollerName))
            {
                throw new VegaException("Duplicated rcv poller name:" + pollerName);
            }

            // Store in a map for fast lookup
            this.rcvPollersByName.put(pollerName, pollerConfig);
        }
    }

    /**
     * Check and validate the auto-discovery configuration
     * @throws VegaException exception thrown if there is a problem in the configuration
     */
    private void checkAutodiscConfig() throws VegaException
    {
        if (this.autodiscConfig == null)
        {
            throw new VegaException("Autodiscovery configuration not found");
        }

        this.autodiscConfig.completeAndValidateConfig();
    }

    /**
     * Check and validate the configuration for control receiver channel
     * @throws VegaException exception thrown if there is a problem in the configuration
     */
    private void checkControlRcvConfig() throws VegaException
    {
        // If not settled create a new one with default parameters
        if (this.controlRcvConfig == null)
        {
            this.controlRcvConfig = new ControlRcvConfig();
        }

        this.controlRcvConfig.completeAndValidateConfig();
    }

    /**
     * Check and validate the configuration for responses
     * @throws VegaException exception thrown if there is a problem in the configuration
     */
    private void checkResponsesConfig() throws VegaException
    {
        if (this.responsesConfig == null)
        {
            throw new VegaException("Responses configuration not found");
        }

        this.responsesConfig.completeAndValidateConfig();

        // Make sure the reception pool defined in the responses config exists
        this.checkIfPollerExists(this.responsesConfig.getRcvPoller());
    }

    /**
     * Check and validate the configuration for topic templates
     * @throws VegaException exception thrown if there is a problem in the configuration
     */
    private void checkTopicTemplateConfig() throws VegaException
    {
        if (this.topicTemplate == null || topicTemplate.isEmpty())
        {
            throw new VegaException("Topic templates configuration not found, at least a topic pattern template should be defined");
        }

        for (final TopicTemplateConfig templateConfig : this.topicTemplate)
        {
            templateConfig.completeAndValidateConfig();

            // Name of the template
            final String templateName = templateConfig.getName();

            // Look for duplicates
            if (this.topicTemplatesByName.containsKey(templateName))
            {
                throw new VegaException("Duplicated topic template name:" + templateName);
            }

            // Make sure the defined poller in the template configuration exists
            this.checkIfPollerExists(templateConfig.getRcvPoller());

            // Store in a map for fast lookup
            this.topicTemplatesByName.put(templateConfig.getName(), templateConfig);
        }
    }

    /**
     * Check and validate the configuration for topic patterns
     * @throws VegaException exception thrown if there is a problem in the configuration
     */
    private void checkTopicConfig() throws VegaException
    {
        if (this.topic == null || topic.isEmpty())
        {
            throw new VegaException("No topic pattern configuration found, at least a topic pattern should be defined");
        }

        for (final TopicConfig topicConfig : this.topic)
        {
            topicConfig.completeAndValidateConfig();

            // Make sure the template defined for the topic pattern exists
            this.checkIfTopicTemplateExists(topicConfig.getTemplate());

            // Create the pattern and check for duplicates
            final PatternEquals patternEquals = new PatternEquals(topicConfig.getPattern());
            if (this.topicConfigByPattern.containsKey(patternEquals))
            {
                throw new VegaException("Duplicated topic name pattern found in the xml configuration file:" + patternEquals);
            }

            // Store in a map for fast lookup
            this.topicConfigByPattern.put(patternEquals, topicConfig);
        }
    }

    /**
     * Check and validate the configuration for topic security templates
     * @throws VegaException exception thrown if there is a problem in the configuration
     */
    private void checkTopicSecurityTemplateConfig() throws VegaException
    {
        // If not settled, ignore it. Security is optional
        if (this.topicSecurityTemplate == null)
        {
            return;
        }

        for (final TopicSecurityTemplateConfig templateConfig : this.topicSecurityTemplate)
        {
            templateConfig.completeAndValidateConfig();

            // Name of the template
            final String templateName = templateConfig.getName();

            // Look for duplicates
            if (this.topicSecurityTemplatesByName.containsKey(templateName))
            {
                throw new VegaException("Duplicated topic template name:" + templateName);
            }

            // Store in a map for fast lookup
            this.topicSecurityTemplatesByName.put(templateConfig.getName(), templateConfig);
        }
    }

    /**
     * Check and validate the configuration for topic security patterns
     * @throws VegaException exception thrown if there is a problem in the configuration
     */
    private void checkTopicSecurityConfig() throws VegaException
    {
        // If not settled ignore, the parameter is not compulsory
        if (this.topicSecurity == null)
        {
            return;
        }

        for (final TopicSecurityConfig topicConfig : this.topicSecurity)
        {
            topicConfig.completeAndValidateConfig();

            // Make sure the template defined for the topic pattern exists
            this.checkIfTopicSecurityTemplateExists(topicConfig.getTemplate());

            // Create the pattern and check for duplicates
            final PatternEquals patternEquals = new PatternEquals(topicConfig.getPattern());
            if (this.topicSecurityConfigByPattern.containsKey(patternEquals))
            {
                throw new VegaException("Duplicated security topic name pattern found in the xml configuration file:" + patternEquals);
            }

            // Store in a map for fast lookup
            this.topicSecurityConfigByPattern.put(patternEquals, topicConfig);
        }
    }

    /**
     * Check by poller name if it has been configured
     * @throws VegaException exception thrown if it has not been configured
     */
    private void checkIfPollerExists(final String pollerName) throws VegaException
    {
        if (!this.rcvPollersByName.containsKey(pollerName))
        {
            throw new VegaException("Cannot find any receiver poller with name " + pollerName);
        }
    }

    /**
     * Check by template name if the topic template has been configured
     * @throws VegaException exception thrown if it has not been configured
     */
    private void checkIfTopicTemplateExists(final String templateName) throws VegaException
    {
        if (!this.topicTemplatesByName.containsKey(templateName))
        {
            throw new VegaException("Cannot find any topic template with name " + templateName);
        }
    }

    /**
     * Check by security template name if the topic security template has been configured
     * @throws VegaException exception thrown if it has not been configured
     */
    private void checkIfTopicSecurityTemplateExists(final String templateName) throws VegaException
    {
        if (!this.topicSecurityTemplatesByName.containsKey(templateName))
        {
            throw new VegaException("Cannot find any security topic template with name " + templateName);
        }
    }

    /**
     * Returns Config topicCfg, it will look in all stored patterns to first one in the list that matches.
     * This means configurations should contain the most specific patterns first.
     *
     * @param topicName name of the topic (it will be matched again all patterns)
     * @return the topic configuration found, null if not found
     */
    private TopicConfig getTopicCfgForTopic(final String topicName)
    {
        for (final Map.Entry<PatternEquals, TopicConfig> topicCfg : this.topicConfigByPattern.entrySet())
        {
            if (topicCfg.getKey().matches(topicName))
            {
                return topicCfg.getValue();
            }
        }

        return null;
    }

    /**
     * Returns the template for the given topic name
     *
     * @param topicName name of the topic to look for the template
     * @return the template that matches the given topic name, null if not found
     */
    public TopicTemplateConfig getTopicTemplateForTopic(final String topicName)
    {
        // Find the topic configuration
        final TopicConfig topicConfig = this.getTopicCfgForTopic(topicName);

        // If there is no topic configuration, there is no template either
        if (topicConfig == null)
        {
            return null;
        }

        // Find the template now
        return this.getTopicTemplate(topicConfig.getTemplate());
    }

    /**
     * Returns the template for the given template name
     *
     * @param templateName name of the topic template configuration, null if not found
     * @return the template that matches the given name
     */
    private TopicTemplateConfig getTopicTemplate(final String templateName)
    {
        return this.topicTemplatesByName.get(templateName);
    }

    /**
     * Rerturn the poller configuration for the given poller name
     * @param pollerName the name of the poller
     *
     * @return the configuration or null if not found
     */
    public RcvPollerConfig getPollerConfigForPollerName(final String pollerName)
    {
        return this.rcvPollersByName.get(pollerName);
    }

    /**
     * Returns Config topicCfg for security configuration, it will look in all stored patterns to first one in the list that matches.
     * This means configurations should contain the most specific patterns first.
     *
     * @param topicName name of the topic (it will be matched again all patterns)
     * @return the topic security configuration found or null if not found
     */
    private TopicSecurityConfig getTopicSecurityCfgForTopic(final String topicName)
    {
        for (final Map.Entry<PatternEquals, TopicSecurityConfig> topicCfg : this.topicSecurityConfigByPattern.entrySet())
        {
            if (topicCfg.getKey().matches(topicName))
            {
                return topicCfg.getValue();
            }
        }

        return null;
    }

    /**
     * Returns the security template for the given topic name
     *
     * @param topicName name of the topic to look for the security template
     * @return the security template that matches the given topic name or null if not found
     */
    public TopicSecurityTemplateConfig getTopicSecurityTemplateForTopic(final String topicName)
    {
        // Find the topic configuration
        final TopicSecurityConfig topicConfig = this.getTopicSecurityCfgForTopic(topicName);

        // If there is no topic configuration, there is no template either
        if (topicConfig == null)
        {
            return null;
        }

        // Find the template now
        return this.getTopicSecurityTemplate(topicConfig.getTemplate());
    }

    /**
     * Return all the security ids used in any topic configured with security
     * @return a map with all the security id's, empty if no topic is using security at all
     */
    public Set<Integer> getAllSecureTopicsSecurityIds()
    {
        final Set<Integer> result = new HashSet<>();

        // Make sure the template is not null
        if (this.topicSecurityTemplate != null)
        {
            this.topicSecurityTemplate.forEach(template ->
            {
                result.addAll(template.getPubSecIds());
                result.addAll(template.getSubSecIds());
            });
        }

        return result;
    }

    /**
     * Returns the template for the given template name
     *
     * @param templateName name of the topic template configuration
     * @return the template that matches the given name, null if not found
     */
    private TopicSecurityTemplateConfig getTopicSecurityTemplate(final String templateName)
    {
        return this.topicSecurityTemplatesByName.get(templateName);
    }
}
