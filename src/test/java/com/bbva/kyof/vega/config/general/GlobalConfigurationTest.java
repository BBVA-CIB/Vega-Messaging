package com.bbva.kyof.vega.config.general;


import com.bbva.kyof.vega.autodiscovery.daemon.CommandLineParserTest;
import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;


/**
 * Class to test {@link GlobalConfiguration}
 * Created by XE52727 on 22/06/16.
 */
public class GlobalConfigurationTest
{
    // Configuration
    private static final String EXTERNAL_DRIVER_DIR = CommandLineParserTest.class.getClassLoader().getResource("externalDriverDir").getPath();

    @Test
    public void emptyConstructor() throws VegaException
    {
        new GlobalConfiguration();
    }

    @Test(expected = VegaException.class)
    public void missingDriverType() throws VegaException
    {
        new GlobalConfiguration().completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void nullRvcPollerConfig() throws VegaException
    {
        GlobalConfiguration.builder().driverType(AeronDriverType.EMBEDDED).build().completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void emptyRvcPollerConfig() throws VegaException
    {
        GlobalConfiguration.builder().driverType(AeronDriverType.EMBEDDED).rcvPollerConfig(new LinkedList<>()).build().completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void invalidRvcPollerConfig() throws VegaException
    {
        GlobalConfiguration.builder().
                driverType(AeronDriverType.EMBEDDED).
                rcvPollerConfig(Collections.singletonList(new RcvPollerConfig())).
                build().
                completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void duplicatedRvcPollerConfig() throws VegaException
    {
        final RcvPollerConfig pollerConfig = this.createValidRcvPollerConfig("poller1");
        final RcvPollerConfig pollerConfig2 = this.createValidRcvPollerConfig("poller1");

        final GlobalConfiguration config = GlobalConfiguration.builder().
                driverType(AeronDriverType.EMBEDDED).
                rcvPollerConfig(Arrays.asList(pollerConfig, pollerConfig2)).
                build();

        config.completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void emptyAutoDiscConfig() throws VegaException
    {
        final GlobalConfiguration.GlobalConfigurationBuilder builder = GlobalConfiguration.builder().driverType(AeronDriverType.EMBEDDED);
        this.addRcvPollerConfig(builder);
        builder.build().completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void emptyResponsesConfig() throws VegaException
    {
        final GlobalConfiguration.GlobalConfigurationBuilder builder = GlobalConfiguration.builder().driverType(AeronDriverType.EMBEDDED);
        this.addRcvPollerConfig(builder);
        this.addAutoDiscConfig(builder);
        builder.build().completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void invalidPollerResponsesConfig() throws VegaException
    {
        final GlobalConfiguration.GlobalConfigurationBuilder builder = GlobalConfiguration.builder().driverType(AeronDriverType.EMBEDDED);
        this.addRcvPollerConfig(builder);
        this.addAutoDiscConfig(builder);
        builder.responsesConfig(ResponsesConfig.builder().rcvPoller("invalid").build());
        builder.build().completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void nullTopicTemplateConfig() throws VegaException
    {
        final GlobalConfiguration.GlobalConfigurationBuilder builder = GlobalConfiguration.builder().driverType(AeronDriverType.EMBEDDED);
        this.addRcvPollerConfig(builder);
        this.addAutoDiscConfig(builder);
        this.addResponsesConfig(builder);
        builder.build().completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void emptyTopicTemplateConfig() throws VegaException
    {
        final GlobalConfiguration.GlobalConfigurationBuilder builder = GlobalConfiguration.builder().driverType(AeronDriverType.EMBEDDED);
        this.addRcvPollerConfig(builder);
        this.addAutoDiscConfig(builder);
        this.addResponsesConfig(builder);
        builder.topicTemplate(new LinkedList<>());
        builder.build().completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void duplicatedTopicTemplateConfig() throws VegaException
    {
        final GlobalConfiguration.GlobalConfigurationBuilder builder = GlobalConfiguration.builder().driverType(AeronDriverType.EMBEDDED);
        this.addRcvPollerConfig(builder);
        this.addAutoDiscConfig(builder);
        this.addResponsesConfig(builder);
        builder.topicTemplate(Arrays.asList(createTopicTemplateConfig("name"), createTopicTemplateConfig("name")));
        builder.build().completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void nullTopicConfig() throws VegaException
    {
        final GlobalConfiguration.GlobalConfigurationBuilder builder = GlobalConfiguration.builder().driverType(AeronDriverType.EMBEDDED);
        this.addRcvPollerConfig(builder);
        this.addAutoDiscConfig(builder);
        this.addResponsesConfig(builder);
        this.addTopicTemplateConfig(builder);
        builder.build().completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void emptyTopicConfig() throws VegaException
    {
        final GlobalConfiguration.GlobalConfigurationBuilder builder = GlobalConfiguration.builder().driverType(AeronDriverType.EMBEDDED);
        this.addRcvPollerConfig(builder);
        this.addAutoDiscConfig(builder);
        this.addResponsesConfig(builder);
        this.addTopicTemplateConfig(builder);
        builder.topic(new LinkedList<>());
        builder.build().completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void invalidTopicConfig() throws VegaException
    {
        final GlobalConfiguration.GlobalConfigurationBuilder builder = GlobalConfiguration.builder().driverType(AeronDriverType.EMBEDDED);
        this.addRcvPollerConfig(builder);
        this.addAutoDiscConfig(builder);
        this.addResponsesConfig(builder);
        this.addTopicTemplateConfig(builder);
        builder.topic(Collections.singletonList(TopicConfig.builder().pattern("ab*").template("invalidTemplate").build()));
        builder.build().completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void duplicatedTopicConfig() throws VegaException
    {
        final GlobalConfiguration.GlobalConfigurationBuilder builder = GlobalConfiguration.builder().driverType(AeronDriverType.EMBEDDED);
        this.addRcvPollerConfig(builder);
        this.addAutoDiscConfig(builder);
        this.addResponsesConfig(builder);
        this.addTopicTemplateConfig(builder);

        builder.topic(Arrays.asList(
                TopicConfig.builder().pattern("ab*").template("template").build(),
                TopicConfig.builder().pattern("ab*").template("template").build()));

        builder.build().completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void invalidTopicSecurityConfig() throws VegaException
    {
        final GlobalConfiguration.GlobalConfigurationBuilder builder = GlobalConfiguration.builder().driverType(AeronDriverType.EMBEDDED);
        this.addRcvPollerConfig(builder);
        this.addAutoDiscConfig(builder);
        this.addResponsesConfig(builder);
        this.addTopicTemplateConfig(builder);
        this.addTopicTemplateConfig(builder);
        this.addTopicConfig(builder);
        builder.topicSecurity(Collections.singletonList(TopicSecurityConfig.builder().pattern("ab*").template("invalidTemplate").build()));
        builder.build().completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void duplicatedTopicSecurityConfig() throws VegaException
    {
        final GlobalConfiguration.GlobalConfigurationBuilder builder = GlobalConfiguration.builder().driverType(AeronDriverType.EMBEDDED);
        this.addRcvPollerConfig(builder);
        this.addAutoDiscConfig(builder);
        this.addResponsesConfig(builder);
        this.addTopicTemplateConfig(builder);
        this.addTopicTemplateConfig(builder);
        this.addTopicConfig(builder);

        builder.topicSecurityTemplate(Collections.singletonList(
                createTopicSecurityTemplateConfig("template")));

        builder.topicSecurity(Arrays.asList(
                TopicSecurityConfig.builder().pattern("ab*").template("template").build(),
                TopicSecurityConfig.builder().pattern("ab*").template("template").build()));

        builder.build().completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void duplicatedTopicSecurityTemplateConfig() throws VegaException
    {
        final GlobalConfiguration.GlobalConfigurationBuilder builder = GlobalConfiguration.builder().driverType(AeronDriverType.EMBEDDED);
        this.addRcvPollerConfig(builder);
        this.addAutoDiscConfig(builder);
        this.addResponsesConfig(builder);
        this.addTopicTemplateConfig(builder);
        this.addTopicTemplateConfig(builder);
        this.addTopicConfig(builder);

        builder.topicSecurityTemplate(Arrays.asList(
                createTopicSecurityTemplateConfig("ab*"),
                createTopicSecurityTemplateConfig("ab*")));

        builder.build().completeAndValidateConfig();
    }

    @Test
    public void validConfig() throws VegaException
    {
        final GlobalConfiguration.GlobalConfigurationBuilder builder = GlobalConfiguration.builder().driverType(AeronDriverType.EXTERNAL);
        builder.externalDriverDir(EXTERNAL_DRIVER_DIR);
        this.addRcvPollerConfig(builder);
        this.addAutoDiscConfig(builder);
        this.addResponsesConfig(builder);
        this.addTopicTemplateConfig(builder);
        this.addTopicConfig(builder);
        this.addTopicSecurityTemplateConfig(builder);
        this.addTopicSecurityConfig(builder);

        final GlobalConfiguration configuration = builder.build();

        configuration.completeAndValidateConfig();

        // There should be no embedded driver configuration
        Assert.assertNull(configuration.getEmbeddedDriverConfigFile());

        // There should be secure topics
        Assert.assertTrue(configuration.hasAnySecureTopic());

        // Now test the aditional getters
        Assert.assertNotNull(configuration.getAutodiscConfig());
        Assert.assertEquals(configuration.getDriverType(), AeronDriverType.EXTERNAL);
        Assert.assertEquals(configuration.getExternalDriverDir(), EXTERNAL_DRIVER_DIR);
        Assert.assertNotNull(configuration.getResponsesConfig());
        Assert.assertNotNull(configuration.getAutodiscConfig());

        // Poller map get
        Assert.assertNull(configuration.getPollerConfigForPollerName("invalidPoller"));
        Assert.assertNotNull(configuration.getPollerConfigForPollerName("poller"));

        // Topic config map get
        Assert.assertFalse(configuration.getTopicTemplateForTopic("invalidTopic") != null);
        Assert.assertTrue(configuration.getTopicTemplateForTopic("abc") != null);

        // Topic security config map get
        Assert.assertNull(configuration.getTopicSecurityTemplateForTopic("invalidTopic"));
        Assert.assertNotNull(configuration.getTopicSecurityTemplateForTopic("secure"));

        // Test control rcv config
        Assert.assertTrue(configuration.getControlRcvConfig() != null);
        Assert.assertTrue(configuration.getControlRcvConfig().getMaxPort() == ControlRcvConfig.DEFAULT_MAX_PORT);
        Assert.assertTrue(configuration.getControlRcvConfig().getMinPort() == ControlRcvConfig.DEFAULT_MIN_PORT);
        Assert.assertTrue(configuration.getControlRcvConfig().getNumStreams() == ControlRcvConfig.DEFAULT_NUM_STREAMS);
    }

    @Test
    public void validConfigNoTopicSec() throws VegaException
    {
        final GlobalConfiguration.GlobalConfigurationBuilder builder = GlobalConfiguration.builder().driverType(AeronDriverType.EXTERNAL);
        builder.externalDriverDir(EXTERNAL_DRIVER_DIR);
        this.addRcvPollerConfig(builder);
        this.addAutoDiscConfig(builder);
        this.addResponsesConfig(builder);
        this.addTopicTemplateConfig(builder);
        this.addTopicConfig(builder);

        final GlobalConfiguration configuration = builder.build();

        configuration.completeAndValidateConfig();

        // Topic security config map get
        Assert.assertNull(configuration.getTopicSecurityTemplateForTopic("invalidTopic"));
        Assert.assertNull(configuration.getTopicSecurityTemplateForTopic("secure"));
    }

    private RcvPollerConfig createValidRcvPollerConfig(final String pollerName)
    {
        return RcvPollerConfig.builder().name(pollerName).idleStrategyType(IdleStrategyType.BUSY_SPIN).build();
    }

    private TopicTemplateConfig createTopicTemplateConfig(final String name)
    {
        return TopicTemplateConfig.builder().name(name).rcvPoller("poller").transportType(TransportMediaType.UNICAST).build();
    }

    private TopicSecurityTemplateConfig createTopicSecurityTemplateConfig(final String name)
    {
        return TopicSecurityTemplateConfig.builder().name(name).
                pubSecIds(new HashSet<>(Collections.singletonList(11111))).
                subSecIds(new HashSet<>(Collections.singletonList(22222))).build();
    }

    private AutoDiscoveryConfig createValidAutoDiscConfig()
    {
        return AutoDiscoveryConfig.builder().autoDiscoType(AutoDiscoType.MULTICAST).build();
    }

    private void addRcvPollerConfig(final GlobalConfiguration.GlobalConfigurationBuilder builer)
    {
        builer.rcvPollerConfig(Collections.singletonList(createValidRcvPollerConfig("poller")));
    }

    private void addAutoDiscConfig(final GlobalConfiguration.GlobalConfigurationBuilder builer)
    {
        builer.autodiscConfig(this.createValidAutoDiscConfig());
    }

    private void addResponsesConfig(final GlobalConfiguration.GlobalConfigurationBuilder builer)
    {
        builer.responsesConfig(ResponsesConfig.builder().rcvPoller("poller").build());
    }

    private void addTopicTemplateConfig(final GlobalConfiguration.GlobalConfigurationBuilder builder)
    {
        builder.topicTemplate(Collections.singletonList(this.createTopicTemplateConfig("template")));
    }

    private void addTopicConfig(final GlobalConfiguration.GlobalConfigurationBuilder builder)
    {
        builder.topic(Collections.singletonList(TopicConfig.builder().template("template").pattern("ab.*").build()));
    }

    private void addTopicSecurityTemplateConfig(final GlobalConfiguration.GlobalConfigurationBuilder builder)
    {
        builder.topicSecurityTemplate(Collections.singletonList(this.createTopicSecurityTemplateConfig("secTemplate")));
    }

    private void addTopicSecurityConfig(final GlobalConfiguration.GlobalConfigurationBuilder builder)
    {
        builder.topicSecurity(Collections.singletonList(TopicSecurityConfig.builder().template("secTemplate").pattern("sec.*").build()));
    }
}