package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by cnebrera on 14/10/2016.
 */
public class TopicSecurityTemplateConfigTest
{
    private static Set<Integer> PUB_SEC_IDS = new HashSet<>();
    private static Set<Integer> SUB_SEC_IDS = new HashSet<>();

    @BeforeClass
    public static void beforeClass()
    {
        PUB_SEC_IDS.add(11111);
        SUB_SEC_IDS.add(22222);
    }

    @Test
    public void emptyConstructor() throws Exception
    {
        new TopicSecurityTemplateConfig();
    }

    @Test(expected = VegaException.class)
    public void validateMissingName() throws Exception
    {
        // Should fail, it is missing the name
        final TopicSecurityTemplateConfig invalidConfig = TopicSecurityTemplateConfig.builder().build();
        invalidConfig.completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void validateMissingPubIds() throws Exception
    {
        // Should fail, it is missing the transport type
        final TopicSecurityTemplateConfig invalidConfig = TopicSecurityTemplateConfig.builder().name("aname").build();
        invalidConfig.completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void validateEmptyPubIds() throws Exception
    {
        // Should fail, it is missing the transport type
        final TopicSecurityTemplateConfig invalidConfig = TopicSecurityTemplateConfig.builder().name("aname").pubSecIds(new HashSet<>()).build();
        invalidConfig.completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void validateMissingSubIds() throws Exception
    {
        // Should fail, it is missing the transport type
        final TopicSecurityTemplateConfig invalidConfig = TopicSecurityTemplateConfig.builder().name("aname").pubSecIds(PUB_SEC_IDS).build();
        invalidConfig.completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void validateEmptySubIds() throws Exception
    {
        // Should fail, it is missing the transport type
        final TopicSecurityTemplateConfig invalidConfig = TopicSecurityTemplateConfig.builder().name("aname").pubSecIds(PUB_SEC_IDS).subSecIds(new HashSet<>()).build();
        invalidConfig.completeAndValidateConfig();
    }

    @Test
    public void testAllParamsOk() throws Exception
    {
        // Should fail, it is missing the transport type
        final TopicSecurityTemplateConfig validConfig = TopicSecurityTemplateConfig.builder().
                name("aname").
                controlMsgInterval(200L).
                pubSecIds(PUB_SEC_IDS).
                subSecIds(SUB_SEC_IDS).build();

        validConfig.completeAndValidateConfig();

        Assert.assertEquals(validConfig.getName(), "aname");
        Assert.assertTrue(validConfig.getControlMsgInterval() == 200L);
        Assert.assertTrue(validConfig.getPubSecIds().contains(11111));
        Assert.assertTrue(validConfig.getSubSecIds().contains(22222));
    }
}