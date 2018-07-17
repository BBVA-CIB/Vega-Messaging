package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 14/10/2016.
 */
public class TopicSecurityConfigTest
{
    @Test
    public void empyConstructor() throws Exception
    {
        new TopicSecurityConfig();
    }

    @Test(expected = VegaException.class)
    public void validateEmptyBuilder() throws Exception
    {
        final TopicSecurityConfig invalidConfig = TopicSecurityConfig.builder().build();
        invalidConfig.completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void validateMissingTemplate() throws Exception
    {
        final TopicSecurityConfig invalidConfig = TopicSecurityConfig.builder().pattern("ab").build();
        invalidConfig.completeAndValidateConfig();
    }

    @Test
    public void validateParams() throws Exception
    {
        final TopicSecurityConfig config = TopicSecurityConfig.builder().pattern("ab*").template("tmp").build();
        config.completeAndValidateConfig();

        // Common parameters
        Assert.assertEquals(config.getPattern(), "ab*");
        Assert.assertEquals(config.getTemplate(), "tmp");
    }
}