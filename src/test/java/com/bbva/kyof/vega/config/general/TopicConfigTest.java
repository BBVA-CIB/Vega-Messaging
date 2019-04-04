package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 01/08/16.
 */
public class TopicConfigTest
{
    @Test
    public void empyConstructor() throws Exception
    {
        new TopicConfig();
    }

    @Test(expected = VegaException.class)
    public void validateEmptyBuilder() throws Exception
    {
        final TopicConfig invalidConfig = TopicConfig.builder().build();
        invalidConfig.completeAndValidateConfig();
    }

    @Test(expected = VegaException.class)
    public void validateMissingTemplate() throws Exception
    {
        final TopicConfig invalidConfig = TopicConfig.builder().pattern("ab").build();
        invalidConfig.completeAndValidateConfig();
    }

    @Test
    public void validateParams() throws Exception
    {
        final TopicConfig config = TopicConfig.builder().pattern("ab*").template("tmp").build();
        config.completeAndValidateConfig();

        // Common parameters
        Assert.assertEquals(config.getPattern(), "ab*");
        Assert.assertEquals(config.getTemplate(), "tmp");
    }
}