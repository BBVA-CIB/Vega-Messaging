package com.bbva.kyof.vega.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 29/07/16.
 */
public class PatternEqualsTest
{
    @Test
    public void matches() throws Exception
    {
        final PatternEquals pattern1 = new PatternEquals("a*.a");
        final PatternEquals pattern2 = new PatternEquals("b*.a");
        final PatternEquals pattern3 = new PatternEquals("a*.a");

        Assert.assertTrue(pattern1.matches("aaa"));
        Assert.assertFalse(pattern1.matches("baa"));

        Assert.assertFalse(pattern1.equals(pattern2));
        Assert.assertFalse(pattern1.equals(null));
        Assert.assertFalse(pattern1.equals(new Object()));

        Assert.assertTrue(pattern1.equals(pattern1));
        Assert.assertTrue(pattern1.equals(pattern3));
        Assert.assertEquals(pattern1.hashCode(), pattern3.hashCode());
    }
}