package com.bbva.kyof.vega.autodiscovery.exception;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by cnebrera on 29/07/16.
 */
public class AutodiscExceptionTest
{
    @Test
    public void testConstructor1()
    {
        final AutodiscException exception = new AutodiscException("Channel not created. You must to create the context first, for create them.");

        assertEquals(exception.getMessage(), "Channel not created. You must to create the context first, for create them.");
    }
    @Test
    public void testConstructor2()
    {
        try
        {
            throw new NullPointerException("Exception!!!");
        }
        catch(final java.lang.Exception e)
        {
            final AutodiscException exception = new AutodiscException(e);
            assertEquals(exception.getCause(), e);
        }
    }

    @Test
    public void testConstructor3()
    {
        try
        {
            throw new NullPointerException("Exception!!!");
        }
        catch(final java.lang.Exception e)
        {
            final AutodiscException exception = new AutodiscException("Exception message for testing.", e);
            assertEquals(exception.getMessage(), "Exception message for testing.");
            assertEquals(exception.getCause(), e);
        }
    }
}