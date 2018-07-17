package com.bbva.kyof.vega.exception;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Testing for {@link VegaException}
 */
public class VegaExceptionTest
{
    @Test
    public void testConstructor1()
    {
        final VegaException exception = new VegaException("Channel not created. You must to create the context first, for create them.");

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
            final VegaException exception = new VegaException(e);
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
            final VegaException exception = new VegaException("Exception message for testing.", e);
            assertEquals(exception.getMessage(), "Exception message for testing.");
            assertEquals(exception.getCause(), e);
        }

    }
}