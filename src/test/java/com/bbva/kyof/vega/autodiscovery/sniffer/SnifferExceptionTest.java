package com.bbva.kyof.vega.autodiscovery.sniffer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Testing for {@link SnifferException}
 */
public class SnifferExceptionTest
{
    @Test
    public void testConstructor1()
    {
        final SnifferException exception = new SnifferException("Channel not created. You must to create the context first, for create them.");

        assertEquals(exception.getMessage(), "Channel not created. You must to create the context first, for create them.");
    }
    @Test
    public void testConstructor2()
    {
        try
        {
            throw new NullPointerException("Exception!!!");
        }
        catch(final Exception e)
        {
            final SnifferException exception = new SnifferException(e);
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
        catch(final Exception e)
        {
            final SnifferException exception = new SnifferException("Exception message for testing.", e);
            assertEquals(exception.getMessage(), "Exception message for testing.");
            assertEquals(exception.getCause(), e);
        }

    }
}