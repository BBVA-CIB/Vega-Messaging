/**
 * 
 */
package com.bbva.kyof.vega.serialization;

import org.junit.Test;

import java.lang.reflect.Constructor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test coverage for LLUSurrogate class
 * @author psm
 *
 */
public class SurrogateTest
{
	@Test
	public void testConstructor() throws Exception
	{
		Constructor<StringSurrogate> c = StringSurrogate.class.getDeclaredConstructor();
		c.setAccessible(true);
		StringSurrogate instance = c.newInstance();
	}

	@Test
	public void testIsSurrogate()
	{
		assertFalse(StringSurrogate.isSurrogate('\uD700'));
	}

	@Test
	public void testHigh()
	{
		//To Do
		StringSurrogate.high(0xd700);
	}

	@Test
	public void testLow()
	{
		assertEquals(StringSurrogate.low(0xdc00), 0xdc00);
	}

	@Test
	public void testParse()
	{
		final String testString = '\uD800' + '\uDFFF' + "akjsdlkajsLSDHFSDYFHWENWE54678754677·$%·$&/·/$!{}]";

		for (int i = 0; i < testString.length() - 1; i++)
		{
			StringSurrogate.parse(testString.charAt(i), testString.charAt(i++));
		}
	}
}
