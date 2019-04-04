package com.bbva.kyof.vega;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;

/**
 * {@link Version} class test
 */
public class VersionTest
{
    @Test
    public void testIntegerRep() throws Exception
    {
        // Create 3 versions
        final int version = Version.toIntegerRepresentation((byte)34, (byte)45, (byte)67);
        final int localVersionCompatible = Version.toIntegerRepresentation((byte)1, (byte)5, (byte)1);
        final int compatibleVersion = Version.toIntegerRepresentation((byte)34, (byte)50, (byte)32);
        final int incompatibleVersion = Version.toIntegerRepresentation((byte)36, (byte)45, (byte)67);

        // Check the independent values
        Assert.assertEquals(Version.getMayorFromIntRepresentation(version), (byte)34);
        Assert.assertEquals(Version.getMinorFromIntRepresentation(version), (byte)45);
        Assert.assertEquals(Version.getPatchFromIntRepresentation(version), (byte)67);

        // Check compatibility
        Assert.assertTrue(Version.areCompatible(version, compatibleVersion));
        Assert.assertFalse(Version.areCompatible(version, incompatibleVersion));

        // Check local compatibility
        Assert.assertTrue(Version.isCompatibleWithLocal(localVersionCompatible));
        Assert.assertFalse(Version.isCompatibleWithLocal(incompatibleVersion));

        // Finally check string representation
        Assert.assertEquals(Version.toStringRep(version), "34.45.67");
    }

    @Test
    public void testConstructor() throws Exception
    {
        Constructor<?>[] cons = Version.class.getDeclaredConstructors();
        cons[0].setAccessible(true);
        cons[0].newInstance((Object[]) null);
    }
}