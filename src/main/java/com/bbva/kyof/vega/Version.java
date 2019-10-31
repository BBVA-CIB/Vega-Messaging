package com.bbva.kyof.vega;

/**
 * This class contains information about the version of the library
 */
public final class Version
{
    /** Mayor library version of current code */
    private static final byte LOCAL_MAYOR = 2;
    /** Minor library version of current code */
    private static final byte LOCAL_MINOR = 3;
    /** Patch library version of current code */
    private static final byte LOCAL_PATCH = 0;

    /** Current version of the library */
    public static final int LOCAL_VERSION = Version.toIntegerRepresentation(LOCAL_MAYOR, LOCAL_MINOR, LOCAL_PATCH);

    /** Private constructor to avoid instantiation */
    private Version()
    {
        // Nothing to do
    }

    /**
     * Convert the given version values to the corresponding integer representation
     *
     * @param mayor library version of current code
     * @param minor library version of current code
     * @param path library version of current code
     * @return the given version in its integer representation
     */
    public static int toIntegerRepresentation(final byte mayor, final byte minor, final byte path)
    {
        return ((int) mayor & 0xff) | (((int) minor & 0xff) << 8)
                | (((int) path & 0xff) << 16);
    }

    /**
     * Get the mayor version from the int representation
     *
     * @param intValue version in its integer representation
     * @return mayor library version of current code
     */
    public static byte getMayorFromIntRepresentation(final int intValue)
    {
        return (byte) (intValue & 0xFF);
    }

    /**
     * Get the minor version from the int representation
     *
     * @param intValue version in its integer representation
     * @return minor library version of current code
     */
    public static byte getMinorFromIntRepresentation(final int intValue)
    {
        return (byte) ((intValue >> 8) & 0xFF);
    }

    /**
     * Get the path version from the int representation
     *
     * @param intValue version in its integer representation
     * @return path library version of current code
     */
    public static byte getPatchFromIntRepresentation(final int intValue)
    {
        return (byte) ((intValue >> 16) & 0xFF);
    }

    /**
     * Return true if the given versions are compatible
     *
     * @param version1 to test
     * @param version2 to test
     * @return true if the given versions are compatible
     */
    public static boolean areCompatible(final int version1, final int version2)
    {
        return getMayorFromIntRepresentation(version1) == getMayorFromIntRepresentation(version2);
    }

    /**
     * Return true if the given version is compatible with the current instance version
     *
     * @param version1 given version to test
     * @return true if the given version is compatible with the current instance version
     */
    public static boolean isCompatibleWithLocal(final int version1)
    {
        return getMayorFromIntRepresentation(version1) == LOCAL_MAYOR;
    }

    /**
     * Convert the version int format to String representation
     *
     * @param intValue  given version to test
     * @return the version to String representation
     */
    public static String toStringRep(final int intValue)
    {
        return String.format("%d.%d.%d",
                (byte) (intValue & 0xFF),
                (byte) ((intValue >> 8) & 0xFF),
                (byte) ((intValue >> 16) & 0xFF));
    }
}
