package com.bbva.kyof.vega.serialization;

/**
 * Utility class for dealing with surrogates in UTF8 transformations
 */
final class StringSurrogate
{
    /** UTF-16 min high surrogate character */
    private static final char MIN_HIGH = '\uD800';
    
    /** UTF-16 max high surrogate character */
    private static final char MAX_HIGH = '\uDBFF';
    
    /** UTF-16 min low surrogate character */
    private static final char MIN_LOW  = '\uDC00';
    
    /** UTF-16 max low surrogate character */
    private static final char MAX_LOW  = '\uDFFF';
    
    /** Represents min high surrogate character*/
    private static final char MIN = MIN_HIGH;
    
    /** Represents max low surrogate character*/
    private static final char MAX = MAX_LOW;

    // Range of UCS-4 values that need surrogates in UTF-16
    //
    /** UCS4 min*/
    private static final int UCS4_MIN = 0x10000;

    /**
     * Private constructor
     */
    private StringSurrogate()
    {
    	// Empty constructor
    }

    /**
     * Tells whether or not the given UTF-16 value is a high surrogate.
     * 
     * @param charac
     * @return true if is higher surrogate
     */
    private static boolean isHigh(final int charac)
    {
        return (MIN_HIGH <= charac) && (charac <= MAX_HIGH);
    }

    /**
     * Tells whether or not the given UTF-16 value is a low surrogate.
     * 
     * @param charac
     * @return true if is lower surrogate
     */
    private static boolean isLow(final int charac)
    {
        return (MIN_LOW <= charac) && (charac <= MAX_LOW);
    }

    /**
     * Tells whether or not the given UTF-16 value is a surrogate character,
     * 
     * @param charac
     * @return true if the character is surrogate
     */
    static boolean isSurrogate(final int charac)
    {
        return (MIN <= charac) && (charac <= MAX);
    }

    /**
     * Returns the high UTF-16 surrogate for the given UCS-4 character.
	 *	
     * @param uChar
     * @return returns the highest surrogated character
     */
    static char high(final int uChar)
    {
        return (char)(0xd800 | (((uChar - UCS4_MIN) >> 10) & 0x3ff));
    }

    /**
     * Returns the low UTF-16 surrogate for the given UCS-4 character.
     * 
     * @param uChar
     * @return the lowest surrogated character
     */
    static char low(final int uChar)
    {
        return (char)(0xdc00 | ((uChar - UCS4_MIN) & 0x3ff));
    }

    /**
     * Converts the given surrogate pair into a 32-bit UCS-4 character.
     * 
     * @param char1 
     * @param char2
     * @return the converted character
     */
    private static int toUCS4(final char char1, final char char2)
    {
        return (((char1 & 0x3ff) << 10) | (char2 & 0x3ff)) + 0x10000;
    }

    /**
     * Parses the character knowing the next character to parse
     * @param character the current character
     * @param nextChar the next character
     * @return the parsed integer
     * @throws IllegalArgumentException exception thrown if there is a aproblem with the characters
     */
    static int parse(final char character, final char nextChar) throws IllegalArgumentException
    {
        if (StringSurrogate.isHigh(character))
        {
            if (StringSurrogate.isLow(nextChar))
            {
                return toUCS4(character, nextChar);
            }
            
            throw new IllegalArgumentException("Error serializing String to Utf8");
        }
        if (StringSurrogate.isLow(character))
        {
        	throw new IllegalArgumentException("Error serializing String to Utf8");
        }
        
        return character;
    }
}


