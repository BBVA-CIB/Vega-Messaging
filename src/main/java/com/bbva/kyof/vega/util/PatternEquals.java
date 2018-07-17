package com.bbva.kyof.vega.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pattern with equals to be used on hashmaps
 */
public class PatternEquals
{
    /**
     * Original pattern wrapped
     */
    private final String patternRegex;

    /**
     * Pattern matcher
     */
    private final Matcher matcher;

    /** Lock for instance synchronization */
    private final Object lock = new Object();

    /**
     * Construct a new pattern equals with the given string
     * @param patternRegex the regex that represent the pattern
     */
    public PatternEquals(final String patternRegex)
    {
        this.patternRegex = patternRegex;
        this.matcher = Pattern.compile(patternRegex).matcher("");
    }

    /**
     * Compiles the given regular expression and attempts to match the given
     * input against it.
     *
     * @param input The input sequence
     * @return true if it matches TOTALLY, false otherwise
     */
    public boolean matches(final CharSequence input)
    {
        synchronized (this.lock)
        {
            this.matcher.reset(input);
            return this.matcher.matches();
        }
    }

    @Override
    public boolean equals(final Object target)
    {
        if (this == target)
        {
            return true;
        }
        if (target == null || getClass() != target.getClass())
        {
            return false;
        }

        PatternEquals that = (PatternEquals) target;

        return patternRegex.equals(that.patternRegex);

    }

    @Override
    public int hashCode()
    {
        return patternRegex.hashCode();
    }
}
