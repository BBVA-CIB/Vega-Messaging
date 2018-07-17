package com.bbva.kyof.vega.util.crypto;

import lombok.Getter;

/**
 * Available signature codec types
 */
enum SignatureCodecType
{
    /** RSA algorithm type */
    SHA_WITH_RSA("SHA1withRSA");

    /** The String value of the codec type in a format that is understable by the Java Cipher */
    @Getter
    private final String stringValue;

    /**
     * Create a new codec type given the string value
     * @param stringValue the string value for the type
     */
    SignatureCodecType(final String stringValue)
    {
        this.stringValue = stringValue;
    }
}
