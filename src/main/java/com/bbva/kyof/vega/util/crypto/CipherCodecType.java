package com.bbva.kyof.vega.util.crypto;

import lombok.Getter;

/**
 * Available cipher codec types
 */
enum CipherCodecType
{
    /** RSA algorithm type */
    RSA("RSA"),
    /** AES algorithm type */
    AES("AES");

    /** The String value of the codec type in a format that is understable by the Java Cipher */
    @Getter
    private final String stringValue;

    /**
     * Create a new codec type given the string value
     * @param stringValue the string value for the type
     */
    CipherCodecType(final String stringValue)
    {
        this.stringValue = stringValue;
    }
}
