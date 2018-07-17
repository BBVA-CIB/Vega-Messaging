package com.bbva.kyof.vega.protocol.common;

/**
 * Enum that represents all the key storage security models available.
 *
 * The keys can be stored as plain XML files, or only encrypt the private key or use a keystore
 */
public enum KeySecurityType
{
    /** Key files stored as plain XML text files */
    PLAIN_KEY_FILE,
    /** Private key file stored encrypted, public key files stored in plain */
    ENCRYPTED_KEY_FILE,
    /** Use a keystore to store all keys using digital certificates */
    KEYSTORE
}
