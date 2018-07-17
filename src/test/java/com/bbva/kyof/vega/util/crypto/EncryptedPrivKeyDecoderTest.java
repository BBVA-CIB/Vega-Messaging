package com.bbva.kyof.vega.util.crypto;

import com.bbva.kyof.vega.TestConstants;
import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 08/03/2017.
 */
public class EncryptedPrivKeyDecoderTest
{
    @Test
    public void decryptPrivKey() throws Exception
    {
        final String privateKey = new String("This is a private key value");
        final byte[] privateKeyBytes = privateKey.getBytes();

        final byte[] encrypted = EncryptedPrivKeyDecoder.encryptPrivateKey(privateKeyBytes, TestConstants.PRIVATE_KEY_PASSWORD_HEX);
        final byte[] decrypted = EncryptedPrivKeyDecoder.decryptPrivKey(TestConstants.PRIVATE_KEY_PASSWORD_HEX, encrypted);

        Assert.assertEquals(privateKey, new String(decrypted));
    }

    @Test(expected = VegaException.class)
    public void encryptWrongKeySize() throws Exception
    {
        EncryptedPrivKeyDecoder.encryptPrivateKey("AA".getBytes(), "AA");
    }

    @Test(expected = VegaException.class)
    public void encryptWrongKeyValues() throws Exception
    {
        EncryptedPrivKeyDecoder.encryptPrivateKey("AA".getBytes(), TestConstants.PRIVATE_KEY_PASSWORD_HEX.replace("A", "M"));
    }
}