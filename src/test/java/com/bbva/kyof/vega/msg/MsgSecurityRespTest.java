package com.bbva.kyof.vega.msg;

import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.crypto.AESCrypto;
import com.bbva.kyof.vega.util.crypto.RSACrypto;
import com.bbva.kyof.vega.util.crypto.RSAKeysHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.KeyPair;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by cnebrera on 08/11/2016.
 */
public class MsgSecurityRespTest
{
    private final static UUID instanceId = UUID.randomUUID();
    private final static UUID requestId = UUID.randomUUID();
    private final static UUID topicPubId = UUID.randomUUID();
    private final static UUID targetInstanceId = UUID.randomUUID();

    private final UnsafeBuffer serializeBuffer = new UnsafeBuffer(new byte[1024]);

    private final UnsafeBufferSerializer bufferSerializer = new UnsafeBufferSerializer();

    private RSACrypto RSACrypto1;
    private RSACrypto RSACrypto2;
    private RSACrypto RSACrypto3;
    private byte[] sessionKey;
    private byte[] encodedSessionKey;

    @Before
    public void before() throws Exception
    {
        final KeyPair keyPair1 = RSAKeysHelper.generateKeyPair();
        final KeyPair keyPair2 = RSAKeysHelper.generateKeyPair();
        final KeyPair keyPair3 = RSAKeysHelper.generateKeyPair();

        // RSA Crypto public keys map
        Map<Integer, PublicKey> publicKeysMap = new HashMap<>();
        publicKeysMap.put(1, keyPair1.getPublic());
        publicKeysMap.put(2, keyPair2.getPublic());

        // RSA Crypto3 public keys map
        Map<Integer, PublicKey> publicKeysMap3 = new HashMap<>();
        publicKeysMap3.put(1, keyPair3.getPublic());

        // Now add the public keys to ensure the trusts in the applications following the Test Class description
        this.RSACrypto1 = new RSACrypto(keyPair1.getPrivate(), publicKeysMap);
        this.RSACrypto2 = new RSACrypto(keyPair2.getPrivate(), publicKeysMap);
        this.RSACrypto3 = new RSACrypto(keyPair3.getPrivate(), publicKeysMap3);

        // Initialize the session key
        this.sessionKey = AESCrypto.createNewInstance().getAESKey();
        this.encodedSessionKey = RSACrypto1.encodeWithPubKey(2, this.sessionKey);
    }

    @Test
    public void testGettersSettersSerializeDeserialize() throws Exception
    {
        // Create the message
        final MsgSecurityResp msgSecurity = new MsgSecurityResp();
        msgSecurity.setInstanceId(instanceId);
        msgSecurity.setSenderSecurityId(1);
        msgSecurity.setRequestId(requestId);
        msgSecurity.setTopicPublisherId(topicPubId);
        msgSecurity.setTargetVegaInstanceId(targetInstanceId);
        msgSecurity.setEncodedSessionKey(encodedSessionKey);

        // Sign and serialize
        bufferSerializer.wrap(serializeBuffer);
        msgSecurity.signAndSerialize(bufferSerializer, RSACrypto1);

        // Now deserialize
        bufferSerializer.setOffset(0);
        final MsgSecurityResp readedMsgSecurity = new MsgSecurityResp();
        readedMsgSecurity.fromBinary(bufferSerializer);

        // Both messages should be equals
        Assert.assertEquals(msgSecurity, readedMsgSecurity);
        Assert.assertEquals(msgSecurity.toString(), readedMsgSecurity.toString());

        // Test individual fields
        Assert.assertEquals(msgSecurity.getInstanceId(), readedMsgSecurity.getInstanceId());
        Assert.assertEquals(msgSecurity.getSenderSecurityId(), readedMsgSecurity.getSenderSecurityId());
        Assert.assertEquals(msgSecurity.getRequestId(), readedMsgSecurity.getRequestId());
        Assert.assertEquals(msgSecurity.getTopicPublisherId(), readedMsgSecurity.getTopicPublisherId());
        Assert.assertEquals(msgSecurity.getTargetVegaInstanceId(), readedMsgSecurity.getTargetVegaInstanceId());
        Assert.assertArrayEquals(msgSecurity.getEncodedSessionKey(), readedMsgSecurity.getEncodedSessionKey());

        // Verify the signature
        Assert.assertTrue(readedMsgSecurity.verifySignature(this.RSACrypto2));

        // If we repeat again, the unsigned contents should not need to be resized
        // Sign and serialize
        bufferSerializer.wrap(serializeBuffer);
        msgSecurity.signAndSerialize(bufferSerializer, RSACrypto1);

        // Now deserialize
        bufferSerializer.setOffset(0);
        readedMsgSecurity.fromBinary(bufferSerializer);

        // Both messages should be equals
        Assert.assertEquals(msgSecurity, readedMsgSecurity);

        // Verify the signature
        Assert.assertTrue(readedMsgSecurity.verifySignature(this.RSACrypto2));

        // Finally verify the signature with a wrong key
        Assert.assertFalse(readedMsgSecurity.verifySignature(this.RSACrypto3));
    }
}