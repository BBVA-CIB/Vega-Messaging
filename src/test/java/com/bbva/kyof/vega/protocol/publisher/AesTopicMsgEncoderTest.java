package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.crypto.AESCrypto;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Created by cnebrera on 15/11/2016.
 */
public class AesTopicMsgEncoderTest
{


    @Test
    public void encryptDecrypt() throws Exception
    {
        final AesTopicMsgEncoder msgEncoder = new AesTopicMsgEncoder();
        AESCrypto aesDecoder = new AESCrypto(msgEncoder.getAESKey());

        // Create a message and encode
        Random rnd = new Random();
        testWithMsgSize(msgEncoder, aesDecoder, rnd, 128);
        testWithMsgSize(msgEncoder, aesDecoder, rnd, 512);
        testWithMsgSize(msgEncoder, aesDecoder, rnd, 1024);
        testWithMsgSize(msgEncoder, aesDecoder, rnd, 2048);
    }

    private void testWithMsgSize(AesTopicMsgEncoder msgEncoder, AESCrypto aesDecoder, Random rnd, int msgSize) throws VegaException
    {
        final byte[] msg = new byte[msgSize];
        rnd.nextBytes(msg);

        UnsafeBuffer msgBuffer = new UnsafeBuffer(msg);
        final ByteBuffer encodedMsg = msgEncoder.encryptMessage(msgBuffer, 0, msg.length);

        // If repeated the encodedMsg result should be the same instance
        Assert.assertTrue(encodedMsg == msgEncoder.encryptMessage(msgBuffer, 0, msg.length));

        // Decode, the result should be the same
        final ByteBuffer decodedMsg = ByteBuffer.allocate(4096);
        aesDecoder.decode(encodedMsg, decodedMsg);

        decodedMsg.flip();

        this.byteBufferEquals(msg, decodedMsg);
    }

    private void byteBufferEquals(final byte[] array, final ByteBuffer buffer)
    {
        Assert.assertTrue(array.length == buffer.limit());

        for (int i = 0; i < buffer.limit(); i++)
        {
            Assert.assertEquals(array[i], buffer.get(i));
        }
    }
}