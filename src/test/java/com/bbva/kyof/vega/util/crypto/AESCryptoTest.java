package com.bbva.kyof.vega.util.crypto;

import com.bbva.kyof.vega.exception.VegaException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

public class AESCryptoTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AESCryptoTest.class);

    private static final int NUM_TESTS = 10000;
    private static final int MSG_SIZE = 256;
    private static final int BYTE_BUFFER_RESULT_SIZE = 272;
    private final static Random RND = new Random(System.currentTimeMillis());

    private static AESCrypto AES_CRYPTO;

    @BeforeClass
    public static void testCreateNewInstance() throws Exception
    {
        // Create an instance of each type and make sure it works fine
        AES_CRYPTO = AESCrypto.createNewInstance();
    }

    @Test
    public void testGetAESKey() throws Exception
    {
        Assert.assertEquals(AES_CRYPTO.getAESKey().length * 8, 128);
    }

    @Test
    public void testEncodeDecodeSingleByteArray() throws Exception
    {
        // Create the 3 buffers
        final ByteBuffer msgBuffer = ByteBuffer.allocate(256);
        final ByteBuffer encodedMsgBuffer = ByteBuffer.allocate(256);
        final ByteBuffer decodedBuffer = ByteBuffer.allocate(256);

        // Now move the positions and mark the status
        msgBuffer.position(8);
        encodedMsgBuffer.position(8);
        decodedBuffer.position(8);

        // Write something and test the encode / decode
        msgBuffer.put("Test Message".getBytes());
        msgBuffer.limit(msgBuffer.position());
        msgBuffer.position(8);

        AES_CRYPTO.encode(msgBuffer, encodedMsgBuffer);

        // Now decode
        encodedMsgBuffer.limit(encodedMsgBuffer.position());
        encodedMsgBuffer.position(8);
        AES_CRYPTO.decode(encodedMsgBuffer, decodedBuffer);

        // Check if the results are the same
        msgBuffer.position(8);
        decodedBuffer.limit(decodedBuffer.position());
        decodedBuffer.position(8);
        Assert.assertEquals(msgBuffer, decodedBuffer);
    }

    @Test
    public void testEncodeDecodeByteArray() throws Exception
    {
        final byte[][] messages = this.createRandomMsgs();
        final byte[][] encodedMsgs = new byte[NUM_TESTS][];

        // Warm up
        for(int i = 0; i < NUM_TESTS; i++)
        {
            encodedMsgs[i] = AES_CRYPTO.encode(messages[i]);
        }

        // Measure
        long startTime = System.nanoTime();
        for(int i = 0; i < NUM_TESTS; i++)
        {
            AES_CRYPTO.encode(messages[i]);
        }
        long stopTime = System.nanoTime();

        LOGGER.info("Byte Array AES encryption time per msg [{}] nanos", ((stopTime - startTime) / NUM_TESTS));

        // Now decode the messages
        for(int i = 0; i < NUM_TESTS; i++)
        {
            assertArrayEquals(AES_CRYPTO.decode(encodedMsgs[i]), messages[i]);
        }

        // Finally lets do a performance test
        startTime = System.nanoTime();
        for(int i = 0; i < NUM_TESTS; i++)
        {
            AES_CRYPTO.decode(encodedMsgs[i]);
        }
        stopTime = System.nanoTime();

        LOGGER.info("Byte Array AES decryption time per msg [{}] nanos", ((stopTime - startTime) / NUM_TESTS));
    }

    @Test
    public void testEncodeDecodeByteBuffer() throws Exception
    {
        final ByteBuffer[] messages = this.createRandomByteBufferMsgs();
        final ByteBuffer[] encodedMsgs = this.createEmptyByteBufferArrays(BYTE_BUFFER_RESULT_SIZE);
        final ByteBuffer[] decodedMsgs = this.createEmptyByteBufferArrays(BYTE_BUFFER_RESULT_SIZE);

        // Warm up
        for(int i = 0; i < NUM_TESTS; i++)
        {
            AES_CRYPTO.encode(messages[i], encodedMsgs[i]);
        }

        // Reset the buffers positions
        this.flipByteBufferArray(messages);
        this.flipByteBufferArray(encodedMsgs);

        // Measure
        long startTime = System.nanoTime();
        for(int i = 0; i < NUM_TESTS; i++)
        {
            AES_CRYPTO.encode(messages[i], encodedMsgs[i]);
        }
        long stopTime = System.nanoTime();

        LOGGER.info("ByteBuffer AES encryption time per msg [{}] nanos", ((stopTime - startTime) / NUM_TESTS));

        // Reset the buffers positions
        this.flipByteBufferArray(messages);
        this.flipByteBufferArray(encodedMsgs);

        // Now decode the messages
        for(int i = 0; i < NUM_TESTS; i++)
        {
            AES_CRYPTO.decode(encodedMsgs[i], decodedMsgs[i]);

            // Extract the arrays and make sure they are equals
            byte[] originalMsgArray = messages[i].array();
            byte[] decodedMsgArray = new byte[originalMsgArray.length];
            decodedMsgs[i].flip();
            decodedMsgs[i].get(decodedMsgArray);

            assertArrayEquals(originalMsgArray, decodedMsgArray);
        }

        // Reset the buffer positions
        this.flipByteBufferArray(encodedMsgs);
        this.resetByteBufferArray(decodedMsgs);

        // Finally lets do a performance test
        startTime = System.nanoTime();
        for(int i = 0; i < NUM_TESTS; i++)
        {
            AES_CRYPTO.decode(encodedMsgs[i], decodedMsgs[i]);
        }
        stopTime = System.nanoTime();

        LOGGER.info("ByteBuffer AES decryption time per msg [{}] nanos", ((stopTime - startTime) / NUM_TESTS));
    }

    @Test
    public void testEncodeWithNotEnoughSpace() throws Exception
    {
        // Create the 3 buffers
        final ByteBuffer msgBuffer = ByteBuffer.wrap("Test Message".getBytes());
        final ByteBuffer encodedMsgBuffer = ByteBuffer.allocate(2);

        // Write something and test the encode / decode
        try
        {
            AES_CRYPTO.encode(msgBuffer, encodedMsgBuffer);
        }
        catch (VegaException e)
        {
            // Now with enough space to check there is no problem with the crypto after an error
            AES_CRYPTO.encode(msgBuffer, ByteBuffer.allocate(128));
            return;
        }

        Assert.assertFalse("Should have failed due to an exception", true);
    }

    @Test
    public void testEncodeDecodeMultipleSizes() throws Exception
    {
        for(int i = 0; i < 4098 * 2; i++)
        {
            // Create the 3 buffers
            final byte[] msg = new byte[i];
            RND.nextBytes(msg);

            Assert.assertArrayEquals(AES_CRYPTO.decode(AES_CRYPTO.encode(msg)), msg);
        }
    }

    @Test
    public void testDecodeWithNotEnoughSpace() throws Exception
    {
        // Create the 3 buffers
        final ByteBuffer msgBuffer = ByteBuffer.wrap("Test Message".getBytes());
        final ByteBuffer encodedMsgBuffer = ByteBuffer.allocate(128);

        // Write something and test the encode / decode
        AES_CRYPTO.encode(msgBuffer, encodedMsgBuffer);
        encodedMsgBuffer.flip();

        // Now decode
        try
        {
            AES_CRYPTO.decode(encodedMsgBuffer, ByteBuffer.allocate(4));
        }
        catch (VegaException e)
        {
            AES_CRYPTO.decode(encodedMsgBuffer, ByteBuffer.allocate(128));
        }
    }

    @Test
    public void testDecodeWrongPadding() throws Exception
    {
        // Create the 3 buffers
        final ByteBuffer msgBuffer = ByteBuffer.wrap("Test Message".getBytes());
        final ByteBuffer encodedMsgBuffer = ByteBuffer.allocate(128);

        // Write something and test the encode / decode
        AES_CRYPTO.encode(msgBuffer, encodedMsgBuffer);

        // Now decode
        try
        {
            AES_CRYPTO.decode(encodedMsgBuffer, ByteBuffer.allocate(4));
        }
        catch (VegaException e)
        {
            encodedMsgBuffer.flip();
            AES_CRYPTO.decode(encodedMsgBuffer, ByteBuffer.allocate(128));
        }
    }

    @Test(expected = VegaException.class)
    public void testDecodeWrongPaddingArray() throws Exception
    {
        // Create the 3 buffers
        final ByteBuffer msgBuffer = ByteBuffer.wrap("Test Message".getBytes());

        // Now decode
        AES_CRYPTO.decode(msgBuffer.array());
    }

    @Test(expected = VegaException.class)
    public void testAesInvalidKey() throws Exception
    {
        new AESCrypto(new byte[34]);
    }

    @Test
    public void testExpectedEncryptedSize() throws Exception
    {
        final ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(32);
        buffer.limit(64);
        final byte[] array = new byte[32];

        Assert.assertEquals(AES_CRYPTO.expectedEncryptedSize(buffer), AES_CRYPTO.expectedEncryptedSize(array));
    }

    private byte[][] createRandomMsgs()
    {
        // Create
        final byte[][] messages = new byte[NUM_TESTS][];

        for(int i = 0; i < NUM_TESTS; i++)
        {
            final byte[] currentMessage = new byte[MSG_SIZE];
            RND.nextBytes(currentMessage);
            messages[i] = currentMessage;
        }

        return messages;
    }

    private ByteBuffer[] createEmptyByteBufferArrays(final int size)
    {
        // Create
        final ByteBuffer[] result = new ByteBuffer[NUM_TESTS];

        for(int i = 0; i < NUM_TESTS; i++)
        {
            result[i] = ByteBuffer.allocate(size);
        }

        return result;
    }

    /** Create an array of random byte buffer messages */
    private ByteBuffer[] createRandomByteBufferMsgs()
    {
        // Create
        final ByteBuffer[] messages = new ByteBuffer[NUM_TESTS];

        for(int i = 0; i < NUM_TESTS; i++)
        {
            final ByteBuffer currentMessage = ByteBuffer.allocate(MSG_SIZE);
            RND.nextBytes(currentMessage.array());
            currentMessage.position(0);
            currentMessage.limit(currentMessage.capacity());
            messages[i] = currentMessage;
        }

        return messages;
    }

    /** Reset the given array of byte buffer positions to 0 */
    private void flipByteBufferArray(final ByteBuffer[] array)
    {
        for(int i = 0; i < NUM_TESTS; i++)
        {
            array[i].flip();
        }
    }

    /** Reset the given array of byte buffer positions to 0 */
    private void resetByteBufferArray(final ByteBuffer[] array)
    {
        for(int i = 0; i < NUM_TESTS; i++)
        {
            array[i].clear();
        }
    }
}