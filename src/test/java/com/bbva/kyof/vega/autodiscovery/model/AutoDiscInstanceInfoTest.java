package com.bbva.kyof.vega.autodiscovery.model;

import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by cnebrera on 02/08/16.
 */
public class AutoDiscInstanceInfoTest
{

    final static int RESP_TRANSP_IP = 33;
    final static int RESP_TRANSP_PORT = 32;
    final static int RESP_TRANSP_STREAM_ID = 22;

    final static int CONTROL_TRANSP_IP = 55;
    final static int CONTROL_TRANSP_PORT = 56;
    final static int CONTROL_TRANSP_STREAM_ID = 66;

    @Test
    public void fromBinaryToBinary()
    {
        // Create the header
        final UUID uniqueId = UUID.randomUUID();

        final AutoDiscInstanceInfo instanceInfo = new AutoDiscInstanceInfo("name", uniqueId, RESP_TRANSP_IP, RESP_TRANSP_PORT, RESP_TRANSP_STREAM_ID, CONTROL_TRANSP_IP, CONTROL_TRANSP_PORT, CONTROL_TRANSP_STREAM_ID);

        // Create the buffer to serialize it
        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        UnsafeBufferSerializer serializer = new UnsafeBufferSerializer();
        serializer.wrap(buffer);

        // Write to binary
        instanceInfo.toBinary(serializer);

        // Check the current offset, should be the serialization size
        Assert.assertTrue(serializer.getOffset() == instanceInfo.serializedSize());

        // Flip the buffer
        buffer.limit(serializer.getOffset());
        buffer.position(0);

        // Wrap again and read
        serializer.wrap(buffer);

        AutoDiscInstanceInfo readedInfo = new AutoDiscInstanceInfo();
        readedInfo.fromBinary(serializer);

        // Check all values
        Assert.assertEquals(instanceInfo, instanceInfo);
        Assert.assertEquals(instanceInfo, readedInfo);
        Assert.assertNotEquals(instanceInfo, null);
        Assert.assertNotEquals(instanceInfo, new AutoDiscInstanceInfo("name", UUID.randomUUID(), RESP_TRANSP_IP, RESP_TRANSP_PORT, RESP_TRANSP_STREAM_ID, CONTROL_TRANSP_IP, CONTROL_TRANSP_PORT, CONTROL_TRANSP_STREAM_ID));
        Assert.assertNotNull(instanceInfo.toString());
        Assert.assertTrue(instanceInfo.hashCode() == readedInfo.hashCode());
        Assert.assertEquals(instanceInfo.getUniqueId(), uniqueId);
        Assert.assertEquals(instanceInfo.getInstanceName(), "name");
        Assert.assertTrue(instanceInfo.getResponseTransportIp() == RESP_TRANSP_IP);
        Assert.assertTrue(instanceInfo.getResponseTransportPort() == RESP_TRANSP_PORT);
        Assert.assertTrue(instanceInfo.getResponseTransportStreamId() == RESP_TRANSP_STREAM_ID);
        Assert.assertTrue(instanceInfo.getControlRcvTransportIp() == CONTROL_TRANSP_IP);
        Assert.assertTrue(instanceInfo.getControlRcvTransportPort() == CONTROL_TRANSP_PORT);
        Assert.assertTrue(instanceInfo.getControlRcvTransportStreamId() == CONTROL_TRANSP_STREAM_ID);

        // Check again the limits
        Assert.assertTrue(serializer.getOffset() == readedInfo.serializedSize());
    }
}