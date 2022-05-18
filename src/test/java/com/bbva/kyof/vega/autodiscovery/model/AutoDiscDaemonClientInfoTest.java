package com.bbva.kyof.vega.autodiscovery.model;

import com.bbva.kyof.vega.TestConstants;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by cnebrera on 02/08/16.
 */
public class AutoDiscDaemonClientInfoTest
{
    final static int UNI_RESOL_CLIENT_IP = 44;
    final static int UNI_RESOL_CLIENT_PORT = 33;
    final static int UNI_RESOL_STREAM_ID = 22;
    final static String UNI_RESOL_CLIENT_HOSTNAME = "host_512";
    @Test
    public void fromBinaryToBinary()
    {
        // Create the header
        final UUID uniqueId = UUID.randomUUID();
        final AutoDiscDaemonClientInfo daemonClientInfo = new AutoDiscDaemonClientInfo(uniqueId, UNI_RESOL_CLIENT_IP, UNI_RESOL_CLIENT_PORT, UNI_RESOL_STREAM_ID, UNI_RESOL_CLIENT_HOSTNAME);

        // Create the buffer to serialize it
        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        UnsafeBufferSerializer serializer = new UnsafeBufferSerializer();
        serializer.wrap(buffer);

        // Write to binary
        daemonClientInfo.toBinary(serializer);

        // Check the current offset, should be the serialization size
        Assert.assertEquals(serializer.getOffset(), daemonClientInfo.serializedSize());

        // Flip the buffer
        buffer.limit(serializer.getOffset());
        buffer.position(0);

        // Wrap again and read
        serializer.wrap(buffer);

        AutoDiscDaemonClientInfo readedInfo = new AutoDiscDaemonClientInfo();
        readedInfo.fromBinary(serializer);

        // Check all values
        Assert.assertEquals(daemonClientInfo, daemonClientInfo);
        Assert.assertEquals(daemonClientInfo, readedInfo);
        Assert.assertNotEquals(daemonClientInfo, null);
        Assert.assertNotEquals(daemonClientInfo, new AutoDiscDaemonClientInfo(UUID.randomUUID(), 44, 33, 22,"otro"));
        Assert.assertNotNull(daemonClientInfo.toString());
        Assert.assertEquals(daemonClientInfo.hashCode(), readedInfo.hashCode());
        Assert.assertEquals(daemonClientInfo.getUniqueId(), uniqueId);
        Assert.assertEquals(UNI_RESOL_CLIENT_IP, daemonClientInfo.getUnicastResolverClientIp());
        Assert.assertEquals(UNI_RESOL_CLIENT_PORT, daemonClientInfo.getUnicastResolverClientPort());
        Assert.assertEquals(UNI_RESOL_STREAM_ID, daemonClientInfo.getUnicastResolverClientStreamId());
        Assert.assertEquals(UNI_RESOL_CLIENT_HOSTNAME, daemonClientInfo.getUnicastResolverHostname() );

        // Check again the limits
        Assert.assertEquals(serializer.getOffset(), readedInfo.serializedSize());
    }
}