package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.config.general.GlobalConfiguration;
import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by cnebrera on 11/08/16.
 */
public class AeronSubscriberTest
{
    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static SubnetAddress SUBNET_ADDRESS;
    private static VegaContext VEGA_CONTEXT;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx1 = new Aeron.Context();
        ctx1.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON = Aeron.connect(ctx1);

        SUBNET_ADDRESS = InetUtil.getDefaultSubnet();
        VEGA_CONTEXT = new VegaContext(AERON, new GlobalConfiguration());

        Thread.sleep(1000);
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Test
    public void testCreateSubscriptionAndGetMessage() throws Exception
    {
        final int mcastIp = InetUtil.convertIpAddressToInt("224.1.1.1");
        final int ucastIp = InetUtil.convertIpAddressToInt(SUBNET_ADDRESS.getIpAddres().getHostAddress());

        final AeronSubscriberParams ipcSubscriberParams = new AeronSubscriberParams(TransportMediaType.IPC, mcastIp, 0, 2, null);
        final AeronSubscriberParams mcastSubscriberParams = new AeronSubscriberParams(TransportMediaType.MULTICAST, mcastIp, 28889, 2, SUBNET_ADDRESS);
        final AeronSubscriberParams unicastSubscriberParams = new AeronSubscriberParams(TransportMediaType.UNICAST, ucastIp, 29333, 2, SUBNET_ADDRESS);

        final AeronSubscriber ipcSubscriber = new AeronSubscriber(VEGA_CONTEXT, ipcSubscriberParams);
        final AeronSubscriber mcastSubscriber = new AeronSubscriber(VEGA_CONTEXT, mcastSubscriberParams);
        final AeronSubscriber ucastSubscriber = new AeronSubscriber(VEGA_CONTEXT, unicastSubscriberParams);

        // Check get params
        Assert.assertEquals(mcastSubscriberParams, mcastSubscriber.getParams());

        // Now create the aeron publishers
        Publication ipcPublication = AERON.addPublication(AeronChannelHelper.createIpcChannelString(), 2);
        Publication mcastPublication = AERON.addPublication(AeronChannelHelper.createMulticastChannelString(mcastIp, 28889, SUBNET_ADDRESS), 2);
        Publication ucastPublication = AERON.addPublication(AeronChannelHelper.createUnicastChannelString(ucastIp, 29333, SUBNET_ADDRESS), 2);

        // Give it time to initialize
        Thread.sleep(1000);

        // Send some messages
        ByteBuffer buffer = ByteBuffer.allocate(128);
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(buffer);
        unsafeBuffer.putInt(0, 11);
        unsafeBuffer.putInt(4, 22);
        unsafeBuffer.putInt(8, 33);

        ipcPublication.offer(unsafeBuffer, 0, 4);
        mcastPublication.offer(unsafeBuffer, 4, 4);
        ucastPublication.offer(unsafeBuffer, 8, 4);

        // Wait a bit
        Thread.sleep(100);

        // Poll!!!
        AtomicInteger lastPolledMessage = new AtomicInteger();
        ipcSubscriber.poll((directBuffer, i, i1, header) -> lastPolledMessage.set(directBuffer.getInt(i)), 1);
        Assert.assertEquals(11, lastPolledMessage.get());
        mcastSubscriber.poll((directBuffer, i, i1, header) -> lastPolledMessage.set(directBuffer.getInt(i)), 1);
        Assert.assertEquals(22, lastPolledMessage.get());
        ucastSubscriber.poll((directBuffer, i, i1, header) -> lastPolledMessage.set(directBuffer.getInt(i)), 1);
        Assert.assertEquals(33, lastPolledMessage.get());

        // Close them and send messages again
        ipcSubscriber.close();
        ipcSubscriber.close();
        ucastSubscriber.close();
        ucastSubscriber.close();
        mcastSubscriber.close();
        mcastSubscriber.close();

        ipcPublication.offer(unsafeBuffer, 0, 4);
        mcastPublication.offer(unsafeBuffer, 4, 4);
        ucastPublication.offer(unsafeBuffer, 8, 4);

        Thread.sleep(100);

        // Poll, there should be nothing this time
        lastPolledMessage.set(0);
        ipcSubscriber.poll((directBuffer, i, i1, header) -> lastPolledMessage.set(directBuffer.getInt(i)), 1);
        Assert.assertEquals(0, lastPolledMessage.get());
        mcastSubscriber.poll((directBuffer, i, i1, header) -> lastPolledMessage.set(directBuffer.getInt(i)), 1);
        Assert.assertEquals(0, lastPolledMessage.get());
        ucastSubscriber.poll((directBuffer, i, i1, header) -> lastPolledMessage.set(directBuffer.getInt(i)), 1);
        Assert.assertEquals(0, lastPolledMessage.get());

        // Close the publications
        ipcPublication.close();
        mcastPublication.close();
        ucastPublication.close();
    }
}