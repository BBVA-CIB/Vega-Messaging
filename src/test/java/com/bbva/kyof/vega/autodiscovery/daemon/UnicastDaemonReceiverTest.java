package com.bbva.kyof.vega.autodiscovery.daemon;

import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.autodiscovery.model.*;
import com.bbva.kyof.vega.msg.BaseHeader;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import com.bbva.kyof.vega.serialization.IUnsafeSerializable;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by cnebrera on 05/08/16.
 */
public class UnicastDaemonReceiverTest
{
    private static final UUID SENDER_INSTANCE_ID = UUID.randomUUID();

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(1024);
    private final UnsafeBufferSerializer sendBufferSerializer = new UnsafeBufferSerializer();

    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;

    private static UnicastDaemonReceiver RECEIVER;
    private static EventListener LISTENER = new EventListener();
    private static Publication PUBLICATION;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();
        final Aeron.Context ctx = new Aeron.Context();
        ctx.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());
        AERON = Aeron.connect(ctx);

        // Create the parameters
        DaemonParameters parameters = DaemonParameters.builder().clientTimeout(500L).aeronDriverType(DaemonParameters.AeronDriverType.EMBEDDED).build();
        parameters.completeAndValidateParameters();

        // Create the test receivers
        RECEIVER = new UnicastDaemonReceiver(AERON, parameters, LISTENER);

        // Create the publication
        final String ipcChannel = AeronChannelHelper.createUnicastChannelString(parameters.getIpAddress(), parameters.getPort(), parameters.getSubnetAddress());
        PUBLICATION = AERON.addPublication(ipcChannel, DaemonParameters.DEFAULT_STREAM_ID);

        // Wait and let the connections to be created
        Thread.sleep(2000);
    }

    @AfterClass
    public static void afterClass()
    {
        RECEIVER.close();
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Test
    public void testClientInfoMsgs() throws Exception
    {
        LISTENER.reset();

        // Create a publication to send messages to the deaemon
        final AutoDiscDaemonClientInfo daemonClientInfo = new AutoDiscDaemonClientInfo(UUID.randomUUID(), 34, 23, 33);

        // Send a message
        this.sendMessage(MsgType.AUTO_DISC_DAEMON_CLIENT_INFO, daemonClientInfo);
        Thread.sleep(100);
        this.callReceiverLifeCycle();

        // There should be now a new instance info event
        Assert.assertEquals(LISTENER.newClientInfo, daemonClientInfo);
        // And the onReceiveAutoDiscDaemonClientInfo event is called too
        Assert.assertEquals(LISTENER.receiveAutoDiscDaemonClientInfo, daemonClientInfo);
        LISTENER.reset();
        // Check if initialized correctly onReceiveAutoDiscDaemonClientInfo
        Assert.assertEquals(LISTENER.receiveAutoDiscDaemonClientInfo, null);

        // Now call again, since there has been no time out, there shouldn't be a new event
        this.sendMessage(MsgType.AUTO_DISC_DAEMON_CLIENT_INFO, daemonClientInfo);
        Thread.sleep(100);
        this.callReceiverLifeCycle();

        Assert.assertNull(LISTENER.newClientInfo);
        // The nonReceiveAutoDiscDaemonClientInfo event is called always
        Assert.assertEquals(LISTENER.receiveAutoDiscDaemonClientInfo, daemonClientInfo);
        Assert.assertNull(LISTENER.clientInfoRemoved);

        // Wait 300 millis, the time out is in 500, there should be no time out
        Thread.sleep(300);
        this.callReceiverLifeCycle();
        Assert.assertNull(LISTENER.clientInfoRemoved);

        // Now wait a bit more, there should be a time out
        Thread.sleep(300);
        this.callReceiverLifeCycle();
        Assert.assertEquals(LISTENER.clientInfoRemoved, daemonClientInfo);
        LISTENER.reset();
        // Check if initialized correctly onReceiveAutoDiscDaemonClientInfo
        Assert.assertEquals(LISTENER.receiveAutoDiscDaemonClientInfo, null);

        // If we send again, there should be a new element again
        this.sendMessage(MsgType.AUTO_DISC_DAEMON_CLIENT_INFO, daemonClientInfo);
        Thread.sleep(100);
        this.callReceiverLifeCycle();
        Assert.assertEquals(LISTENER.newClientInfo, daemonClientInfo);
        // The nonReceiveAutoDiscDaemonClientInfo event is called always
        Assert.assertEquals(LISTENER.receiveAutoDiscDaemonClientInfo, daemonClientInfo);
    }

    @Test
    public void testOtherInfoMsgs() throws Exception
    {
        LISTENER.reset();

        Assert.assertFalse(LISTENER.msgToFordward);

        final AutoDiscInstanceInfo instanceInfo = new AutoDiscInstanceInfo("instance1", UUID.randomUUID(), 12, 23, 55, 22, 33, 66);
        final AutoDiscTopicInfo topicInfo = new AutoDiscTopicInfo(SENDER_INSTANCE_ID, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic");
        final AutoDiscTopicSocketInfo topicSocketInfo = new AutoDiscTopicSocketInfo(SENDER_INSTANCE_ID, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic", UUID.randomUUID(), 1, 2, 4);

        // Send a message
        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfo);
        Thread.sleep(100);
        this.callReceiverLifeCycle();

        // There should be now a new instance info event
        Assert.assertTrue(LISTENER.msgToFordward);
        LISTENER.reset();

        // Send a message
        this.sendMessage(MsgType.AUTO_DISC_TOPIC_SOCKET, topicSocketInfo);
        Thread.sleep(100);
        this.callReceiverLifeCycle();

        // There should be now a new instance info event
        Assert.assertTrue(LISTENER.msgToFordward);
        LISTENER.reset();

        // Send a message
        this.sendMessage(MsgType.AUTO_DISC_INSTANCE, instanceInfo);
        Thread.sleep(100);
        this.callReceiverLifeCycle();

        // There should be now a new instance info event
        Assert.assertTrue(LISTENER.msgToFordward);
        LISTENER.reset();

        // Finally send wrong message
        this.sendMessage(MsgType.DATA, topicInfo);
        Thread.sleep(100);
        this.callReceiverLifeCycle();

        // There should be now a new instance info event
        Assert.assertFalse(LISTENER.msgToFordward);
        LISTENER.reset();

        // Send non compatible version
        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfo, true);
        Thread.sleep(100);
        this.callReceiverLifeCycle();
    }

    private void callReceiverLifeCycle()
    {
        RECEIVER.pollForNewMessages();
        RECEIVER.checkNextClientTimeout();
    }

    private void sendMessage(final byte msgType, final IUnsafeSerializable serializable)
    {
        this.sendMessage(msgType, serializable, false);
    }

    private void sendMessage(final byte msgType, final IUnsafeSerializable serializable, boolean wrongVersion)
    {
        // Prepare the send buffer
        this.sendBuffer.clear();
        this.sendBufferSerializer.wrap(this.sendBuffer);

        // Set msg type and write the base header
        BaseHeader baseHeader;

        if (wrongVersion)
        {
            baseHeader = new BaseHeader(msgType, Version.toIntegerRepresentation((byte)55, (byte)3, (byte)1));
        }
        else
        {
            baseHeader = new BaseHeader(msgType, Version.LOCAL_VERSION);
        }

        // Write the base header
        baseHeader.toBinary(this.sendBufferSerializer);

        // Serialize the message
        serializable.toBinary(this.sendBufferSerializer);

        // Send the message
        PUBLICATION.offer(this.sendBufferSerializer.getInternalBuffer(), 0, this.sendBufferSerializer.getOffset());
    }

    static class EventListener implements IDaemonReceiverListener
    {
        AutoDiscDaemonClientInfo newClientInfo;
        AutoDiscDaemonClientInfo clientInfoRemoved;
        boolean msgToFordward;
        AutoDiscDaemonClientInfo receiveAutoDiscDaemonClientInfo;

        @Override
        public void onRemovedAutoDiscDaemonClientInfo(AutoDiscDaemonClientInfo clientInfo)
        {
            clientInfoRemoved = clientInfo;
        }

        @Override
        public void onNewMessageToFordward(DirectBuffer buffer, int offset, int length)
        {
            msgToFordward = true;
        }

        @Override
        public void onNewAutoDiscDaemonClientInfo(AutoDiscDaemonClientInfo info)
        {
            newClientInfo = info;
        }

        @Override
        public void onReceiveAutoDiscDaemonClientInfo(AutoDiscDaemonClientInfo info)
        {
            receiveAutoDiscDaemonClientInfo = info;
        }

        public void reset()
        {
            newClientInfo = null;
            clientInfoRemoved = null;
            msgToFordward = false;
            receiveAutoDiscDaemonClientInfo = null;
        }
    }
}