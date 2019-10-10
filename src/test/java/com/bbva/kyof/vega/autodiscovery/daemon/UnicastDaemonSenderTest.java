package com.bbva.kyof.vega.autodiscovery.daemon;

import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.msg.BaseHeader;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.serialization.IUnsafeSerializable;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.junit.*;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by cnebrera on 05/08/16.
 */
public class UnicastDaemonSenderTest
{
    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static UnicastDaemonSender DAEMON_SENDER;

    private final static SubnetAddress SUBNET = InetUtil.getDefaultSubnet();
    private final static String IP = SUBNET.getIpAddres().getHostAddress();
    private final static UUID DAEMON_ID = UUID.randomUUID();

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(1024);
    private final UnsafeBufferSerializer sendBufferSerializer = new UnsafeBufferSerializer();

    private final static int PORT_CLIENT_1 = 23400;
    private final static int PORT_CLIENT_2 = 23401;
    private final static int PORT_DAEMON = 23403;
    private final static int CLIENTS_STREAM_ID = 20;
    private UnicastDaemonClientSimulator clientSimulator1;
    private UnicastDaemonClientSimulator clientSimulator2;
    private UnicastDaemonClientSimulator clientSimulator3;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();
        final Aeron.Context ctx = new Aeron.Context();
        ctx.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());
        AERON = Aeron.connect(ctx);

        DaemonParameters daemonParameters = DaemonParameters.builder().
                subnet(SUBNET.toString()).
                port(PORT_DAEMON).
                clientTimeout(500L).
                aeronDriverType(DaemonParameters.AeronDriverType.EMBEDDED).
                build();

        daemonParameters.completeAndValidateParameters();
        DAEMON_SENDER = new UnicastDaemonSender(AERON, daemonParameters, UUID.randomUUID());
    }

    @AfterClass
    public static void afterClass()
    {
        DAEMON_SENDER.close();
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Before
    public void setUp()
    {
        // Create 3 clients, 2 sharing the channel and stream
        this.clientSimulator1 = new UnicastDaemonClientSimulator(AERON, IP, PORT_CLIENT_1, PORT_DAEMON, CLIENTS_STREAM_ID, SUBNET);
        this.clientSimulator1.start("ClientSimulator1");

        this.clientSimulator2 = new UnicastDaemonClientSimulator(AERON, IP, PORT_CLIENT_1, PORT_DAEMON, CLIENTS_STREAM_ID, SUBNET);
        this.clientSimulator2.start("ClientSimulator2");

        this.clientSimulator3 = new UnicastDaemonClientSimulator(AERON, IP, PORT_CLIENT_2, PORT_DAEMON, CLIENTS_STREAM_ID, SUBNET);
        this.clientSimulator3.start("ClientSimulato3");
    }

    @After
    public void tearDown() throws Exception
    {
        // Stop the clients
        clientSimulator1.close();
        clientSimulator2.close();
        clientSimulator3.close();
    }

    @Test
    public void onNewAutoDiscDaemonClientInfo() throws Exception
    {
        // Create a new DaemonClient
        final UnicastDaemonClientSimulator clientSimulator = new UnicastDaemonClientSimulator(AERON, IP, PORT_CLIENT_1, PORT_DAEMON, CLIENTS_STREAM_ID, SUBNET);
        clientSimulator.start("ClientSimulator");

        // Create a message to forward and send it, the client should not receive it yet
        final AutoDiscTopicInfo topicInfoMsg = new AutoDiscTopicInfo(DAEMON_ID, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic");
        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfoMsg, false);

        // Wait for message arrival
        Thread.sleep(100);
        Assert.assertTrue(clientSimulator.getNumRcvTopicInfoMsgs() == 0);

        // Now tell the daemon about the new client
        DAEMON_SENDER.onNewAutoDiscDaemonClientInfo(clientSimulator.getClientInfo());
        // If done twice, second time it wont be added
        DAEMON_SENDER.onNewAutoDiscDaemonClientInfo(clientSimulator.getClientInfo());

        // Wait a bit
        Thread.sleep(1000);

        // Try again
        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfoMsg, false);

        // Wait for message arrival
        Thread.sleep(100);
        Assert.assertTrue(clientSimulator.getNumRcvTopicInfoMsgs() == 1);
        Assert.assertEquals(clientSimulator.getLastReceivedTopicInfoMsg(), topicInfoMsg);

        // Try again
        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfoMsg, false);

        // Wait for message arrival
        Thread.sleep(100);
        Assert.assertTrue(clientSimulator.getNumRcvTopicInfoMsgs() == 2);
        Assert.assertEquals(clientSimulator.getLastReceivedTopicInfoMsg(), topicInfoMsg);

        // Remove the client, no more messages should arrive
        DAEMON_SENDER.onRemovedAutoDiscDaemonClientInfo(clientSimulator.getClientInfo());

        // If removed twice, second time it wont be removed
        DAEMON_SENDER.onRemovedAutoDiscDaemonClientInfo(clientSimulator.getClientInfo());

        // Wait a bit
        Thread.sleep(1000);

        // Wait for message arrival
        Thread.sleep(100);
        Assert.assertTrue(clientSimulator.getNumRcvTopicInfoMsgs() == 2);
    }

    @Test
    public void onMultipleAutoDiscDaemonClientInfo() throws Exception
    {
        // Create a message to forward and send it, the client should not receive it yet
        final AutoDiscTopicInfo topicInfoMsg = new AutoDiscTopicInfo(DAEMON_ID, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic");

        // Now tell the daemon about the new clients
        DAEMON_SENDER.onNewAutoDiscDaemonClientInfo(clientSimulator1.getClientInfo());
        DAEMON_SENDER.onNewAutoDiscDaemonClientInfo(clientSimulator2.getClientInfo());
        DAEMON_SENDER.onNewAutoDiscDaemonClientInfo(clientSimulator3.getClientInfo());

        // Wait a bit
        Thread.sleep(1000);

        // Send the message, it should have arrived to all of them
        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfoMsg, false);

        // Wait for message arrival
        Thread.sleep(100);
        Assert.assertTrue(clientSimulator1.getNumRcvTopicInfoMsgs() == 1);
        Assert.assertEquals(clientSimulator1.getLastReceivedTopicInfoMsg(), topicInfoMsg);
        Assert.assertTrue(clientSimulator2.getNumRcvTopicInfoMsgs() == 1);
        Assert.assertEquals(clientSimulator2.getLastReceivedTopicInfoMsg(), topicInfoMsg);
        Assert.assertTrue(clientSimulator3.getNumRcvTopicInfoMsgs() == 1);
        Assert.assertEquals(clientSimulator3.getLastReceivedTopicInfoMsg(), topicInfoMsg);

        // Now remove the second client, since it shares socket with the first, the publisher socket is not removed
        DAEMON_SENDER.onRemovedAutoDiscDaemonClientInfo(clientSimulator2.getClientInfo());

        // Send again
        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfoMsg, false);

        // Wait for message arrival
        Thread.sleep(100);
        Assert.assertTrue(clientSimulator1.getNumRcvTopicInfoMsgs() == 2);
        Assert.assertTrue(clientSimulator2.getNumRcvTopicInfoMsgs() == 2); // It still arrives
        Assert.assertTrue(clientSimulator3.getNumRcvTopicInfoMsgs() == 2);

        // Now remove the first client, now the socket is really removed
        DAEMON_SENDER.onRemovedAutoDiscDaemonClientInfo(clientSimulator1.getClientInfo());

        // Send again
        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfoMsg, false);

        // Wait for message arrival
        Thread.sleep(100);
        Assert.assertTrue(clientSimulator1.getNumRcvTopicInfoMsgs() == 2);
        Assert.assertTrue(clientSimulator2.getNumRcvTopicInfoMsgs() == 2);
        Assert.assertTrue(clientSimulator3.getNumRcvTopicInfoMsgs() == 3);
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
        DAEMON_SENDER.onNewMessageToFordward(this.sendBufferSerializer.getInternalBuffer(), 0, this.sendBufferSerializer.getOffset());
    }
}