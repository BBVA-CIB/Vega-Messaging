package com.bbva.kyof.vega.autodiscovery.daemon;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.junit.*;

import java.util.UUID;

/**
 * Created by cnebrera on 05/08/16.
 */
public class UnicastDaemonTest
{
    private final static SubnetAddress SUBNET = InetUtil.getDefaultSubnet();
    private final static String IP = SUBNET.getIpAddres().getHostAddress();

    private static UnicastDaemon DAEMON;
    private static MediaDriver CLIENTS_MEDIA_DRIVER;
    private static Aeron CLIENTS_AERON;

    private final static int PORT_CLIENT_1 = 23400;
    private final static int PORT_CLIENT_2 = 23401;
    private final static int PORT_DAEMON = 23403;
    private final static int CLIENTS_STREAM_ID = 10;

    private UnicastDaemonClientSimulator clientSimulator1;
    private UnicastDaemonClientSimulator clientSimulator2;
    private UnicastDaemonClientSimulator clientSimulator3;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        // Create the daemon with an embedded media driver
        final DaemonParameters daemonParameters = DaemonParameters.builder().
                subnet(SUBNET.toString()).
                port(PORT_DAEMON).
                clientTimeout(5000L).
                aeronDriverType(DaemonParameters.AeronDriverType.EMBEDDED).
                build();

        daemonParameters.completeAndValidateParameters();
        DAEMON = new UnicastDaemon(daemonParameters);
        DAEMON.start("UnicastDaemon");

        // Initialize the shared Aeron driver for clients
        CLIENTS_MEDIA_DRIVER = MediaDriver.launchEmbedded();
        final Aeron.Context ctx = new Aeron.Context();
        ctx.aeronDirectoryName(CLIENTS_MEDIA_DRIVER.aeronDirectoryName());
        CLIENTS_AERON = Aeron.connect(ctx);
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        DAEMON.close();
        CLIENTS_AERON.close();
        CloseHelper.quietClose(CLIENTS_MEDIA_DRIVER);
    }

    @Before
    public void setUp() throws Exception
    {
        // Create 3 clients, 2 sharing the channel and stream
        this.clientSimulator1 = new UnicastDaemonClientSimulator(CLIENTS_AERON, IP, PORT_CLIENT_1, PORT_DAEMON, CLIENTS_STREAM_ID, SUBNET);
        this.clientSimulator1.start("ClientSimulator1");

        this.clientSimulator2 = new UnicastDaemonClientSimulator(CLIENTS_AERON, IP, PORT_CLIENT_1, PORT_DAEMON, CLIENTS_STREAM_ID, SUBNET);
        this.clientSimulator2.start("ClientSimulator2");

        this.clientSimulator3 = new UnicastDaemonClientSimulator(CLIENTS_AERON, IP, PORT_CLIENT_2, PORT_DAEMON, CLIENTS_STREAM_ID, SUBNET);
        this.clientSimulator3.start("ClientSimulato3");

        Thread.sleep(2000);
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
    public void onMultipleAutoDiscDaemonClientInfo() throws Exception
    {
        // Create a message to forward and send it, the client should not receive it yet
        final AutoDiscTopicInfo topicInfoMsg = new AutoDiscTopicInfo(UUID.randomUUID(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic");

        // Send the message, should not be forward
        clientSimulator1.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfoMsg, false);

        Thread.sleep(500);
        Assert.assertTrue(clientSimulator1.getNumRcvTopicInfoMsgs() == 0);
        Assert.assertTrue(clientSimulator2.getNumRcvTopicInfoMsgs() == 0);
        Assert.assertTrue(clientSimulator3.getNumRcvTopicInfoMsgs() == 0);

        // Now send a client info for client simulator 3
        clientSimulator3.sendClientInfo(false);

        // Wait for message arrival and process
        Thread.sleep(1000);

        // Send a message again, not it should arrive only to client 3
        clientSimulator1.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfoMsg, false);

        // Wait for message arrival and process
        Thread.sleep(1000);
        Assert.assertTrue(clientSimulator1.getNumRcvTopicInfoMsgs() == 0);
        Assert.assertTrue(clientSimulator2.getNumRcvTopicInfoMsgs() == 0);
        Assert.assertTrue(clientSimulator3.getNumRcvTopicInfoMsgs() == 1);

        // Connect now client 1, it shares reception with client 2
        clientSimulator1.sendClientInfo(false);
        Thread.sleep(1000);

        // Send a message again, not it should arrive to all
        clientSimulator1.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfoMsg, false);
        // Wait for message arrival and process
        Thread.sleep(1000);
        Assert.assertTrue(clientSimulator1.getNumRcvTopicInfoMsgs() == 1);
        Assert.assertTrue(clientSimulator2.getNumRcvTopicInfoMsgs() == 1);
        Assert.assertTrue(clientSimulator3.getNumRcvTopicInfoMsgs() == 2);

        // If we wait 300 millis, the client 2 should have timed out. Refresh client simulator 1 to prevent timeout
        Thread.sleep(1500);
        clientSimulator1.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfoMsg, false);
        clientSimulator1.sendClientInfo(false);
        Thread.sleep(500);

        Assert.assertTrue(clientSimulator1.getNumRcvTopicInfoMsgs() == 2);
        Assert.assertTrue(clientSimulator2.getNumRcvTopicInfoMsgs() == 2);
        Assert.assertTrue(clientSimulator3.getNumRcvTopicInfoMsgs() == 2);

        // Wait 500 more, the client 1 should not have timed out, we just updated it
        Thread.sleep(2500);
        clientSimulator1.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfoMsg, false);
        Thread.sleep(500);
        Assert.assertTrue(clientSimulator1.getNumRcvTopicInfoMsgs() == 3);
        Assert.assertTrue(clientSimulator2.getNumRcvTopicInfoMsgs() == 3);
        Assert.assertTrue(clientSimulator3.getNumRcvTopicInfoMsgs() == 2);

        // Wait 500 more, now it should be timed out
        Thread.sleep(2500);
        clientSimulator1.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfoMsg, false);
        Thread.sleep(500);
        Assert.assertTrue(clientSimulator1.getNumRcvTopicInfoMsgs() == 3);
        Assert.assertTrue(clientSimulator2.getNumRcvTopicInfoMsgs() == 3);
        Assert.assertTrue(clientSimulator3.getNumRcvTopicInfoMsgs() == 2);
    }
}