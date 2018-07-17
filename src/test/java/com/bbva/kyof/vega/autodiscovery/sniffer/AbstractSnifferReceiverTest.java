package com.bbva.kyof.vega.autodiscovery.sniffer;

import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.msg.BaseHeader;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.serialization.IUnsafeSerializable;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.net.AeronChannelHelper;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.junit.*;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by cnebrera on 04/08/16.
 */
public class AbstractSnifferReceiverTest
{
    private final static SubnetAddress SUBNET = InetUtil.getDefaultSubnet();

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(1024);
    private final UnsafeBufferSerializer sendBufferSerializer = new UnsafeBufferSerializer();

    private static final int STREAM_ID = 10;
    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static Publication PUBLICATION;

    private static AutodiscManagerSniffer SNIFFER;
    private static SnifferListener SNIFFER_LISTENER = new SnifferListener();
    private static final String[] VALID_CONFIG = new String[]{"-p", "35001", "-sn", SUBNET.toString(), "-t", "10000", "-ip", "225.0.0.1"};


    @BeforeClass
    public static void beforeClass() throws Exception
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx = new Aeron.Context();
        ctx.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON = Aeron.connect(ctx);

        // Create the publication
        final int multicastIp = InetUtil.convertIpAddressToInt("225.0.0.1");
        final String multicastChannelString = AeronChannelHelper.createMulticastChannelString(multicastIp, 35001, SUBNET);
        PUBLICATION = AERON.addPublication(multicastChannelString, STREAM_ID);

        final SnifferCommandLineParser parser = new SnifferCommandLineParser();
        final SnifferParameters parameters = parser.parseCommandLine(VALID_CONFIG);

        SNIFFER = new AutodiscManagerSniffer(AERON, parameters, SNIFFER_LISTENER);
        SNIFFER.start();

        // Wait and let the connections to be created
        Thread.sleep(2000);
    }

    @AfterClass
    public static void afterClass()
    {
        PUBLICATION.close();
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Before
    public void setUp() throws Exception
    {

    }


    @Test
    public void testInstance() throws InterruptedException
    {
        final AutoDiscInstanceInfo instanceInfo = new AutoDiscInstanceInfo("instance1", UUID.randomUUID(), 12, 23, 55, 12, 23, 56);

        this.sendMessage(MsgType.AUTO_DISC_INSTANCE, instanceInfo);

        //Test new instance info received
        Thread.sleep(1000);
        Assert.assertEquals(instanceInfo, SNIFFER_LISTENER.receivedInstanceInfo);
        SNIFFER_LISTENER.reset();

        //Test new instance info time out received
        Thread.sleep(10000);
        Assert.assertNull(SNIFFER_LISTENER.receivedInstanceInfo);
    }

    @Test
    public void testTopics() throws InterruptedException
    {
        final AutoDiscTopicInfo topicInfo = new AutoDiscTopicInfo(UUID.randomUUID(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic");
        this.sendMessage(MsgType.AUTO_DISC_TOPIC, topicInfo);

        //Test new instance info received
        Thread.sleep(1000);
        Assert.assertEquals(topicInfo, SNIFFER_LISTENER.receivedTopicInfo);
        SNIFFER_LISTENER.reset();

        //Test new instance info time out received
        Thread.sleep(10000);
        Assert.assertNull(SNIFFER_LISTENER.receivedTopicInfo);
    }

    @Test
    public void testTopicSockets() throws InterruptedException
    {
        final AutoDiscTopicSocketInfo topicSocketInfo = new AutoDiscTopicSocketInfo(UUID.randomUUID(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic", UUID.randomUUID(), 1, 2, 4);
        this.sendMessage(MsgType.AUTO_DISC_TOPIC_SOCKET, topicSocketInfo);

        //Test new instance info received
        Thread.sleep(1000);
        Assert.assertEquals(topicSocketInfo, SNIFFER_LISTENER.receivedTopicSocketInfo);
        SNIFFER_LISTENER.reset();

        //Test new instance info time out received
        Thread.sleep(10000);
        Assert.assertNull(SNIFFER_LISTENER.receivedTopicSocketInfo);
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
            baseHeader = new BaseHeader(msgType, Version.toIntegerRepresentation((byte) 55, (byte) 3, (byte) 1));
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

    static class SnifferListener implements ISnifferListener
    {
        AutoDiscInstanceInfo receivedInstanceInfo;
        AutoDiscTopicInfo receivedTopicInfo;
        AutoDiscTopicSocketInfo receivedTopicSocketInfo;
        AutoDiscTopicInfo timedTopicMsg;
        AutoDiscTopicSocketInfo timedTopicSocketMsg;
        AutoDiscInstanceInfo timedOutInstanceMsg;

        public void reset()
        {
            receivedInstanceInfo = null;
            receivedTopicInfo = null;
            receivedTopicSocketInfo = null;
            timedTopicMsg = null;
            timedTopicSocketMsg = null;
            timedOutInstanceMsg = null;
        }

        public void onNewAutoDiscTopicInfo(AutoDiscTopicInfo info)
        {
            this.receivedTopicInfo = info;
        }

        @Override
        public void onTimedOutAutoDiscTopicInfo(AutoDiscTopicInfo info)
        {
            this.timedTopicMsg = info;
        }

        @Override
        public void onNewAutoDiscTopicSocketInfo(AutoDiscTopicSocketInfo info)
        {
            this.receivedTopicSocketInfo = info;
        }

        @Override
        public void onTimedOutAutoDiscTopicSocketInfo(AutoDiscTopicSocketInfo info)
        {
            this.timedTopicSocketMsg = info;
        }

        @Override
        public void onNewAutoDiscInstanceInfo(AutoDiscInstanceInfo info)
        {
            this.receivedInstanceInfo = info;
        }

        @Override
        public void onTimedOutAutoDiscInstanceInfo(AutoDiscInstanceInfo info)
        {
            this.timedOutInstanceMsg = info;
        }


    }
}