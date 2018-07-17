package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.config.general.ConfigReader;
import com.bbva.kyof.vega.config.general.ConfigReaderTest;
import com.bbva.kyof.vega.config.general.GlobalConfiguration;
import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.protocol.AutoDiscManagerMock;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.control.IOwnSecPubTopicsChangesListener;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.easymock.EasyMock;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Created by cnebrera on 11/08/16.
 */
public class SendManagerTest
{
    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static SubnetAddress SUBNET_ADDRESS;
    private static VegaContext VEGA_CONTEXT;
    private static AutoDiscManagerMock AUTO_DISC_MANAGER_MOCK;

    PublishersManagerUnicast publisherManager;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx1 = new Aeron.Context();
        ctx1.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON = Aeron.connect(ctx1);

        SUBNET_ADDRESS = InetUtil.getDefaultSubnet();

        // Create the context
        VEGA_CONTEXT = new VegaContext(AERON, loadConfiguration());

        // Mock auto-discovery manager calls
        AUTO_DISC_MANAGER_MOCK = new AutoDiscManagerMock();
        VEGA_CONTEXT.setAutodiscoveryManager(AUTO_DISC_MANAGER_MOCK.getMock());

        // Give it time to start
        Thread.sleep(1000);
    }

    private static GlobalConfiguration loadConfiguration() throws VegaException
    {
        /** File containing the configuration */
        String validConfigFile = ConfigReaderTest.class.getClassLoader().getResource("config/sendManagerTestConfig.xml").getPath();
        return ConfigReader.readConfiguration(validConfigFile);
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Test(expected = VegaException.class)
    public void testCreateTopicPublisherWithNoConfig() throws Exception
    {
        final IOwnSecPubTopicsChangesListener listener = EasyMock.createNiceMock(IOwnSecPubTopicsChangesListener.class);
        EasyMock.replay(listener);

        try(final SendManager sendManager = new SendManager(VEGA_CONTEXT, listener))
        {
            sendManager.createTopicPublisher("dTopic");
        }
    }

    @Test(expected = VegaException.class)
    public void testDestroyTopicPublisherWithNoConfig() throws Exception
    {
        final IOwnSecPubTopicsChangesListener listener = EasyMock.createNiceMock(IOwnSecPubTopicsChangesListener.class);
        EasyMock.replay(listener);

        try(final SendManager sendManager = new SendManager(VEGA_CONTEXT, listener))
        {
            sendManager.destroyTopicPublisher("dTopic");
        }
    }

    @Test
    public void testCreateTopicPublishers() throws Exception
    {
        final IOwnSecPubTopicsChangesListener listener = EasyMock.createNiceMock(IOwnSecPubTopicsChangesListener.class);
        EasyMock.replay(listener);

        final SendManager sendManager = new SendManager(VEGA_CONTEXT, listener);

        final ITopicPublisher mcastPublisher = sendManager.createTopicPublisher("aTopic");
        final ITopicPublisher ucastPublisher = sendManager.createTopicPublisher("bTopic");
        final ITopicPublisher ipcPublisher = sendManager.createTopicPublisher("cTopic");

        // Create several receivers, one per topic transport
        final String ip = SUBNET_ADDRESS.getIpAddres().getHostAddress();
        SimpleReceiver simpleReceiverMcast = new SimpleReceiver(AERON, TransportMediaType.MULTICAST, "224.0.0.9", 23335, 2, SUBNET_ADDRESS);
        SimpleReceiver simpleReceiverIpc = new SimpleReceiver(AERON, TransportMediaType.IPC, null, 0, 2, null);

        // Wait for connections to discover each other
        Thread.sleep(1000);

        // Now test send
        this.sendMessageAndCheckArrival(mcastPublisher, simpleReceiverMcast, true);
        this.sendMessageAndCheckArrival(ipcPublisher, simpleReceiverIpc, true);

        sendManager.close();
    }

    @Test
    public void testDestroyTopicPublishers() throws Exception
    {
        final IOwnSecPubTopicsChangesListener listener = EasyMock.createNiceMock(IOwnSecPubTopicsChangesListener.class);
        EasyMock.replay(listener);

        final SendManager sendManager = new SendManager(VEGA_CONTEXT, listener);

        final ITopicPublisher mcastPublisher = sendManager.createTopicPublisher("aTopic");
        final ITopicPublisher ucastPublisher = sendManager.createTopicPublisher("bTopic");
        final ITopicPublisher ipcPublisher = sendManager.createTopicPublisher("cTopic");

        // Create several receivers, one per topic transport
        final String ip = SUBNET_ADDRESS.getIpAddres().getHostAddress();
        SimpleReceiver simpleReceiverMcast = new SimpleReceiver(AERON, TransportMediaType.MULTICAST, "224.0.0.9", 23335, 2, SUBNET_ADDRESS);
        SimpleReceiver simpleReceiverIpc = new SimpleReceiver(AERON, TransportMediaType.IPC, null, 0, 2, null);

        // Wait for connections to discover each other
        Thread.sleep(1000);

        // Destroy the publishers
        sendManager.destroyTopicPublisher("aTopic");
        sendManager.destroyTopicPublisher("bTopic");
        sendManager.destroyTopicPublisher("cTopic");

        // Now test send
        this.sendMessageAndCheckArrival(mcastPublisher, simpleReceiverMcast, false);
        this.sendMessageAndCheckArrival(ipcPublisher, simpleReceiverIpc, false);

        sendManager.close();
    }

    private void sendMessageAndCheckArrival(ITopicPublisher topicPublisher, SimpleReceiver simpleReceiver, boolean shouldArrive) throws InterruptedException
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(128));
        buffer.putInt(0, 128);

        topicPublisher.sendMsg(buffer, 0, 4);
        Thread.sleep(100);
        simpleReceiver.pollReceivedMessage();

        if (shouldArrive)
        {
            Assert.assertTrue((simpleReceiver.getReusableReceivedMsg().getTopicPublisherId() != null));
        }
        else
        {
            Assert.assertTrue((simpleReceiver.getReusableReceivedMsg().getTopicPublisherId() == null));
        }

        simpleReceiver.reset();
    }
}