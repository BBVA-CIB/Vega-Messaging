package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.Version;
import com.bbva.kyof.vega.config.general.GlobalConfiguration;
import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.msg.*;
import com.bbva.kyof.vega.protocol.common.VegaContext;
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
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

/**
 * Created by cnebrera on 11/08/16.
 */
public class AeronPublisherTest
{
    private static final Random RND = new Random();
    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static SubnetAddress SUBNET_ADDRESS;
    private static VegaContext VEGA_CONTEXT;

    @BeforeClass
    public static void beforeClass()
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx1 = new Aeron.Context();
        ctx1.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON = Aeron.connect(ctx1);

        SUBNET_ADDRESS = InetUtil.getDefaultSubnet();
        VEGA_CONTEXT = new VegaContext(AERON, new GlobalConfiguration());
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Test
    public void testClaimOfferResultToString()
    {
        Assert.assertEquals(AeronPublisher.claimOfferResultToString(1234), "1234");
        Assert.assertEquals(AeronPublisher.claimOfferResultToString(Publication.NOT_CONNECTED), "NOT_CONNECTED");
        Assert.assertEquals(AeronPublisher.claimOfferResultToString(Publication.BACK_PRESSURED), "BACK_PRESSURED");
        Assert.assertEquals(AeronPublisher.claimOfferResultToString(Publication.ADMIN_ACTION), "ADMIN_ACTION");
        Assert.assertEquals(AeronPublisher.claimOfferResultToString(Publication.CLOSED), "CLOSED");
        Assert.assertEquals(AeronPublisher.claimOfferResultToString(-1234), "UNKNOWN");
    }

    @Test
    public void testBackPressure() throws Exception
    {
        // Create the publisher
        final AeronPublisherParams params = new AeronPublisherParams(
                TransportMediaType.UNICAST,
                InetUtil.convertIpAddressToInt(SUBNET_ADDRESS.getIpAddres().getHostAddress()),
                28005,
                10,
                SUBNET_ADDRESS);

        final AeronPublisher publisher = new AeronPublisher(VEGA_CONTEXT, params);

        Assert.assertEquals(params, publisher.getParams());

        final SimpleReceiver simpleReceiver = new SimpleReceiver(AERON, TransportMediaType.MULTICAST, SUBNET_ADDRESS.getIpAddres().getHostAddress(), 28005, 10, SUBNET_ADDRESS);

        // Give it time to initialize
        Thread.sleep(1000);

        // Create random contents
        final UnsafeBuffer sendBuffer = new UnsafeBuffer(ByteBuffer.allocate(1024));

        // Create a topic ID
        final UUID topicId = UUID.randomUUID();

        // Send the message
        while(publisher.sendMessage(MsgType.DATA, topicId, sendBuffer, 0, 1024) != PublishResult.BACK_PRESSURED);

        Assert.assertTrue(true);

        publisher.close();
        simpleReceiver.close();
    }

    @Test
    public void testMcastPublish() throws Exception
    {
        // Create the publisher
        final AeronPublisherParams params = new AeronPublisherParams(
                TransportMediaType.MULTICAST,
                InetUtil.convertIpAddressToInt("224.1.1.1"),
                28001,
                4,
                SUBNET_ADDRESS);

        final AeronPublisher publisher = new AeronPublisher(VEGA_CONTEXT, params);

        // Create a subscriber for it as well
        final SimpleReceiver simpleReceiver = new SimpleReceiver(AERON, TransportMediaType.MULTICAST, "224.1.1.1", 28001, 4, SUBNET_ADDRESS);

        // Give it time to initialize
        Thread.sleep(1000);

        this.testSendMessages(publisher, simpleReceiver);
        this.testSendRequests(publisher, simpleReceiver);
        this.testSendResponses(publisher, simpleReceiver);

        publisher.close();
        simpleReceiver.close();
    }

    @Test
    public void testUcastPublish() throws Exception
    {
        // Create the publisher
        final AeronPublisherParams params = new AeronPublisherParams(
                TransportMediaType.UNICAST,
                InetUtil.convertIpAddressToInt(SUBNET_ADDRESS.getIpAddres().getHostAddress()),
                28002,
                5,
                SUBNET_ADDRESS);

        final AeronPublisher publisher = new AeronPublisher(VEGA_CONTEXT, params);

        // Create a subscriber for it as well
        final SimpleReceiver simpleReceiver = new SimpleReceiver(AERON, TransportMediaType.MULTICAST, SUBNET_ADDRESS.getIpAddres().getHostAddress(), 28002, 5, SUBNET_ADDRESS);

        // Give it time to initialize
        Thread.sleep(1000);

        this.testSendMessages(publisher, simpleReceiver);
        this.testSendRequests(publisher, simpleReceiver);
        this.testSendResponses(publisher, simpleReceiver);

        publisher.close();
        simpleReceiver.close();
    }

    @Test
    public void testIpcPublish() throws Exception
    {
        // Create the publisher
        final AeronPublisherParams params = new AeronPublisherParams(
                TransportMediaType.IPC,
                0,
                0,
                5,
                null);

        final AeronPublisher publisher = new AeronPublisher(VEGA_CONTEXT, params);

        // Create a subscriber for it as well
        final SimpleReceiver simpleReceiver = new SimpleReceiver(AERON, TransportMediaType.IPC, null, 0, 5, null);

        // Give it time to initialize
        Thread.sleep(1000);

        this.testSendMessages(publisher, simpleReceiver);
        this.testSendRequests(publisher, simpleReceiver);
        this.testSendResponses(publisher, simpleReceiver);

        publisher.close();
        publisher.close();
        simpleReceiver.close();

        // Send again, should do nothing
        Assert.assertTrue(publisher.sendMessage(MsgType.DATA, null, null, 0, 0) == PublishResult.OK);
        Assert.assertTrue(publisher.sendRequest(MsgType.DATA_REQ, null, null, null, 0, 0) == PublishResult.OK);
        Assert.assertTrue(publisher.sendResponse(null, null, 0, 0) == PublishResult.OK);
    }

    private void testSendMessages(AeronPublisher publisher, SimpleReceiver subscription) throws Exception
    {
        // Try different message sizes
        int msgSize = 128;

        while (msgSize < 128000)
        {
            this.testSendMessage(publisher, subscription, msgSize);
            msgSize = msgSize * 2;
        }
    }

    private void testSendRequests(AeronPublisher publisher, SimpleReceiver subscription) throws Exception
    {
        // Try different message sizes
        int msgSize = 128;

        while (msgSize < 128000)
        {
            this.testSendRequest(publisher, subscription, msgSize);
            msgSize = msgSize * 2;
        }
    }

    private void testSendResponses(AeronPublisher publisher, SimpleReceiver subscription) throws Exception
    {
        // Try different message sizes
        int msgSize = 128;

        while (msgSize < 128000)
        {
            this.testSendResponse(publisher, subscription, msgSize);
            msgSize = msgSize * 2;
        }
    }

    private void testSendMessage(final AeronPublisher publisher, final SimpleReceiver subscription, final int msgSize) throws Exception
    {
        // Create random contents
        final byte[] array = new byte[msgSize];
        RND.nextBytes(array);
        final UnsafeBuffer sendBuffer = new UnsafeBuffer(array);

        // Create a topic ID
        final UUID topicId = UUID.randomUUID();

        // Send the message
        Assert.assertTrue(publisher.sendMessage(MsgType.DATA, topicId, sendBuffer, 0, msgSize) == PublishResult.OK);

        // Give it time to arrive
        Thread.sleep(10);

        // Get the message
        subscription.pollReceivedMessage();

        // Check values
        Assert.assertTrue(subscription.getReusableDataMsgHeader().getTopicPublisherId().equals(topicId));
        Assert.assertTrue(subscription.getReusableDataMsgHeader().getInstanceId().equals(VEGA_CONTEXT.getInstanceUniqueId()));
        Assert.assertTrue(subscription.getReusableBaseHeader().getVersion() == Version.LOCAL_VERSION);

        // Check the received message contents
        final IRcvMessage receivedMsg = subscription.getReusableReceivedMsg();
        this.checkResult(array, receivedMsg.getContents(), receivedMsg.getContentOffset(), receivedMsg.getContentLength());
    }

    private void checkResult(final byte[] sendMsg, final UnsafeBuffer receivedContent, final int receivedContentOffset, final int receivedContentLength)
    {
        final byte[] resultArray = new byte[receivedContentLength];
        receivedContent.getBytes(receivedContentOffset, resultArray);
        Assert.assertTrue(Arrays.equals(resultArray, sendMsg));
    }

    private void testSendRequest(final AeronPublisher publisher, final SimpleReceiver subscription, final int msgSize) throws Exception
    {
        // Create random contents
        final byte[] array = new byte[msgSize];
        RND.nextBytes(array);
        final UnsafeBuffer sendBuffer = new UnsafeBuffer(array);

        // Create a topic ID
        final UUID topicId = UUID.randomUUID();
        final UUID requestId = UUID.randomUUID();

        // Send the message
        Assert.assertTrue(publisher.sendRequest(MsgType.DATA_REQ, topicId, requestId, sendBuffer, 0, msgSize) == PublishResult.OK);

        // Give it time to arrive
        Thread.sleep(10);

        // Get the message
        subscription.pollReceivedMessage();

        // Check values
        Assert.assertTrue(subscription.getReusableReqMsgHeader().getTopicPublisherId().equals(topicId));
        Assert.assertTrue(subscription.getReusableReqMsgHeader().getRequestId().equals(requestId));
        Assert.assertTrue(subscription.getReusableReqMsgHeader().getInstanceId().equals(VEGA_CONTEXT.getInstanceUniqueId()));
        Assert.assertTrue(subscription.getReusableBaseHeader().getVersion() == Version.LOCAL_VERSION);

        // Check the received request contents
        final IRcvRequest receivedMsg = subscription.getReusableReceivedRequest();
        this.checkResult(array, receivedMsg.getContents(), receivedMsg.getContentOffset(), receivedMsg.getContentLength());
    }

    private void testSendResponse(final AeronPublisher publisher, final SimpleReceiver subscription, final int msgSize) throws Exception
    {
        // Create random contents
        final byte[] array = new byte[msgSize];
        RND.nextBytes(array);
        final UnsafeBuffer sendBuffer = new UnsafeBuffer(array);

        // Create a topic ID
        final UUID requestId = UUID.randomUUID();

        // Send the message
        Assert.assertTrue(publisher.sendResponse(requestId, sendBuffer, 0, msgSize) == PublishResult.OK);

        // Give it time to arrive
        Thread.sleep(10);

        // Get the message
        subscription.pollReceivedMessage();

        // Check values
        Assert.assertTrue(subscription.getReusableRespMsgHeader().getRequestId().equals(requestId));
        Assert.assertTrue(subscription.getReusableRespMsgHeader().getInstanceId().equals(VEGA_CONTEXT.getInstanceUniqueId()));
        Assert.assertTrue(subscription.getReusableBaseHeader().getVersion() == Version.LOCAL_VERSION);

        // Check the received request contents
        final IRcvResponse receivedMsg = subscription.getReusableReceivedResponse();
        this.checkResult(array, receivedMsg.getContents(), receivedMsg.getContentOffset(), receivedMsg.getContentLength());
    }


}