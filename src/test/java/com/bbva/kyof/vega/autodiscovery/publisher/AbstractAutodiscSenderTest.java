package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.config.general.AutoDiscoType;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.serialization.IUnsafeSerializable;
import io.aeron.Aeron;
import io.aeron.Publication;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

/**
 * Created by cnebrera on 02/08/16.
 */
public class AbstractAutodiscSenderTest
{
    private UUID sender1Id = UUID.randomUUID();

    private TestAutoDiscSender sender;
    private TestAutoDiscSender2 sender2;

    private final AutoDiscInstanceInfo instanceInfo = new AutoDiscInstanceInfo("name", UUID.randomUUID(), 1, 2, 3, "respHost", 4, 5, 6, "ctrlHost");


    private final AutoDiscTopicInfo topicInfo1 = new AutoDiscTopicInfo(sender1Id, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1");
    private final AutoDiscTopicInfo topicInfo2 = new AutoDiscTopicInfo(sender1Id, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1");
    private final AutoDiscTopicInfo topicInfo3 = new AutoDiscTopicInfo(sender1Id, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic2");

    private final AutoDiscTopicSocketInfo topicSocketInfo1 = new AutoDiscTopicSocketInfo(sender1Id, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1", UUID.randomUUID(), 1, 2, 3,
            "host_topic_1");
    private final AutoDiscTopicSocketInfo topicSocketInfo2 = new AutoDiscTopicSocketInfo(sender1Id, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1", UUID.randomUUID(), 1, 2, 3,
            "host_topic_2");
    private final AutoDiscTopicSocketInfo topicSocketInfo3 = new AutoDiscTopicSocketInfo(sender1Id, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic2", UUID.randomUUID(), 1, 2, 3,
            "host_topic_3");

    @Before
    public void setUp() throws Exception
    {
        // Create the configuration, with 300 millis refresh interval
        final AutoDiscoveryConfig config = AutoDiscoveryConfig.builder().autoDiscoType(AutoDiscoType.MULTICAST).refreshInterval(100L).build();
        config.completeAndValidateConfig();

        // Create the test senders
        this.sender = new TestAutoDiscSender(null, config);
        this.sender2 = new TestAutoDiscSender2(null, config);
    }

    @Test
    public void testClose()
    {
        //Now, the publication is in the implementations of this abstract class. It is externally closed
        Publication publication = sender.getPublication();
        publication.close();
        this.sender.close();
        Assert.assertTrue(this.sender.testIsClosed);
    }

    @Test
    public void testRegisterInstance() throws Exception
    {
        // There should be no adverts if we try to send
        Assert.assertEquals(0, this.sender.sendNextTopicAdverts());

        // Register an instance
        this.sender.registerInstance(instanceInfo);

        // Wait a bit
        Thread.sleep(400);

        // Send next topic adverts again, now it should be one
        Assert.assertEquals(1, this.sender.sendNextTopicAdverts());

        // There should be no adverts if we try to send again
        Assert.assertEquals(0, this.sender.sendNextTopicAdverts());

        // Wait a bit
        Thread.sleep(400);

        // Send next topic adverts again, now it should be one again
        Assert.assertEquals(1, this.sender.sendNextTopicAdverts());

        // Now remove the instance
        this.sender.unRegisterInstance();

        // Wait a bit
        Thread.sleep(400);

        // There should be no adverts if we try to send again
        Assert.assertEquals(0, this.sender.sendNextTopicAdverts());
    }

    @Test
    public void testRegisterTopicInfo() throws Exception
    {
        // Test immediate send upon register
        this.sender2.registerTopic(this.topicInfo1);
        Assert.assertEquals(1, this.sender2.numTopicMsgsSent);

        // Register an instance
        this.sender.registerTopic(this.topicInfo1);

        // Wait a bit
        Thread.sleep(400);

        // Send next topic adverts again, now it should be one
        Assert.assertEquals(1, this.sender.sendNextTopicAdverts());
        // There should be no adverts if we try to send again
        Assert.assertEquals(0, this.sender.sendNextTopicAdverts());

        // Insert 2 more
        this.sender.registerTopic(this.topicInfo2);
        this.sender.registerTopic(this.topicInfo3);

        // Wait a bit
        Thread.sleep(400);

        // Send next topic adverts again, now it should be one again but 3 times
        // The first burst interval sends 1 topic advert + 1 topic by timeout
        Assert.assertEquals(2, this.sender.sendNextTopicAdverts());

        //One old remains (timeout 100 ms, 3 topics => burstInterval = 33)
        // Wait a burstInterval
        Thread.sleep(33);
        Assert.assertEquals(1, this.sender.sendNextTopicAdverts());
        Assert.assertEquals(0, this.sender.sendNextTopicAdverts());
        Assert.assertEquals(0, this.sender.sendNextTopicAdverts());

        // Now remove 2 TOPICS info
        this.sender.unregisterTopic(topicInfo1);
        this.sender.unregisterTopic(topicInfo2);

        // Wait a bit
        Thread.sleep(400);

        // There should be only one now
        Assert.assertEquals(1, this.sender.sendNextTopicAdverts());
        Assert.assertEquals(0, this.sender.sendNextTopicAdverts());

        // If we add the same one again, it should not be added
        this.sender.registerTopic(this.topicInfo3);

        Thread.sleep(400);

        // There should be still only one now
        Assert.assertEquals(1, this.sender.sendNextTopicAdverts());
        Assert.assertEquals(0, this.sender.sendNextTopicAdverts());
    }

    @Test
    public void testRegisterTopicSocketInfo() throws Exception
    {
        // Test immediate send upon register
        this.sender2.registerTopicSocket(this.topicSocketInfo1);
        Assert.assertEquals(1, this.sender2.numTopicSocketMsgSent);

        // Register an instance
        this.sender.registerTopicSocket(this.topicSocketInfo1);

        // Wait a bit
        Thread.sleep(400);

        // Send next topic adverts again, now it should be one
        Assert.assertEquals(1, this.sender.sendNextTopicAdverts());
        // There should be no adverts if we try to send again
        Assert.assertEquals(0, this.sender.sendNextTopicAdverts());

        // Insert 2 more
        this.sender.registerTopicSocket(this.topicSocketInfo2);
        this.sender.registerTopicSocket(this.topicSocketInfo3);

        // Wait a bit
        Thread.sleep(400);

        // Send next topic adverts again, now it should be one again but 3 times
        //The first burst send one topicSocket + one old
        Assert.assertEquals(2, this.sender.sendNextTopicAdverts());

        //One old remains (timeout 100 ms, 3 topics => burstInterval = 33)
        // Wait a burstInterval
        Thread.sleep(33);
        Assert.assertEquals(1, this.sender.sendNextTopicAdverts());
        Assert.assertEquals(0, this.sender.sendNextTopicAdverts());
        Assert.assertEquals(0, this.sender.sendNextTopicAdverts());

        // Now remove 2 TOPICS info
        this.sender.unregisterTopicSocket(topicSocketInfo1);
        this.sender.unregisterTopicSocket(topicSocketInfo2);

        // Wait a bit
        Thread.sleep(400);

        // There should be only one now
        Assert.assertEquals(1, this.sender.sendNextTopicAdverts());
        Assert.assertEquals(0, this.sender.sendNextTopicAdverts());

        // If we add the same one again, it should not be added
        this.sender.registerTopicSocket(this.topicSocketInfo3);

        Thread.sleep(400);

        // There should be still only one now
        Assert.assertEquals(1, this.sender.sendNextTopicAdverts());
        Assert.assertEquals(0, this.sender.sendNextTopicAdverts());
    }

    @Test
    public void testRepublishInstanceInfo() throws Exception
    {
        Assert.assertEquals(0, this.sender2.numInstanceMsgSent);

        // Register an instance
        this.sender2.registerInstance(instanceInfo);

        // It should have sent the message immediately
        Assert.assertEquals(1, this.sender2.numInstanceMsgSent);

        // Republish
        this.sender2.republishInstanceInfo();

        Assert.assertEquals(2, this.sender2.numInstanceMsgSent);

        // Just send, the next one should not be sent yet
        this.sender2.sendNextTopicAdverts();
        Assert.assertEquals(2, this.sender2.numInstanceMsgSent);

        // Wait a bit
        Thread.sleep(400);

        // Send next topic adverts again, now it should be one
        this.sender2.sendNextTopicAdverts();
        Assert.assertEquals(3, this.sender2.numInstanceMsgSent);
    }

    @Test
    public void testRepublishTopicInfo() throws Exception
    {
        Assert.assertEquals(0, this.sender2.numInstanceMsgSent);

        // Register topic and topic sockets
        this.sender2.registerInstance(this.instanceInfo);
        this.sender2.registerTopic(this.topicInfo1);
        this.sender2.registerTopic(this.topicInfo2);
        this.sender2.registerTopic(this.topicInfo3);
        this.sender2.registerTopicSocket(this.topicSocketInfo1);
        this.sender2.registerTopicSocket(this.topicSocketInfo2);
        this.sender2.registerTopicSocket(this.topicSocketInfo3);

        // Check immediate send
        Assert.assertEquals(1, this.sender2.numInstanceMsgSent);
        Assert.assertEquals(3, this.sender2.numTopicMsgsSent);
        Assert.assertEquals(3, this.sender2.numTopicSocketMsgSent);

        // Resend, it should not increase anything because there is no "topic2" on that transport
        this.sender2.republishAllInfoAboutTopic("topic2", AutoDiscTransportType.SUB_IPC);
        Assert.assertEquals(3, this.sender2.numTopicMsgsSent);
        Assert.assertEquals(3, this.sender2.numTopicSocketMsgSent);

        // Resend, now using the right transport
        this.sender2.republishAllInfoAboutTopic("topic2", AutoDiscTransportType.PUB_IPC);
        Assert.assertEquals(4, this.sender2.numTopicMsgsSent);
        Assert.assertEquals(4, this.sender2.numTopicSocketMsgSent);

        // Resend, now using for topic 1
        this.sender2.republishAllInfoAboutTopic("topic1", AutoDiscTransportType.PUB_IPC);
        Assert.assertEquals(6, this.sender2.numTopicMsgsSent);
        Assert.assertEquals(6, this.sender2.numTopicSocketMsgSent);

        Thread.sleep(400);
        //The first burst interval send 1 topic advert and 1 topicSocket advert
        // + 1 topic timeout + 1 topicSocket timeout + 1 instance
        Assert.assertEquals(5, this.sender2.sendNextTopicAdverts());
        Assert.assertEquals(2, this.sender2.numInstanceMsgSent);
        Assert.assertEquals(8, this.sender2.numTopicMsgsSent);
        Assert.assertEquals(8, this.sender2.numTopicSocketMsgSent);

    }

    // Implementation for testing
    private class TestAutoDiscSender extends AbstractAutodiscSender
    {
        boolean testIsClosed = false;

        TestAutoDiscSender(Aeron aeron, AutoDiscoveryConfig config)
        {
            super(aeron, config);
        }

        @Override
        public Publication getPublication()
        {
            final Publication publication = EasyMock.createNiceMock(Publication.class);
            publication.close();
            EasyMock.expectLastCall().andAnswer(this::closedCalled).anyTimes();

            EasyMock.replay(publication);

            return publication;
        }

        private Object closedCalled()
        {
            this.testIsClosed = true;
            return null;
        }
    }

    // Implementation for testing
    private class TestAutoDiscSender2 extends AbstractAutodiscSender
    {
        int numInstanceMsgSent = 0;
        int numTopicMsgsSent = 0;
        int numTopicSocketMsgSent = 0;

        TestAutoDiscSender2(Aeron aeron, AutoDiscoveryConfig config)
        {
            super(aeron, config);
        }

        @Override
        public Publication getPublication()
        {
            return null;
        }

        @Override
        int sendMessageIfNotNull(final byte msgType, final IUnsafeSerializable serializable)
        {
            if (serializable == null)
            {
                return 0;
            }

            switch (msgType)
            {
                case MsgType.AUTO_DISC_INSTANCE:
                    numInstanceMsgSent++;
                    return 1;
                case MsgType.AUTO_DISC_TOPIC:
                    numTopicMsgsSent++;
                    return 1;
                case MsgType.AUTO_DISC_TOPIC_SOCKET:
                    numTopicSocketMsgSent++;
                    return 1;
                default:
                    return 0;
            }
        }
    }
}