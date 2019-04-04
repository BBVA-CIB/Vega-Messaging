package com.bbva.kyof.vega.autodiscovery.advert;

import com.bbva.kyof.vega.autodiscovery.advert.ActiveAdvertsQueue;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by cnebrera on 04/08/16.
 */
public class ActiveAdvertsQueueTest
{
    @Test
    public void testSingleElement() throws Exception
    {
        final ActiveAdvertsQueue<AutoDiscTopicInfo> queue = new ActiveAdvertsQueue<>(200);
        final UUID instanceId = UUID.randomUUID();
        final UUID topicInfoUniqueId = UUID.randomUUID();

        // Get next timed out element, should be null since it is empty
        Assert.assertNull(queue.returnNextTimedOutElement());

        // Add an element, since it is empty the result should be true
        Assert.assertTrue(queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, topicInfoUniqueId, "topic")));
        // Add again, the result should be false, they share the unique id
        Assert.assertFalse(queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.SUB_UNI, topicInfoUniqueId, "topicTAC")));

        // Wait a bit to force the time out
        Thread.sleep(300);

        // Get the timed out element, it should be the first added and the second add should not be there
        AutoDiscTopicInfo nextTimedOutElement = queue.returnNextTimedOutElement();
        Assert.assertNotNull(nextTimedOutElement);
        Assert.assertEquals(nextTimedOutElement.getTopicName(), "topic");
        Assert.assertNull(queue.returnNextTimedOutElement());

        // Clear
        queue.clear();
        Thread.sleep(300);

        Assert.assertNull(queue.returnNextTimedOutElement());
    }

    @Test
    public void testMultipleElement() throws Exception
    {
        final UUID instanceId = UUID.randomUUID();

        final UUID uniqueId1 = UUID.randomUUID();
        final UUID uniqueId2 = UUID.randomUUID();
        final UUID uniqueId3 = UUID.randomUUID();

        // Create the queue
        final ActiveAdvertsQueue<AutoDiscTopicInfo> queue = new ActiveAdvertsQueue<>(1000);

        // Add some elements separated by some time
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId1, "topic"));
        Thread.sleep(250);
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId2, "topic"));
        Thread.sleep(250);
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId3, "topic"));

        // The next element is not yet ready
        Assert.assertNull(queue.returnNextTimedOutElement());

        // Wait a bit more
        Thread.sleep(501);
        Assert.assertEquals(queue.returnNextTimedOutElement().getUniqueId(), uniqueId1);
        Assert.assertNull(queue.returnNextTimedOutElement());

        // Wait a bit more
        Thread.sleep(251);
        Assert.assertEquals(queue.returnNextTimedOutElement().getUniqueId(), uniqueId2);
        Assert.assertNull(queue.returnNextTimedOutElement());

        // Wait a bit more, the next 2 element should have timed out
        Thread.sleep(251);
        Assert.assertEquals(queue.returnNextTimedOutElement().getUniqueId(), uniqueId3);
        Assert.assertNull(queue.returnNextTimedOutElement());

        // Wait a bit more, the next element should have timed out
        Thread.sleep(501);
        Assert.assertNull(queue.returnNextTimedOutElement());

        // The queue should be now empty
        final AtomicLong sum = new AtomicLong(0);
        queue.runForEachElement((element) -> sum.getAndIncrement());

        Assert.assertTrue(sum.get() == 0);
    }

    @Test
    public void testMultipleElementWithUpdates() throws Exception
    {
        final UUID instanceId = UUID.randomUUID();

        final UUID uniqueId1 = UUID.randomUUID();
        final UUID uniqueId2 = UUID.randomUUID();
        final UUID uniqueId3 = UUID.randomUUID();

        // Create the queue
        final ActiveAdvertsQueue<AutoDiscTopicInfo> queue = new ActiveAdvertsQueue<>(1000);

        // Add some elements separated by some time
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId1, "topic"));
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId2, "topic"));
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId3, "topic"));

        // The queue should have 3 elements
        final AtomicLong sum = new AtomicLong(0);
        queue.runForEachElement((element) -> sum.getAndIncrement());
        Assert.assertTrue(sum.get() == 3);

        // Wait a bit and update a single element
        Thread.sleep(500);
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId1, "topic"));
        Thread.sleep(600);

        Assert.assertEquals(queue.returnNextTimedOutElement().getUniqueId(), uniqueId2);
        Assert.assertEquals(queue.returnNextTimedOutElement().getUniqueId(), uniqueId3);
        Assert.assertNull(queue.returnNextTimedOutElement());

        Thread.sleep(600);
        Assert.assertEquals(queue.returnNextTimedOutElement().getUniqueId(), uniqueId1);
    }
}