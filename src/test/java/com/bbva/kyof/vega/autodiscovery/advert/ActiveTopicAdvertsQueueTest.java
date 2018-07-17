package com.bbva.kyof.vega.autodiscovery.advert;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by cnebrera on 04/08/16.
 */
public class ActiveTopicAdvertsQueueTest extends ActiveAdvertsQueueTest
{
    @Test
    public void testSingleElement() throws Exception
    {
        final ActiveTopicAdvertsQueue<AutoDiscTopicInfo> queue = new ActiveTopicAdvertsQueue<>(200);
        final UUID instanceId = UUID.randomUUID();
        final UUID uniqueId = UUID.randomUUID();

        // Get next timed out element, should be null since it is empty
        Assert.assertNull(queue.returnNextTimedOutElement());

        // Add an element, since it is empty the result should be true
        Assert.assertTrue(queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId, "topic")));
        // Add again, the result should be false, they share the unique id
        Assert.assertFalse(queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.SUB_UNI, uniqueId, "topicTAC")));

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
        final ActiveTopicAdvertsQueue<AutoDiscTopicInfo> queue = new ActiveTopicAdvertsQueue<>(1000);

        // Add some elements separated by some time
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId1, "topic"));
        Thread.sleep(250);
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.SUB_MUL, uniqueId2, "topic2"));
        Thread.sleep(250);
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_UNI, uniqueId3, "topic3"));

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
        final ActiveTopicAdvertsQueue<AutoDiscTopicInfo> queue = new ActiveTopicAdvertsQueue<>(1000);

        // Add some elements separated by some time
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId1, "topic"));
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.SUB_MUL, uniqueId2, "topic2"));
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_UNI, uniqueId3, "topic3"));

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

    @Test
    public void testMultipleElementAndTimeouts() throws Exception
    {
        final UUID instanceId = UUID.randomUUID();

        final UUID uniqueId1 = UUID.randomUUID();
        final UUID uniqueId2 = UUID.randomUUID();
        final UUID uniqueId3 = UUID.randomUUID();
        final UUID uniqueId4 = UUID.randomUUID();
        final UUID uniqueId5 = UUID.randomUUID();
        final UUID uniqueId6 = UUID.randomUUID();

        // Create the queue
        final ActiveTopicAdvertsQueue<AutoDiscTopicInfo> queue = new ActiveTopicAdvertsQueue<>(1000);

        // Add some elements separated by some time
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId1, "topic"));
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_MUL, uniqueId2, "topic"));
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_UNI, uniqueId3, "topic2"));
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.SUB_IPC, uniqueId4, "topic2"));
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.SUB_MUL, uniqueId5, "topic3"));
        queue.addOrUpdateAdvert(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.SUB_UNI, uniqueId6, "topic3"));

        // The queue should have 6 elements
        AtomicLong sum = new AtomicLong(0);
        queue.runForEachElement((element) -> sum.getAndIncrement());
        Assert.assertTrue(sum.get() == 6);
        sum.set(0);

        // Test consume if match, it shouldn't match anyone
        queue.consumeIfMatch("topic", AutoDiscTransportType.PUB_UNI, (element) -> sum.getAndIncrement());
        Assert.assertTrue(sum.get() == 0);

        // Test consume if match, it shouldn't match anyone
        queue.consumeIfMatch("topicc", AutoDiscTransportType.PUB_IPC, (element) -> sum.getAndIncrement());
        Assert.assertTrue(sum.get() == 0);

        // Test consume if match, it should match one now
        queue.consumeIfMatch("topic", AutoDiscTransportType.PUB_IPC, (element) -> sum.getAndIncrement());
        Assert.assertTrue(sum.get() == 1);
        sum.set(0);

        // Test consume if topic match filter
        final Set<UUID> matches = new HashSet<>();
        queue.consumeIfTopicMatchFilter((topic) -> topic.endsWith("2"), (element) -> matches.add(element.getUniqueId()));
        Assert.assertTrue(matches.contains(uniqueId3));
        Assert.assertTrue(matches.contains(uniqueId4));
        Assert.assertTrue(matches.size() == 2);
    }
}