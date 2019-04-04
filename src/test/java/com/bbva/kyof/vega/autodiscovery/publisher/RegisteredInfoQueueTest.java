package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by cnebrera on 02/08/16.
 */
public class RegisteredInfoQueueTest
{
    @Test
    public void testAddRemoveClear()
    {
        final RegisteredInfoQueue<AutoDiscTopicInfo> queue = new RegisteredInfoQueue<>(100);
        final UUID instanceId = UUID.randomUUID();
        final UUID uniqueId = UUID.randomUUID();

        // Add an element, since it is empty the result should be true
        Assert.assertTrue(queue.add(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId, "topic")));
        // Add again, the result should be false, they share the unique id
        Assert.assertFalse(queue.add(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.SUB_UNI, uniqueId, "topicTAC")));

        // Now remove the element, the result should be true
        Assert.assertTrue(queue.remove(uniqueId));
        // Remove again, now it should be false
        Assert.assertFalse(queue.remove(uniqueId));

        // Since is not there, we should be able to add again
        Assert.assertTrue(queue.add(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId, "topic")));

        // Now clear
        queue.clear();

        // Remove again, now it should be false, it has been cleared
        Assert.assertFalse(queue.remove(uniqueId));

        // Since is not there, we should be able to add again
        Assert.assertTrue(queue.add(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId, "topic")));
    }

    @Test
    public void getNextIfShouldSend() throws Exception
    {
        final UUID instanceId = UUID.randomUUID();

        final UUID uniqueId1 = UUID.randomUUID();
        final UUID uniqueId2 = UUID.randomUUID();
        final UUID uniqueId3 = UUID.randomUUID();

        // Create the queue
        final RegisteredInfoQueue<AutoDiscTopicInfo> queue = new RegisteredInfoQueue<>(1000);

        // Try on empty queue
        Assert.assertNull(queue.getNextIfShouldSend(System.currentTimeMillis()));

        // Add some elements separated by some time
        queue.add(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId1, "topic"));
        Thread.sleep(250);
        queue.add(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId2, "topic"));
        Thread.sleep(250);
        queue.add(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId3, "topic"));

        // The next element is not yet ready
        Assert.assertNull(queue.getNextIfShouldSend(System.currentTimeMillis()));

        // Wait a bit more
        Thread.sleep(501);
        Assert.assertEquals(queue.getNextIfShouldSend(System.currentTimeMillis()).getUniqueId(), uniqueId1);
        Assert.assertNull(queue.getNextIfShouldSend(System.currentTimeMillis()));

        // Wait a bit more
        Thread.sleep(251);
        Assert.assertEquals(queue.getNextIfShouldSend(System.currentTimeMillis()).getUniqueId(), uniqueId2);
        Assert.assertNull(queue.getNextIfShouldSend(System.currentTimeMillis()));

        // Wait a bit more, the next 2 element should be ready to send
        Thread.sleep(251);
        Assert.assertEquals(queue.getNextIfShouldSend(System.currentTimeMillis()).getUniqueId(), uniqueId3);
        Assert.assertNull(queue.getNextIfShouldSend(System.currentTimeMillis()));

        // Wait a bit more, the next 2 element should be ready to send
        Thread.sleep(501);
        Assert.assertEquals(queue.getNextIfShouldSend(System.currentTimeMillis()).getUniqueId(), uniqueId1);
        Assert.assertNull(queue.getNextIfShouldSend(System.currentTimeMillis()));
    }

    @Test
    public void resetNextSendTime() throws Exception
    {
        final AtomicLong numElementsConsumed = new AtomicLong(0);

        final UUID instanceId = UUID.randomUUID();

        final UUID uniqueId1 = UUID.randomUUID();
        final UUID uniqueId2 = UUID.randomUUID();
        final UUID uniqueId3 = UUID.randomUUID();
        final UUID uniqueId4 = UUID.randomUUID();

        // Create the queue and add 3 elements, one every 100 milliseconds
        final RegisteredInfoQueue<AutoDiscTopicInfo> queue = new RegisteredInfoQueue<>(300);

        queue.add(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId1, "topic1"));
        queue.add(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, uniqueId2, "topic2"));
        queue.add(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.SUB_IPC, uniqueId3, "topic1"));
        queue.add(new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.SUB_IPC, uniqueId4, "topic2"));

        // Wait a bit
        Thread.sleep(400);

        // All should be ready to send, but let's reset time for the first element
        queue.resetNextSendTimeAndConsume("topic1",
                System.currentTimeMillis(),
                (element) -> element.getTransportType() == AutoDiscTransportType.PUB_IPC,
                (element) -> numElementsConsumed.getAndIncrement());

        Assert.assertTrue(numElementsConsumed.get() == 1);
        Assert.assertEquals(queue.getNextIfShouldSend(System.currentTimeMillis()).getUniqueId(), uniqueId2);
        Assert.assertEquals(queue.getNextIfShouldSend(System.currentTimeMillis()).getUniqueId(), uniqueId3);
        Assert.assertEquals(queue.getNextIfShouldSend(System.currentTimeMillis()).getUniqueId(), uniqueId4);
        Assert.assertNull(queue.getNextIfShouldSend(System.currentTimeMillis()));

        // Wait 100, the element 1 should be ready again
        Thread.sleep(400);
        Assert.assertEquals(queue.getNextIfShouldSend(System.currentTimeMillis()).getUniqueId(), uniqueId1);

        // Now remove 2 elements
        queue.remove(uniqueId1);
        queue.remove(uniqueId2);

        numElementsConsumed.set(0);

        // Run again
        queue.resetNextSendTimeAndConsume("topic1",
                System.currentTimeMillis(),
                (element) -> true,
                (element) -> numElementsConsumed.getAndIncrement());

        Assert.assertTrue(numElementsConsumed.get() == 1);

        // Remove last topic 2 element, reset and run again
        queue.remove(uniqueId4);
        numElementsConsumed.set(0);
        queue.resetNextSendTimeAndConsume("topic2",
                System.currentTimeMillis(),
                (element) -> true,
                (element) -> numElementsConsumed.getAndIncrement());

        Assert.assertTrue(numElementsConsumed.get() == 0);
    }
}