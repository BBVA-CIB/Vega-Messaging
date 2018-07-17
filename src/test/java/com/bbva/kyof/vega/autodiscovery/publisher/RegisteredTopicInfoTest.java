package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Created by cnebrera on 02/08/16.
 */
public class RegisteredTopicInfoTest
{
    @Test
    public void getInfoEqualsAndHashCode()
    {
        final UUID instanceId = UUID.randomUUID();

        final AutoDiscTopicInfo topicInfo = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic");
        final AutoDiscTopicInfo topicInfo2 = new AutoDiscTopicInfo(instanceId, AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic");

        final RegisteredTopicInfo<AutoDiscTopicInfo> registeredInfo1 = new RegisteredTopicInfo<>(topicInfo, 100);
        final RegisteredTopicInfo<AutoDiscTopicInfo> registeredInfo2 = new RegisteredTopicInfo<>(topicInfo, 100);
        final RegisteredTopicInfo<AutoDiscTopicInfo> registeredInfo3 = new RegisteredTopicInfo<>(topicInfo2, 100);

        Assert.assertTrue(registeredInfo1.getInfo() == topicInfo);

        Assert.assertEquals(registeredInfo1, registeredInfo1);
        Assert.assertEquals(registeredInfo1, registeredInfo2);
        Assert.assertNotEquals(registeredInfo1, registeredInfo3);
        Assert.assertNotEquals(registeredInfo1, null);
        Assert.assertNotEquals(registeredInfo1, new Object());

        Assert.assertTrue(registeredInfo1.hashCode() == registeredInfo2.hashCode());
        Assert.assertFalse(registeredInfo1.hashCode() == registeredInfo3.hashCode());
    }
}