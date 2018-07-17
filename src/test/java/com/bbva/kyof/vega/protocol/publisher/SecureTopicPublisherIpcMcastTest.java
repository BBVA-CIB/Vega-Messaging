package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.util.crypto.AESCrypto;
import org.agrona.concurrent.UnsafeBuffer;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by cnebrera on 15/11/2016.
 */
public class SecureTopicPublisherIpcMcastTest
{
    private TopicTemplateConfig topicConfig;
    private TopicSecurityTemplateConfig securityTemplateConfig;
    private VegaContext vegaContext;

    @Before
    public void beforeTest()
    {
        topicConfig = TopicTemplateConfig.builder().name("name").transportType(TransportMediaType.MULTICAST).build();
        final Set<Integer> pubTopic1SecureSubs = new HashSet<>(Collections.singletonList(22222));
        final Set<Integer> pubTopic1SecurePubs = new HashSet<>(Collections.singletonList(11111));
        securityTemplateConfig = new TopicSecurityTemplateConfig("topic1", 100L, pubTopic1SecureSubs, pubTopic1SecurePubs);

        vegaContext = new VegaContext(null, null);
    }

    @Test
    public void test() throws Exception
    {
        final SecureTopicPublisherIpcMcast topicPublisher = new SecureTopicPublisherIpcMcast("topic", topicConfig, vegaContext, securityTemplateConfig);

        // Create the Aeron publishers
        AeronPublisher publisher = createAeronPublisherMock();
        topicPublisher.setAeronPublisher(publisher);

        Assert.assertEquals(topicPublisher.getTopicSecurityConfig(), securityTemplateConfig);

        // Session key length should be 128
        Assert.assertEquals(topicPublisher.getSessionKey().length, AESCrypto.DEFAULT_KEY_SIZE);
        Assert.assertTrue(topicPublisher.hasSecurity());

        final UnsafeBuffer messageBuffer = new UnsafeBuffer(ByteBuffer.allocate(128));
        messageBuffer.putInt(0, 128);

        topicPublisher.sendToAeron(messageBuffer, 0, 0, 4);
    }

    private AeronPublisher createAeronPublisherMock()
    {
        AeronPublisher publisher = EasyMock.createNiceMock(AeronPublisher.class);
        EasyMock.replay(publisher);
        return publisher;
    }
}