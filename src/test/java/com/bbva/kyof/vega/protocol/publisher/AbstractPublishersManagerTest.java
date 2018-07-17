package com.bbva.kyof.vega.protocol.publisher;

import com.bbva.kyof.vega.autodiscovery.AutodiscManager;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.config.general.TopicTemplateConfig;
import com.bbva.kyof.vega.config.general.TransportMediaType;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.control.IOwnSecPubTopicsChangesListener;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by cnebrera on 11/08/16.
 */
public class AbstractPublishersManagerTest
{
    TopicTemplateConfig topicConfigUnicast;
    TopicTemplateConfig topicConfigIpc;
    TopicTemplateConfig topicConfigMulticast;
    VegaContext vegaContext;
    AbstractPublishersManager<AbstractTopicPublisher> publisherManager;
    AutodiscManager autodiscManager;

    @Before
    public void beforeTest()
    {
        topicConfigUnicast = TopicTemplateConfig.builder().name("name").transportType(TransportMediaType.UNICAST).build();
        topicConfigMulticast = TopicTemplateConfig.builder().name("name2").transportType(TransportMediaType.MULTICAST).build();
        topicConfigIpc = TopicTemplateConfig.builder().name("name3").transportType(TransportMediaType.IPC).build();
        autodiscManager = EasyMock.createNiceMock(AutodiscManager.class);
        vegaContext = EasyMock.createNiceMock(VegaContext.class);

        EasyMock.expect(vegaContext.getAutodiscoveryManager()).andAnswer(() -> autodiscManager).anyTimes();
        EasyMock.replay(autodiscManager, vegaContext);

        final IOwnSecPubTopicsChangesListener secPubTopicsChangesListener = EasyMock.createNiceMock(IOwnSecPubTopicsChangesListener.class);
        EasyMock.replay(secPubTopicsChangesListener);

        publisherManager = new AbstractPublishersManager<AbstractTopicPublisher>(vegaContext, secPubTopicsChangesListener)
        {
            @Override
            protected void processCreatedTopicPublisher(AbstractTopicPublisher topicPublisher)
            {

            }

            @Override
            protected AbstractTopicPublisher instantiateTopicPublisher(String topicName, TopicTemplateConfig templateCfg)
            {
                final AbstractTopicPublisher result = EasyMock.createNiceMock(AbstractTopicPublisher.class);
                EasyMock.expect(result.getTopicConfig()).andAnswer(() -> templateCfg).anyTimes();
                EasyMock.expect(result.getTopicName()).andAnswer(() -> topicName).anyTimes();
                EasyMock.replay(result);
                return result;
            }

            @Override
            protected AbstractTopicPublisher instantiateSecureTopicPublisher(String topicName, TopicTemplateConfig templateCfg, TopicSecurityTemplateConfig securityTemplateConfig) throws VegaException
            {
                return null;
            }

            @Override
            protected void processTopicPublisherBeforeDestroy(AbstractTopicPublisher topicPublisher)
            {

            }

            @Override
            protected void cleanAfterClose()
            {

            }

            @Override
            public void onNewAutoDiscTopicInfo(AutoDiscTopicInfo info)
            {

            }

            @Override
            public void onTimedOutAutoDiscTopicInfo(AutoDiscTopicInfo info)
            {

            }

            @Override
            public void onNewAutoDiscTopicSocketInfo(AutoDiscTopicSocketInfo info)
            {

            }

            @Override
            public void onTimedOutAutoDiscTopicSocketInfo(AutoDiscTopicSocketInfo info)
            {

            }
        };
    }

    @After
    public void afterTest() throws Exception
    {
        publisherManager.close();
    }

    @Test
    public void testCreateTopicPublisher() throws Exception
    {
        final AbstractTopicPublisher AbstractTopicPublisher = publisherManager.createTopicPublisher("topic", topicConfigUnicast, null);

        Assert.assertEquals(AbstractTopicPublisher.getTopicConfig(), topicConfigUnicast);
        Assert.assertEquals(AbstractTopicPublisher.getTopicName(), "topic");
    }

    @Test
    public void testCreateTopicPublisherMulticast() throws Exception
    {
        final AbstractTopicPublisher AbstractTopicPublisher = publisherManager.createTopicPublisher("topic", topicConfigMulticast, null);

        Assert.assertEquals(AbstractTopicPublisher.getTopicConfig(), topicConfigMulticast);
        Assert.assertEquals(AbstractTopicPublisher.getTopicName(), "topic");
    }

    @Test
    public void testCreateTopicPublisherIpc() throws Exception
    {
        final AbstractTopicPublisher AbstractTopicPublisher = publisherManager.createTopicPublisher("topic", topicConfigIpc, null);

        Assert.assertEquals(AbstractTopicPublisher.getTopicConfig(), topicConfigIpc);
        Assert.assertEquals(AbstractTopicPublisher.getTopicName(), "topic");
    }

    @Test(expected = VegaException.class)
    public void testCreateTopicPublisherOnClosed() throws Exception
    {
        publisherManager.close();
        final AbstractTopicPublisher AbstractTopicPublisher = publisherManager.createTopicPublisher("topic", topicConfigUnicast, null);
    }

    @Test
    public void testGetTopicPublisher() throws Exception
    {
        final AbstractTopicPublisher AbstractTopicPublisher = publisherManager.createTopicPublisher("topic", topicConfigUnicast, null);

        Assert.assertTrue(AbstractTopicPublisher == publisherManager.getTopicPublisherForTopicName("topic"));
        Assert.assertNull(publisherManager.getTopicPublisherForTopicName("topic2"));
    }

    @Test
    public void testGetTopicPublisherAfterClose() throws Exception
    {
        final AbstractTopicPublisher AbstractTopicPublisher = publisherManager.createTopicPublisher("topic", topicConfigUnicast, null);

        publisherManager.close();

        Assert.assertNull(publisherManager.getTopicPublisherForTopicName("topic"));
    }

    @Test(expected = VegaException.class)
    public void testCreateTopicPublisherTwice() throws Exception
    {
        final AbstractTopicPublisher AbstractTopicPublisher = publisherManager.createTopicPublisher("topic", topicConfigUnicast, null);
        publisherManager.createTopicPublisher("topic", topicConfigUnicast, null);
    }

    @Test
    public void removePublisher() throws Exception
    {
        final AbstractTopicPublisher AbstractTopicPublisher = publisherManager.createTopicPublisher("topic", topicConfigUnicast, null);
        publisherManager.destroyTopicPublisher("topic");
    }

    @Test(expected = VegaException.class)
    public void removePublisherOnClosed() throws Exception
    {
        final AbstractTopicPublisher AbstractTopicPublisher = publisherManager.createTopicPublisher("topic", topicConfigUnicast, null);

        this.publisherManager.close();

        publisherManager.destroyTopicPublisher("topic");
    }

    @Test(expected = VegaException.class)
    public void removePublisherTwice() throws Exception
    {
        final AbstractTopicPublisher AbstractTopicPublisher = publisherManager.createTopicPublisher("topic", topicConfigUnicast, null);
        publisherManager.destroyTopicPublisher("topic");
        publisherManager.destroyTopicPublisher("topic");
    }
}