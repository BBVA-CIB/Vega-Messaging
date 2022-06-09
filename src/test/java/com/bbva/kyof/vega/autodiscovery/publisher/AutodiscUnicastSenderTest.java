package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscDaemonClientInfo;
import com.bbva.kyof.vega.config.general.AutoDiscoType;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.config.general.UnicastInfo;
import com.bbva.kyof.vega.exception.VegaException;
import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.Publication;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.UUID;

/**
 * Created by cnebrera on 02/08/16.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Aeron.class)
public class AutodiscUnicastSenderTest
{
    final AutoDiscDaemonClientInfo daemonClientInfo = new AutoDiscDaemonClientInfo(UUID.randomUUID(), 12, 13, 14, "unicast_host");
    boolean testIsClosed = false;

    @Test
    public void testCreatePublication() throws Exception
    {
        final Aeron aeron = PowerMock.createNiceMock(Aeron.class);
        final ConcurrentPublication publication = EasyMock.createNiceMock(ConcurrentPublication.class);
        final PublicationsManager publicationsManager = EasyMock.createNiceMock(PublicationsManager.class);
        final PublicationInfo publicationInfo =
                new PublicationInfo(publication, null, 0, 0, true);
        final PublicationInfo[] publicationInfoArray = {publicationInfo};

        EasyMock.expect(aeron.addPublication(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(publication).anyTimes();
        EasyMock.expect(publicationsManager.getPublicationsInfoArray()).andReturn(publicationInfoArray).anyTimes();
        EasyMock.replay(publication, publicationsManager);
        PowerMock.replayAll(aeron);

        // Create the configuration, with 300 millis refresh interval
        final AutoDiscoveryConfig config = AutoDiscoveryConfig.builder()
                .autoDiscoType(AutoDiscoType.UNICAST_DAEMON)
                .unicastInfoArray(Collections.singletonList(new UnicastInfo("192.168.1.1",37000)))
                .refreshInterval(100L).build();
        config.completeAndValidateConfig();

        AutodiscUnicastSender sender = new AutodiscUnicastSender(
                aeron,
                config,
                daemonClientInfo,
                publicationsManager);

        Assert.assertEquals(0, sender.sendNextTopicAdverts());

        Thread.sleep(400);

        Assert.assertEquals(1, sender.sendNextTopicAdverts());
    }

    @Test
    public void testGetPublication() throws VegaException
    {
        final Aeron aeron = PowerMock.createNiceMock(Aeron.class);
        final ConcurrentPublication publication = EasyMock.createNiceMock(ConcurrentPublication.class);
        final PublicationsManager publicationsManager = EasyMock.createNiceMock(PublicationsManager.class);
        final PublicationInfo publicationInfo =
                new PublicationInfo(publication, null, 0, 0, true);

        EasyMock.expect(aeron.addPublication(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(publication).anyTimes();
        EasyMock.expect(publicationsManager.getRandomPublicationInfo()).andReturn(publicationInfo).anyTimes();

        EasyMock.replay(publication, publicationsManager);
        PowerMock.replayAll(aeron);

        // Create the configuration, with 300 millis refresh interval
        final AutoDiscoveryConfig config = AutoDiscoveryConfig.builder()
                .autoDiscoType(AutoDiscoType.UNICAST_DAEMON)
                .unicastInfoArray(Collections.singletonList(new UnicastInfo("192.168.1.1",37000)))
                .refreshInterval(100L).build();
        config.completeAndValidateConfig();

        AutodiscUnicastSender sender = new AutodiscUnicastSender(
                aeron,
                config,
                daemonClientInfo,
                publicationsManager);

        Publication result = sender.getPublication();

        Assert.assertNotNull(result);
        Assert.assertEquals(publication, result);
    }

    @Test
    public void testGetPublicationChangingPublication() throws VegaException
    {
        final Aeron aeron = PowerMock.createNiceMock(Aeron.class);
        final ConcurrentPublication publication = EasyMock.createNiceMock(ConcurrentPublication.class);
        final PublicationsManager publicationsManager = EasyMock.createNiceMock(PublicationsManager.class);
        final PublicationInfo publicationInfoDisabled =
                new PublicationInfo(null, null, 0, 0, false);
        final PublicationInfo publicationInfoEnabled =
                new PublicationInfo(publication, null, 0, 0, false);

        EasyMock.expect(aeron.addPublication(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(publication).anyTimes();
        EasyMock.expect(publicationsManager.hasEnabledPublications()).andReturn(true).anyTimes();

        //First response getRandomPublicationInfo returns the disabled one
        EasyMock.expect(publicationsManager.getRandomPublicationInfo()).andReturn(publicationInfoDisabled).once();
        //Second response getRandomPublicationInfo returns the enabled one
        EasyMock.expect(publicationsManager.getRandomPublicationInfo()).andReturn(publicationInfoEnabled).once();

        EasyMock.replay(publication, publicationsManager);
        PowerMock.replayAll(aeron);

        // Create the configuration, with 300 millis refresh interval
        final AutoDiscoveryConfig config = AutoDiscoveryConfig.builder()
                .autoDiscoType(AutoDiscoType.UNICAST_DAEMON)
                .unicastInfoArray(Collections.singletonList(new UnicastInfo("192.168.1.1",37000)))
                .refreshInterval(100L).build();
        config.completeAndValidateConfig();

        AutodiscUnicastSender sender = new AutodiscUnicastSender(
                aeron,
                config,
                daemonClientInfo,
                publicationsManager);

        Publication result = sender.getPublication();

        Assert.assertNotNull(result);
        Assert.assertEquals(publication, result);
    }

    @Test
    public void testClose() throws VegaException
    {

        final Aeron aeron = PowerMock.createNiceMock(Aeron.class);
        final ConcurrentPublication publication = EasyMock.createNiceMock(ConcurrentPublication.class);
        final PublicationsManager publicationsManager = EasyMock.createNiceMock(PublicationsManager.class);
        final PublicationInfo publicationInfo =
                new PublicationInfo(publication, null, 0, 0, true);
        final PublicationInfo[] publicationInfoArray = {publicationInfo};

        EasyMock.expect(aeron.addPublication(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(publication).anyTimes();
        EasyMock.expect(publicationsManager.getPublicationsInfoArray()).andReturn(publicationInfoArray).anyTimes();

        publication.close();
        EasyMock.expectLastCall().andAnswer(this::closedCalled).anyTimes();

        EasyMock.replay(publication, publicationsManager);
        PowerMock.replayAll(aeron);

        // Create the configuration, with 300 millis refresh interval
        final AutoDiscoveryConfig config = AutoDiscoveryConfig.builder()
                .autoDiscoType(AutoDiscoType.UNICAST_DAEMON)
                .unicastInfoArray(Collections.singletonList(new UnicastInfo("192.168.1.1",37000)))
                .refreshInterval(100L).build();
        config.completeAndValidateConfig();

        AutodiscUnicastSender sender = new AutodiscUnicastSender(
                aeron,
                config,
                daemonClientInfo,
                publicationsManager);
        sender.close();

        Assert.assertTrue(this.testIsClosed);
    }

    private Object closedCalled()
    {
        this.testIsClosed = true;
        return null;
    }
}