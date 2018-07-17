package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscDaemonClientInfo;
import com.bbva.kyof.vega.config.general.AutoDiscoType;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
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

import java.util.UUID;

/**
 * Created by cnebrera on 02/08/16.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Aeron.class)
public class AutodiscUnicastSenderTest
{
    final AutoDiscDaemonClientInfo daemonClientInfo = new AutoDiscDaemonClientInfo(UUID.randomUUID(), 12, 13, 14);

    @Test
    public void testCreatePublication() throws Exception
    {
        final Aeron aeron = PowerMock.createNiceMock(Aeron.class);
        final ConcurrentPublication publication = EasyMock.createNiceMock(ConcurrentPublication.class);
        EasyMock.expect(aeron.addPublication(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(publication).anyTimes();
        EasyMock.replay(publication);
        PowerMock.replayAll(aeron);

        // Create the configuration, with 300 millis refresh interval
        final AutoDiscoveryConfig config = AutoDiscoveryConfig.builder()
                .autoDiscoType(AutoDiscoType.UNICAST_DAEMON)
                .resolverDaemonAddress("192.168.0.1")
                .refreshInterval(100L).build();
        config.completeAndValidateConfig();

        AutodiscUnicastSender sender = new AutodiscUnicastSender(aeron, config, daemonClientInfo);

        Assert.assertTrue(sender.sendNextTopicAdverts() == 0);

        Thread.sleep(400);

        Assert.assertTrue(sender.sendNextTopicAdverts() == 1);
    }
}