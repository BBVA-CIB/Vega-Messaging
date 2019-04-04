package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.config.general.AutoDiscoType;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.Publication;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Created by cnebrera on 02/08/16.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Aeron.class)
public class AutodiscMcastSenderTest
{
    @Test
    public void testCreatePublication() throws Exception
    {
        final Aeron aeron = PowerMock.createNiceMock(Aeron.class);
        final ConcurrentPublication publication = EasyMock.createNiceMock(ConcurrentPublication.class);
        EasyMock.expect(aeron.addPublication(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(publication).anyTimes();
        EasyMock.replay(publication);
        PowerMock.replayAll(aeron);

        // Create the configuration, with 300 millis refresh interval
        final AutoDiscoveryConfig config = AutoDiscoveryConfig.builder().autoDiscoType(AutoDiscoType.MULTICAST).refreshInterval(100L).build();
        config.completeAndValidateConfig();

        AutodiscMcastSender sender = new AutodiscMcastSender(aeron, config);
    }
}