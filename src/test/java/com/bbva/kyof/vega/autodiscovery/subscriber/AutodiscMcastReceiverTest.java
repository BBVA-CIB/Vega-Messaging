package com.bbva.kyof.vega.autodiscovery.subscriber;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.config.general.AutoDiscoType;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import io.aeron.Aeron;
import io.aeron.Subscription;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.UUID;

/**
 * Created by cnebrera on 04/08/16.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Aeron.class)
public class AutodiscMcastReceiverTest implements IAutodiscGlobalEventListener
{
    @Test
    public void testCreatePublication() throws Exception
    {
        final Aeron aeron = PowerMock.createNiceMock(Aeron.class);
        final Subscription subscription = EasyMock.createNiceMock(Subscription.class);
        EasyMock.expect(aeron.addSubscription(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(subscription).anyTimes();
        EasyMock.replay(subscription);
        PowerMock.replayAll(aeron);

        // Create the configuration, with 300 millis refresh interval
        final AutoDiscoveryConfig config = AutoDiscoveryConfig.builder().autoDiscoType(AutoDiscoType.MULTICAST).refreshInterval(100L).build();
        config.completeAndValidateConfig();

        AutodiscMcastReceiver receiver = new AutodiscMcastReceiver(UUID.randomUUID(), aeron, config, this);
    }

    @Override
    public void onNewInstanceInfo(AutoDiscInstanceInfo info)
    {

    }

    @Override
    public void onNewTopicInfo(AutoDiscTopicInfo info)
    {

    }
}