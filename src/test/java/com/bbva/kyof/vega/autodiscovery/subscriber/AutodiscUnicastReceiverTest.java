package com.bbva.kyof.vega.autodiscovery.subscriber;

import com.bbva.kyof.vega.autodiscovery.advert.ActiveAdvertsQueue;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscDaemonServerInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.publisher.IPublicationsManager;
import com.bbva.kyof.vega.config.general.AutoDiscoType;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.config.general.UnicastInfo;
import com.bbva.kyof.vega.exception.VegaException;
import io.aeron.Aeron;
import io.aeron.Subscription;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

/**
 * Created by cnebrera on 04/08/16.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Aeron.class)
public class AutodiscUnicastReceiverTest implements IAutodiscGlobalEventListener
{

    private AutodiscUnicastReceiver configureAutodiscUnicastReceiver(IPublicationsManager publicationsManager) throws VegaException
    {
        final Aeron aeron = PowerMock.createNiceMock(Aeron.class);
        final Subscription subscription = EasyMock.createNiceMock(Subscription.class);
        EasyMock.expect(aeron.addSubscription(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(subscription).anyTimes();
        EasyMock.replay(subscription);
        PowerMock.replayAll(aeron);

        // Create the configuration, with 300 millis refresh interval
        final AutoDiscoveryConfig config = AutoDiscoveryConfig.builder()
                .autoDiscoType(AutoDiscoType.UNICAST_DAEMON)
                .unicastInfoArray(Collections.singletonList(new UnicastInfo("192.168.1.1",37000)))
                .refreshInterval(100L).build();
        config.completeAndValidateConfig();

        return new AutodiscUnicastReceiver(UUID.randomUUID(), aeron, config, this, publicationsManager);
    }

    @Test
    public void testCreatePublication() throws Exception
    {
        configureAutodiscUnicastReceiver(null);
    }

    @Test
    public void testProcessAutoDiscDaemonServerInfoMsg()
            throws VegaException, NoSuchFieldException, IllegalAccessException
    {
        final AutoDiscDaemonServerInfo autoDiscDaemonServerInfo = EasyMock.createNiceMock(AutoDiscDaemonServerInfo.class);

        final IPublicationsManager publicationsManager = EasyMock.createNiceMock(IPublicationsManager.class);

        AutodiscUnicastReceiver autodiscUnicastReceiver = configureAutodiscUnicastReceiver(publicationsManager);

        //Create the mocks for the private fields
        final ActiveAdvertsQueue<AutoDiscDaemonServerInfo> autoDiscDaemonServerInfoActiveAdvertsQueue = EasyMock.createNiceMock(ActiveAdvertsQueue.class);
        EasyMock.expect( autoDiscDaemonServerInfoActiveAdvertsQueue.addOrUpdateAdvert(autoDiscDaemonServerInfo) )
                .andReturn(true).anyTimes();

        EasyMock.replay(autoDiscDaemonServerInfo, autoDiscDaemonServerInfoActiveAdvertsQueue, publicationsManager);

        //Mock autoDiscDaemonServerInfoActiveAdvertsQueue with reflection
        Field fieldAutoDiscDaemonServerInfoActiveAdvertsQueue = autodiscUnicastReceiver.getClass().getDeclaredField("autoDiscDaemonServerInfoActiveAdvertsQueue");
        fieldAutoDiscDaemonServerInfoActiveAdvertsQueue.setAccessible(true);
        fieldAutoDiscDaemonServerInfoActiveAdvertsQueue.set(autodiscUnicastReceiver, autoDiscDaemonServerInfoActiveAdvertsQueue);

        boolean result = autodiscUnicastReceiver.processAutoDiscDaemonServerInfoMsg(autoDiscDaemonServerInfo);

        assertTrue(result);
        EasyMock.verify();
    }

    @Test
    public void testCheckAutoDiscDaemonServerInfoTimeouts()
            throws VegaException, NoSuchFieldException, IllegalAccessException
    {
        final AutoDiscDaemonServerInfo autoDiscDaemonServerInfo = EasyMock.createNiceMock(AutoDiscDaemonServerInfo.class);


        final IPublicationsManager publicationsManager = EasyMock.createNiceMock(IPublicationsManager.class);

        AutodiscUnicastReceiver autodiscUnicastReceiver = configureAutodiscUnicastReceiver(publicationsManager);

        //Create the mocks for the private fields
        final ActiveAdvertsQueue<AutoDiscDaemonServerInfo> autoDiscDaemonServerInfoActiveAdvertsQueue = EasyMock.createNiceMock(ActiveAdvertsQueue.class);
        EasyMock.expect( autoDiscDaemonServerInfoActiveAdvertsQueue.returnNextTimedOutElement() )
                .andReturn(autoDiscDaemonServerInfo).anyTimes();

        EasyMock.replay(autoDiscDaemonServerInfo, autoDiscDaemonServerInfoActiveAdvertsQueue, publicationsManager);

        //Mock autoDiscDaemonServerInfoActiveAdvertsQueue with reflection
        Field fieldAutoDiscDaemonServerInfoActiveAdvertsQueue = autodiscUnicastReceiver.getClass().getDeclaredField("autoDiscDaemonServerInfoActiveAdvertsQueue");
        fieldAutoDiscDaemonServerInfoActiveAdvertsQueue.setAccessible(true);
        fieldAutoDiscDaemonServerInfoActiveAdvertsQueue.set(autodiscUnicastReceiver, autoDiscDaemonServerInfoActiveAdvertsQueue);

        int result = autodiscUnicastReceiver.checkAutoDiscDaemonServerInfoTimeouts();

        assertTrue(result == 1);
        EasyMock.verify();
    }

    @Test
    public void testClose() throws VegaException
    {
        AutodiscUnicastReceiver autodiscUnicastReceiver = configureAutodiscUnicastReceiver(null);
        autodiscUnicastReceiver.close();
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