package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.testUtils.ReflectionUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(PowerMockRunner.class)
public class AdvertsUniformSenderTest
{

	@Test
	public void testConstructor() throws NoSuchFieldException, IllegalAccessException
	{
		final AutoDiscoveryConfig config = PowerMock.createMock(AutoDiscoveryConfig.class);
		AdvertsUniformSender advertsUniformSender = new AdvertsUniformSender(config);

		final AutoDiscoveryConfig uniformConfig = (AutoDiscoveryConfig) ReflectionUtils
				.getObjectByReflection(advertsUniformSender, "config");

		Assert.assertEquals(config, uniformConfig);
	}

	@Test
	public void testSendBurstAdverts() throws InterruptedException
	{
		final AtomicLong numElementsConsumed = new AtomicLong(0);

		RegisteredInfoQueue registeredInfos = new RegisteredInfoQueue(100);
		final AutoDiscoveryConfig config = PowerMock.createMock(AutoDiscoveryConfig.class);
		AdvertsUniformSender advertsUniformSender = new AdvertsUniformSender(config);

		//advertsCount = 0 elements, busrtInterval = 1, numberOfAdvertsToSend = 0
		int numAdvertsSent = advertsUniformSender.sendBurstAdverts(registeredInfos,
				(element) -> numElementsConsumed.getAndIncrement());

		Assert.assertEquals(0, numAdvertsSent);
		Assert.assertEquals(0, numElementsConsumed.get());

		registeredInfos.add(new AutoDiscTopicInfo(UUID.randomUUID(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1"));

		//advertsCount = 1 elements, busrtInterval = 5, numberOfAdvertsToSend = 1
		EasyMock.expect(config.getRefreshInterval()).andReturn(5l).anyTimes();
		EasyMock.replay(config);

		numAdvertsSent = advertsUniformSender.sendBurstAdverts(registeredInfos,
				(element) -> numElementsConsumed.getAndIncrement());
		Assert.assertEquals(1, numAdvertsSent);
		Assert.assertEquals(1, numElementsConsumed.get());

        //repeat without timeout
		numAdvertsSent = advertsUniformSender.sendBurstAdverts(registeredInfos,
				(element) -> numElementsConsumed.getAndIncrement());
		Assert.assertEquals(0, numAdvertsSent);
		Assert.assertEquals(1, numElementsConsumed.get());


		//advertsCount = 5 elements, busrtInterval = 1, numberOfAdvertsToSend = 1
		registeredInfos.add(new AutoDiscTopicInfo(UUID.randomUUID(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1"));
		registeredInfos.add(new AutoDiscTopicInfo(UUID.randomUUID(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1"));
		registeredInfos.add(new AutoDiscTopicInfo(UUID.randomUUID(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1"));
		registeredInfos.add(new AutoDiscTopicInfo(UUID.randomUUID(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1"));

		numAdvertsSent = advertsUniformSender.sendBurstAdverts(registeredInfos,
				(element) -> numElementsConsumed.getAndIncrement());
		Assert.assertEquals(1, numAdvertsSent);
		Assert.assertEquals(2, numElementsConsumed.get());

		//advertsCount = 9 elements, busrtInterval = 1, numberOfAdvertsToSend = 1
		Thread.sleep(100);

		registeredInfos.add(new AutoDiscTopicInfo(UUID.randomUUID(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1"));
		registeredInfos.add(new AutoDiscTopicInfo(UUID.randomUUID(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1"));
		registeredInfos.add(new AutoDiscTopicInfo(UUID.randomUUID(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1"));
		registeredInfos.add(new AutoDiscTopicInfo(UUID.randomUUID(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1"));

		numAdvertsSent = advertsUniformSender.sendBurstAdverts(registeredInfos,
				(element) -> numElementsConsumed.getAndIncrement());
		Assert.assertEquals(1, numAdvertsSent);
		Assert.assertEquals(3, numElementsConsumed.get());

		//advertsCount = 10 elements, busrtInterval = 1, numberOfAdvertsToSend = 2
		Thread.sleep(100);

		registeredInfos.add(new AutoDiscTopicInfo(UUID.randomUUID(), AutoDiscTransportType.PUB_IPC, UUID.randomUUID(), "topic1"));

		numAdvertsSent = advertsUniformSender.sendBurstAdverts(registeredInfos,
				(element) -> numElementsConsumed.getAndIncrement());
		Assert.assertEquals(2, numAdvertsSent);
		Assert.assertEquals(5, numElementsConsumed.get());

		Thread.sleep(1);
		numAdvertsSent = advertsUniformSender.sendBurstAdverts(registeredInfos,
				(element) -> numElementsConsumed.getAndIncrement());
		Assert.assertEquals(2, numAdvertsSent);
		Assert.assertEquals(7, numElementsConsumed.get());

		Thread.sleep(1);
		numAdvertsSent = advertsUniformSender.sendBurstAdverts(registeredInfos,
				(element) -> numElementsConsumed.getAndIncrement());
		Assert.assertEquals(2, numAdvertsSent);
		Assert.assertEquals(9, numElementsConsumed.get());

		Thread.sleep(1);
		numAdvertsSent = advertsUniformSender.sendBurstAdverts(registeredInfos,
				(element) -> numElementsConsumed.getAndIncrement());
		Assert.assertEquals(2, numAdvertsSent);
		Assert.assertEquals(11, numElementsConsumed.get());

		Thread.sleep(1);
		numAdvertsSent = advertsUniformSender.sendBurstAdverts(registeredInfos,
				(element) -> numElementsConsumed.getAndIncrement());
		Assert.assertEquals(2, numAdvertsSent);
		Assert.assertEquals(13, numElementsConsumed.get());

		Thread.sleep(1);
		numAdvertsSent = advertsUniformSender.sendBurstAdverts(registeredInfos,
				(element) -> numElementsConsumed.getAndIncrement());
		Assert.assertEquals(2, numAdvertsSent);
		Assert.assertEquals(15, numElementsConsumed.get());
	}
}
