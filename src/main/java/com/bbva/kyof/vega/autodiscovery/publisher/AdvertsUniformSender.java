package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;
/**
 * Implementation of IAdvertsUniformSender to send adverts uniformly
 */
@Slf4j
public class AdvertsUniformSender implements IAdvertsUniformSender
{
	/** Autodiscovery configuration */
	private final AutoDiscoveryConfig config;

	/** Time of last burst of adverts sent*/
	private long lastBurst = 0;

	/**
	 * Constructor
	 * @param pConfig configuration
	 */
	AdvertsUniformSender(final AutoDiscoveryConfig pConfig)
	{
		this.config = pConfig;
	}

	/**
	 * To send uniformly the discovery adverts, Vega divide the refresh interval configured by the total number
	 * of adverts to calculate the time of each interval (each of these intervals are called burst intervals).
	 *
	 * The minimum burt interval is 1 ms
	 *
	 * @param numAdverts total number of adverts
	 * @return the duration of the burs interval
	 */
	private long getBurstInterval(final int numAdverts)
	{
		// Search the best burst interval
		if(numAdverts > 0 && config.getRefreshInterval()/numAdverts > 1)
		{
			//For small number of adverts, calcule the burst interval
			return config.getRefreshInterval()/numAdverts;
		}
		else
		{
			//For a big value of adverts and a small refresh interval, fix the default interval to 1 ms
			return 1;
		}
	}

	/**
	 * Calculate the number of adverts to send in this interval.
	 *
	 * The minimum value returned is 1 advert when the burst interval is > 1 ms (this means there are many time for
	 * sending adverts)
	 *
	 * If the burst interval is = 1, it is necessary to calculate the number of adverts to send
	 * (more than 1 advert each ms).
	 *
	 * @param advertsCount total number of adverts
	 * @param burstInterval miliseconds of each interval
	 * @return the number of adverts to send in this interval
	 */
	private int getNumberOfAdvertsToSend(final int advertsCount, final long burstInterval)
	{
		//If burtInterval > 1 means that the number of adverts to send each burst is 1
		if(burstInterval > 1)
		{
			//the minimum is 1 advert each burst interval
			return 1;
		}
		else
		{
			//advertsCount is an integer, so the result will be an integer too => cast to int
			return (int) (advertsCount / config.getRefreshInterval());
		}
	}

	@Override
	public int sendBurstAdverts(final RegisteredInfoQueue registeredInfos,
			final Consumer consumer)
	{
		final long currentTime = System.currentTimeMillis();

		//get all the topics
		final int advertsCount = registeredInfos.size();

		//calcule the burst interval
		final long burstInterval = getBurstInterval(advertsCount);

		int numAdvertsSent = 0;

		if(advertsCount > 0 && lastBurst + burstInterval < currentTime)
		{
			lastBurst = currentTime;

			//calcule the number of adverts to send in this interval
			final int numberOfAdvertsToSend = getNumberOfAdvertsToSend(advertsCount, burstInterval);

			// Reset timeouts and send the numberOfAdvertsToSend adverts
			numAdvertsSent = registeredInfos.resetNextSendTimeAndMultipleConsume(
					currentTime, numberOfAdvertsToSend,
					info -> consumer.accept(info));

		}

		return numAdvertsSent;
	}

}
