package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.autodiscovery.model.IAutoDiscTopicInfo;

import java.util.function.Consumer;

/**
 * Interface to control all the discovery adverts sending
 * to distribute the adverts traffic uniformly
 *
 */
@FunctionalInterface
public interface IAdvertsUniformSender<T extends IAutoDiscTopicInfo>
{

    /**
     * Distribute the adverts uniformly over a refresh interval
     *
     * This method calculate the number of intervals and the number of topic adverts
     * to send inside each interval, to distribute uniformly the traffic.
     *
     * @param registeredInfos structure with the registered topicInfos
     * @param consumer consumer
     * @return the number of topic adverts sent
     */
    int sendBurstAdverts(RegisteredInfoQueue<T> registeredInfos, Consumer<T> consumer);

}
