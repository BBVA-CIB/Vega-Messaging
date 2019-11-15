package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.autodiscovery.model.IAutoDiscTopicInfo;
import com.bbva.kyof.vega.util.collection.HashMapOfHashSet;
import com.bbva.kyof.vega.util.collection.HashMapStack;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * This class represents a queue of registered information to be periodically sent.
 *
 * The queue stores the information on a HashMapStack by stored information unique id. This allows quick access
 * for information removal and existence check and at the same time by using a HashMapStack it keeps the order of
 * the added elements.
 *
 * The order of insertion is performed to ensure the next element that may need to be sent is the last one added. Every
 * time an element is returned to be sent, is removed and added at the end. Since the send interval is shared by all elements
 * this mechanism ensures that only checking the next element is enough to know what has to be sent.
 *
 * The queue also stores the information separated by topic name, this allows quick search when we need to republish
 * all the information belonging to an specific topic name.
 *
 * This class is not thread safe!
 */
class RegisteredInfoQueue<T extends IAutoDiscTopicInfo>
{
    /** Linked map with all the registered infos by unique id, is the one used to keep order and check for timeouts */
    private final HashMapStack<UUID, RegisteredTopicInfo<T>> registeredInfosById = new HashMapStack<>();
    /** Map with all the registered infos by the topic name they belong to */
    private final HashMapOfHashSet<String, RegisteredTopicInfo<T>> registeredInfosByTopic = new HashMapOfHashSet<>();
    /** Send interval for the registered information of this queue */
    private final long sendInterval;

    /**
     * Constructs a new instance of the queue
     *
     * @param sendInterval send interval for the elements of the queue
     */
    RegisteredInfoQueue(final long sendInterval)
    {
        this.sendInterval = sendInterval;
    }

    /**
     * Adds a new element into the queue. It will check that the element is not already there by looking for the unique id
     * of the element. If already contained the call will be ignored.
     *
     * @param element the element to add
     * @return true if the element has been added, false if it already exists
     */
    public boolean add(final T element)
    {
        // If is not contained, create and store
        if (!registeredInfosById.containsKey(element.getUniqueId()))
        {
            final RegisteredTopicInfo<T> registeredInfo = new RegisteredTopicInfo<>(element, this.sendInterval);
            this.registeredInfosById.put(element.getUniqueId(), registeredInfo);
            this.registeredInfosByTopic.put(element.getTopicName(), registeredInfo);
            return true;
        }

        return false;
    }

    /**
     * Get the next element in the queue that should be sent. The element should be sent if the next send time is bigger
     * than the given current time. It only checks the oldest element in the LinkedMap since they should be ordered by send time.
     *
     * If an element is returned it will also update the next send time for the element and re-add it it to be the newest in the map.
     *
     * @param currentTime the current time in milliseconds
     * @return the next element to send, null if is empty or no element has reached the expected send time following the send interval
     */
    T getNextIfShouldSend(final long currentTime)
    {
        // Get the eldest key value pair on the map
        final UUID eldestKey = this.registeredInfosById.getEldestKey();
        final RegisteredTopicInfo<T> eldestValue = this.registeredInfosById.getEldestValue();

        // If no eldest key or value the map is empty
        if (eldestKey == null || eldestValue == null)
        {
            return null;
        }

        // Check if it should be sent and reset the internal times if positive
        if (eldestValue.checkIfshouldSendAndResetIfRequired(currentTime))
        {
            // Convert the eldest entry into the newest since the send time has been reset
            this.registeredInfosById.removeAndPut(eldestKey, eldestValue);

            // Return the element
            return eldestValue.getInfo();
        }

        return null;
    }

    /**
     * Remove an element from the queue given the unique id of the element
     *
     * @param uniqueId the stored element unique id
     * @return true if the element has been removed, false if it was not there
     */
    public boolean remove(final UUID uniqueId)
    {
        final RegisteredTopicInfo<T> removedInfo = this.registeredInfosById.remove(uniqueId);

        if (removedInfo != null)
        {
            this.registeredInfosByTopic.remove(removedInfo.getInfo().getTopicName(), removedInfo);
            return true;
        }

        return false;
    }

    /** Clear the queue removing all stored information */
    public void clear()
    {
        this.registeredInfosById.clear();
        this.registeredInfosByTopic.clear();
    }

    /**
     * Reset the next send time for any registered event that match the given topic and match the given predicate.
     *
     * It will also consume each element that match with the given topic name.
     *
     * @param topicName the name of the topic to look for
     * @param now current time in milliseconds, it will be used to set the next send interval for each element
     * @param filter additional filter information
     * @param consumer the consumer for the registered event which sendInterval has been reset
     *
     */
    void resetNextSendTimeAndConsume(final String topicName, final long now, final Predicate<T> filter, final Consumer<T> consumer)
    {
        this.registeredInfosByTopic.consumeIfKeyEquals(topicName, registeredInfo ->
        {
            final T info = registeredInfo.getInfo();

            if (filter.test(info))
            {
                // Remove and add again. If it already exists it will force to move it to the beginning.
                this.registeredInfosById.removeAndPut(info.getUniqueId(), registeredInfo);

                // Reset the sendInterval
                registeredInfo.resetNextExpectedSent(now);

                // Consume
                consumer.accept(info);
            }
        });
    }

    /**
     * @return the number of elements
     */
    int size()
    {
        return registeredInfosById.size();
    }

    /**
     * Reset the next send time and consume the first numberOfAdvertsToConsume elements.
     *
     * @param now current time in milliseconds, it will be used to set the next send interval for each element
     * @param numberOfAdvertsToConsume number of elements to consume
     * @param consumer the consumer for the registered events which sendInterval has been reset
     *
     */
    int resetNextSendTimeAndMultipleConsume(final long now, final int numberOfAdvertsToConsume, final Consumer<T> consumer)
    {
        //Iterate numberOfAdvertsToConsume times
        for(int i = 0; i < numberOfAdvertsToConsume; i++)
        {
            // Get the eldest key value pair on the map
            final UUID eldestKey = this.registeredInfosById.getEldestKey();
            final RegisteredTopicInfo<T> eldestValue = this.registeredInfosById.getEldestValue();

            // If no eldest key or value the map is empty
            if (eldestKey == null || eldestValue == null)
            {
                // Return the number of elements processed
                return i;
            }

            //reset the sendInterval
            eldestValue.resetNextExpectedSent(now);

            // Convert the eldest entry into the newest since the send time has been reset
            this.registeredInfosById.removeAndPut(eldestKey, eldestValue);

            // Consume
            consumer.accept(eldestValue.getInfo());
        }

        // Return the number of elements processed
        return numberOfAdvertsToConsume;
    }
}
