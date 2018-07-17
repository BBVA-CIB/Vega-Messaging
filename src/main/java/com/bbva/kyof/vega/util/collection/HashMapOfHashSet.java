package com.bbva.kyof.vega.util.collection;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Represent a HashMap which value is a set of elements.<p>
 *
 * When there is no more elements on the set, the whole set and key are removed from the map. It never contains empty sets.<p>
 *
 * This class is not thread-safe.
 *
 * @param <K> The key type of the map
 * @param <V> The value type of the map
 */
public class HashMapOfHashSet<K, V>
{
    /** The map if hash set */
    private final Map<K, Set<V>> map = new HashMap<>();

    /**
     * Put a new value for the given key. If there is no set yet for the key it will create a new one,
     * in other case it will add it to the existing set.<p>
     *
     * If the value for the key is already there it wont be inserted.
     *
     *
     * @param key the key for the value to put
     * @param value the value to add in the set
     *
     * @return true if added, false if it was already contained
     */
    public boolean put(final K key, final V value)
    {
        Set<V> set = this.map.get(key);

        if (set == null)
        {
            set = new HashSet<>();
            this.map.put(key, set);
        }

        return set.add(value);
    }

    /**
     * Remove the value for the given key. It will remove the internal set for the key if the set is empty
     *
     * @param key the key of the element to remove
     * @param value the value of the element to remove
     * @return true if removed, false if it didn't exists
     */
    public boolean remove(final K key, final V value)
    {
        final Set<V> set = this.map.get(key);

        if (set == null)
        {
            return false;
        }

        final boolean removed = set.remove(value);

        // If removed and there is nothing else in the set, remove the key entry
        if (removed && set.isEmpty())
        {
            this.map.remove(key);
        }

        return removed;
    }

    /**
     * Remove all values for a given key
     *
     * @param key the key of the element to remove
     * @return true if removed, false if it didn't exists
     */
    public boolean removeKey(final K key)
    {
        return this.map.remove(key) != null;
    }

    /**
     * True if there is a set for the given key
     *
     * @param key the key to check
     * @return true if there is a set for the given key
     */
    public boolean containsKey(final K key)
    {
        return map.get(key) != null;
    }

    /**
     * Returns true if the set represented by the key contains the given value
     *
     * @param key the key of the set
     * @param value the value to check inside the set
     * @return the internal value
     */
    public boolean containsValue(final K key, final V value)
    {
        final Set<V> set = map.get(key);

        return set != null && set.contains(value);
    }

    /**
     * Clear the contents
     */
    public void clear()
    {
        map.clear();
    }

    /**
     * Consume all elements in the set that match the given key
     * @param key the key of the elements set to consume
     * @param consumer the consumer function
     */
    public void consumeIfKeyEquals(final K key, final Consumer<V> consumer)
    {
        final Set<V> values = this.map.get(key);

        if (values != null)
        {
            values.forEach(consumer);
        }
    }

    /**
     * Remove the set that match the given key and consume all the set elements
     * @param key the key of the set to remove
     * @param consumer the consumer for the set elements
     */
    public void removeAndConsumeIfKeyEquals(final K key, final Consumer<V> consumer)
    {
        final Set<V> valueSet = this.map.remove(key);

        if (valueSet == null)
        {
            return;
        }

        // Consume all set elements of the set
        valueSet.forEach(consumer);
    }

    /**
     * Return true if any value for the given key match the given filter
     * @param key the key of the set
     * @param filter the filter that any of the elements in the set should match
     *
     * @return true if there is a match, false in other case
     */
    public boolean anyValueForKeyMatchFilter(final K key, final Predicate<V> filter)
    {
        final Set<V> valueSet = this.map.get(key);

        if (valueSet == null)
        {
            return false;
        }

        // Consume all set elements of the set
        for (final V element : valueSet)
        {
            if (filter.test(element))
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Consume all stored elements sets whose key match the given filter
     *
     * @param keyFilter the filter for the key
     * @param consumer the consumer for the sets of elements that match the key
     */
    public void consumeIfKeyMatchFilter(final Predicate<K> keyFilter, final Consumer<V> consumer)
    {
        this.map.forEach((key, value) ->
        {
            if (keyFilter.test(key))
            {
                value.forEach(consumer);
            }
        });
    }
}
