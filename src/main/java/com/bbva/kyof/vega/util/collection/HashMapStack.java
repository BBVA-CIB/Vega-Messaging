package com.bbva.kyof.vega.util.collection;


import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Hash map backed by a double linked list in which elements are keep in inserted order
 *
 * @param <K> Key type of the hashmap
 * @param <V> Value type of the hashmap
 */
public final class HashMapStack<K, V>
{
    /** The internal map of elements */
    private final Map<K, StackElement> hashMap;

    /** The head of the elements on inserted order, represents the most recent inserted element */
    private StackElement head;

    /** The tail of the elements on inserted order, represent the oldest inserted element */
    private StackElement tail;

    /** Constructs a new empty map with default size */
    public HashMapStack()
    {
        this.hashMap = new HashMap<>();
    }

    /**
     * Constructs a new linked hashmap given the initial capacity
     * @param initialCapacity initial capacity of the map
     */
    public HashMapStack(final int initialCapacity)
    {
        this.hashMap = new HashMap<>(initialCapacity);
    }

    /**
     * Constructs a new linked hashmap given the initial capacity and load factor
     * @param initialCapacity initial capacity of the map
     * @param loadFactor load factor of the map
     */
    public HashMapStack(final int initialCapacity, final float loadFactor)
    {
        this.hashMap = new HashMap<>(initialCapacity, loadFactor);
    }

    /**
     * Returns true if there if the key is stored in the map
     * @param key the key
     * @return true if stored
     */
    public boolean containsKey(final K key)
    {
        return this.hashMap.containsKey(key);
    }

    /**
     * Return the element that match the given key
     * @param key the key to match
     * @return the elment that match the key, null if none
     */
    public V get(final K key)
    {
        final StackElement element = this.hashMap.get(key);

        if (element != null)
        {
            return element.value;
        }

        return null;
    }

    /**
     * Put a new element in the map. It wont put it if already contained
     *
     * @param key the element key
     * @param value the element value
     *
     * @return true if added, false if it already exists
     */
    public boolean put(final K key, final V value)
    {
        // Check if it already exists first
        if (this.hashMap.containsKey(key))
        {
            return false;
        }

        // Add the new head element
        this.addNewHead(key, value);

        return true;
    }

    /**
     * Put a new element on the map, if already contained it will remove the old one first
     * @param key the key to remove and add
     * @param value the value to add
     */
    public void removeAndPut(final K key, final V value)
    {
        // First remove the key in case is already there
        this.remove(key);

        // Set the new head element
        this.addNewHead(key, value);
    }

    /**
     * Add a new value to the head of the map
     * @param key the key
     * @param value the value
     */
    private void addNewHead(final K key, final V value)
    {
        // Create the new element and add to the map
        final StackElement newElement = new StackElement(key, value);
        this.hashMap.put(key, newElement);

        // If there were no elements
        if (this.head == null)
        {
            this.head = newElement;
            this.tail = newElement;
        }
        else
        {
            final StackElement previoussHead = this.head;
            this.head = newElement;
            newElement.next = previoussHead;
            previoussHead.previous = newElement;
        }
    }

    /**
     * Remove the element with the given key, return the element if removed, null if not removed
     * @param key the key of the element to remove
     * @return the removed element, null if not found
     */
    public V remove(final K key)
    {
        // Remove from the map
        final StackElement removedElement = this.hashMap.remove(key);

        // If removed, fix the double linked list and update newest and eldest element
        if (removedElement != null)
        {
            this.removeStackElement(removedElement);
            return removedElement.value;
        }

        return null;
    }

    /**
     * Remove the given stack element from the double linked list
     * @param elementToRemove the element to remove
     */
    private void removeStackElement(final StackElement elementToRemove)
    {
        // Get previous and next elements in the linked list
        final StackElement previousElement = elementToRemove.previous;
        final StackElement nextElement = elementToRemove.next;

        if (previousElement == null)
        {
            this.head = nextElement;
        }
        else
        {
            previousElement.next = nextElement;
        }

        if (nextElement == null)
        {
            this.tail = previousElement;
        }
        else
        {
            nextElement.previous = previousElement;
        }
    }

    /**
     * @return the eldest key in the map, null if empty
     */
    public K getEldestKey()
    {
        if (this.tail == null)
        {
            return null;
        }

        return this.tail.key;
    }

    /**
     * @return the eldest value in the map, null if empty
     */
    public V getEldestValue()
    {
        if (this.tail == null)
        {
            return null;
        }

        return this.tail.value;
    }

    /**
     * @return the newest key in the map, null if empty
     */
    public K getNewestKey()
    {
        if (this.head == null)
        {
            return null;
        }

        return this.head.key;
    }

    /**
     * @return the newest value in the map, null if empty
     */
    public V getNewestValue()
    {
        if (this.head == null)
        {
            return null;
        }

        return this.head.value;
    }

    /**
     * @return true if the map is empty
     */
    public boolean isEmpty()
    {
        return this.hashMap.isEmpty();
    }

    /**
     * Clear the contents
     */
    public void clear()
    {
        this.hashMap.clear();
        this.tail = null;
        this.head = null;
    }

    /**
     * Consume all values in the set
     * @param consumer the consumer for the values
     */
    public void consumeAllValues(final Consumer<V> consumer)
    {
        StackElement currentElement = this.head;

        while (currentElement != null)
        {
            consumer.accept(currentElement.value);
            currentElement = currentElement.next;
        }
    }

    /**
     * Internal class that represent one element in the map. Each element is part of a double linked list
     */
    private class StackElement
    {
        /** Next element in the double linked list */
        private StackElement next;
        /** Previous element in the double linked list */
        private StackElement previous;
        /** Element key */
        private final K key;
        /** Element value */
        private final V value;

        /**
         * Create a new stack element
         *
         * @param key the element key
         * @param value the element value
         */
        StackElement(final K key, final V value)
        {
            this.key = key;
            this.value = value;
        }
    }
}
