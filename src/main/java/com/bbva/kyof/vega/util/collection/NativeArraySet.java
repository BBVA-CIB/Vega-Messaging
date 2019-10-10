package com.bbva.kyof.vega.util.collection;

import lombok.Getter;
import org.agrona.collections.ArrayUtil;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;

/**
 * Growable collection implements a native collection in which elements can be added and removed without having to worry about
 * the collection size. In this implementation there is direct access to the internal native collection.
 *
 * Removal don't keep the order of the elements!
 *
 * It works as a set by checking for element existence before additions.
 *
 * Not thread safe!!
 *
 * @param <V> Type of the values of hte array
 */
public class NativeArraySet<V>
{
    /** Default grow factor of the internal collection */
    private static final int DEFAULT_GROW_FACTOR = 2;

    /** Grow factor for the internal collection */
    private final int growFactor;
    /** Stores the position of each element by element key */
    private final Map<V, Integer> elementPositions;
    /** The internal collection */
    @Getter private V[] internalArray;
    /** Number of elements in the array */
    @Getter private int numElements = 0;

    /**
     * Creates a new collection given the initial size
     *
     * @param valueClass class type of the values of the map, correspond also with the internal collection type
     * @param initialSize initial collection size
     */
    public NativeArraySet(final Class<V> valueClass, final int initialSize)
    {
        this(valueClass, initialSize, DEFAULT_GROW_FACTOR);
    }

    /**
     * Creates a new collection given the initial size and grow factor
     *
     * @param valueClass class type of the values of the map, correspond also with the internal collection type
     * @param initialSize initial size for the internal collection
     * @param growFactor grow factor for the internal collection
     */
    public NativeArraySet(final Class<V> valueClass, final int initialSize, final int growFactor)
    {
        this.elementPositions = new HashMap<>(initialSize);
        this.internalArray = (V[]) Array.newInstance(valueClass, initialSize);
        this.growFactor = growFactor;
    }

    /**
     * Run the given consumer for all elements in the array
     * @param consumer the consumer to execute
     */
    public void consumeAll(final Consumer<V> consumer)
    {
        for(int i = 0; i < this.numElements; i++)
        {
            consumer.accept(this.internalArray[i]);
        }
    }

    /**
     * Return true if the given element is contained in the array
     * @param value the element value to check
     * @return true if contained
     */
    public boolean contains(final V value)
    {
        return this.elementPositions.containsKey(value);
    }

    /**
     * Adds a new element into the collection, it will grow the collection if required.
     *
     * It won't add the element if it already exists (check by hash)
     *
     * @param value element to add
     * @return true if added, false if already exists
     */
    public boolean addElement(final V value)
    {
        // Check that is not already there
        if (this.elementPositions.containsKey(value))
        {
            return false;
        }

        // Check if we should grow up the collection
        if (this.numElements == internalArray.length)
        {
            this.internalArray = ArrayUtil.ensureCapacity(this.internalArray, internalArray.length * this.growFactor);
        }

        // Add the element in the last collection position
        this.internalArray[numElements] = value;

        // Store the position of the added element
        this.elementPositions.put(value, numElements);

        // Increase the number of elements
        this.numElements++;

        return true;
    }

    /**
     * Remove an element in the collection. The last element in the internal array will occupy the position of the removed element.
     *
     * @param value the element to remove
     * @return true if removed, false in other case
     */
    public boolean removeElement(final V value)
    {
        // Get the position of the element
        final Integer removedElementPos = this.elementPositions.remove(value);
        if (removedElementPos == null)
        {
            return false;
        }

        // Reduce the number of elements
        this.numElements--;

        // Move the last element to the empty position if there are still elements in the collection
        if (this.numElements > 0)
        {
            // Get the element to move, it will be the last element of the collection
            final V lastElement = this.internalArray[numElements];

            //If the element that will be removed is not the last one, it is necessary an adjustment
            if(!lastElement.equals(this.internalArray[removedElementPos]))
            {
                // Move the last element to the position of the element to remove
                this.internalArray[removedElementPos] = lastElement;
                // Update the moved element position
                this.elementPositions.put(lastElement, removedElementPos);
            }
        }

        // Remove the reference to the last element of the collection
        this.internalArray[this.numElements] = null;

        return true;
    }

    /** @return True if the collection is empty */
    public boolean isEmpty()
    {
        return this.numElements == 0;
    }

    /**
     * Clear internal contents
     */
    public void clear()
    {
        for (int i = 0; i < this.internalArray.length; i++)
        {
            this.internalArray[i] = null;
        }

        this.elementPositions.clear();
        this.numElements = 0;
    }

    /**
     * Returns a random element from internalArray
     * @return the aleatory V element
     */
    public V getRandomElement()
    {
        if(numElements > 0)
        {
            return this.internalArray[new Random().nextInt(numElements)];
        }
        //If the array is empty, return null
        return null;
    }
}
