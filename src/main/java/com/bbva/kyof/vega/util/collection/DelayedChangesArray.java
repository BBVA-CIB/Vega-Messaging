package com.bbva.kyof.vega.util.collection;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class represents a generic collection with very special features and limitations. Please read carefully before use!!!<p>
 *
 * - The internal elements are stored into a primitive genetic collection.<p>
 * - The changes in the collection (add or remove elements) is not performed immediately.<p>
 * - The changes are stored and only performed when the method "applyChanges() is called"<p>
 * - The idea of the collection is to been able to iterate over the internal collection directly<p>
 * - There is one limitation, the thread that iterates over the values has to be the same one
 * that apply the pending changes before or after the iteration. Only 1 thread should be allow to iterate!<p>
 * - You can have any number of threads adding or removing elements.
 * - The class is only synchronized to ensure that modifications cannot occur concurrently.<p>
 * - When new elements are added, the internal collection will grow if required when changes are applied.<p>
 * - Apply pending changes may cause an exception if an element is added or removed twice, the class won´t check if the
 * element is there when the addElement is called, only when changes are applied.
 *
 * @param <T> Type of the contents of the array
 */
public final class DelayedChangesArray<T> implements IDelayedChangesArray<T>
{
    /** Default initial size if no size is specified on construction */
    private static final int DEFAULT_INITIAL_SIZE = 100;

    /** Default growing factor if no factor is specified on construction */
    private static final float DEFAULT_GROW_FACTOR = 1.75f;

    /** List of pending changes to apply */
    private final List<PendingChange<T>> pendingChanges = new LinkedList<>();

    /** Map with all the stored elements in the collection given their position on it, it is used to avoid looking in
     * all the collection for deletions */
    private final Map<T, Integer> arrayPositionByElement;

    /** Final elements if all pending changes where applied already, it is used to avoid adding duplicated elements or
     * remove non existing element */
    private final Set<T> elementsAfterPendingChanges;

    /** Growing factor for the internal arrays, maps and sets */
    private final float growFactor;

    /** Lock for the modifications on the collection */
    private final Object modificationsLock = new Object();

    /** Class type of the generic, we need to store in order to grow up the collection */
    private final Class<T> classType;

    /** Internal collection with all the stored elements */
    private T[] internalArray;

    /** Number of elements currently in the collection */
    private int numElements = 0;

    /** Construct a new collection *
     * @param classType the class represented by this collection
     */
    public DelayedChangesArray(final Class<T> classType)
    {
        this(classType, DEFAULT_INITIAL_SIZE);
    }

    /**
     * Constructs a new collection given the initial size, it will preallocate the required memory for performance improvements
     * @param classType the class type represented by the collection
     * @param initialSize the initial size of the collection
     */
    public DelayedChangesArray(final Class<T> classType, final int initialSize)
    {
        this(classType, initialSize, DEFAULT_GROW_FACTOR);
    }

    /**
     * Constructs a new collection given the initial size.
     * It will preallocate the required memory for performance improvements.
     * The collection will grow using the grow factor: 1.5 means a 50% grow
     *
     * @param classType the class type represented by the collection
     * @param initialSize the initial size of the collection
     * @param growFactor the grow factor in case the collection needs to grow for size reasons
     */
    @SuppressWarnings("unchecked")
    public DelayedChangesArray(final Class<T> classType, final int initialSize, final float growFactor)
    {
        this.growFactor = growFactor;
        this.classType = classType;
        this.internalArray = (T[]) Array.newInstance(classType, initialSize);
        this.arrayPositionByElement = new HashMap<>(initialSize, growFactor);
        this.elementsAfterPendingChanges = new HashSet<>(initialSize);
    }

    @Override
    public T[] getInternalArray()
    {
        return this.internalArray;
    }

    @Override
    public int getNumElements()
    {
        return this.numElements;
    }

    @Override
    public boolean addElement(final T element)
    {
        synchronized (this.modificationsLock)
        {
            if (this.elementsAfterPendingChanges.contains(element))
            {
                return false;
            }

            // Add the element to the set to keep the status after applying changes
            this.elementsAfterPendingChanges.add(element);

            // Add to pending changes, it will apply them to the collection and the map later on
            this.pendingChanges.add(new PendingChange<T>(element, ChangeType.ADD));

            return true;
        }
    }

    @Override
    public boolean removeElement(final T element)
    {
        synchronized (this.modificationsLock)
        {
            if (!this.elementsAfterPendingChanges.remove(element))
            {
                return false;
            }

            // Add the action to pending changes, it will apply them to the collection and map later on
            this.pendingChanges.add(new PendingChange<>(element, ChangeType.REMOVE));

            return true;
        }
    }

    @Override
    public void clear()
    {
        synchronized (this.modificationsLock)
        {
            // Clear all internal contents
            this.pendingChanges.clear();
            this.arrayPositionByElement.clear();
            this.numElements = 0;

            // Remove all references in the collection
            Arrays.fill(this.internalArray, null);
        }
    }

    @Override
    public void applyPendingChanges()
    {
        synchronized (this.modificationsLock)
        {
            if (this.pendingChanges.isEmpty())
            {
                return;
            }

            for (final PendingChange<T> pendingChange : pendingChanges)
            {
                if (pendingChange.getChangeType() == ChangeType.ADD)
                {
                    this.addElementInternalArray(pendingChange.getElement());
                }
                else
                {
                    this.removeElementFromInternalArray(pendingChange.getElement());
                }
            }

            pendingChanges.clear();
        }
    }

    /**
     * Add a new element into the internal collection, the element is always added after the last element of the collection.
     * If there is no space left, the collection is grown using the grow factor.
     *
     * @param newElement new element to add into the collection
     */
    private void addElementInternalArray(final T newElement)
    {
        // Check if there is enough space
        if (this.numElements < this.internalArray.length)
        {
            // Add at the end of the internalArray
            this.internalArray[numElements] = newElement;
            // Add the new element to the map
            this.arrayPositionByElement.put(newElement, numElements);
            // Increase the number of element
            numElements++;
        }
        else
        {
            // Grow up the internalArray and try again
            this.growUpArray();
            this.addElementInternalArray(newElement);
        }
    }

    /**
     * Grow up the internal collection following the provided grow factor
     */
    private void growUpArray()
    {
        // Calculate the new size for the collection
        int newArraySize = (int) (this.internalArray.length * growFactor);

        // If the collection has not grown (it can happen with just 1 element, or very small grow factor
        // Increase the size in just one element)
        if (newArraySize == this.internalArray.length)
        {
            newArraySize++;
        }

        // Grow up the internalArray
        @SuppressWarnings("unchecked")
        final T[] newArray = (T[]) Array.newInstance(this.classType, newArraySize);

        // Copy the internalArray
        System.arraycopy(this.internalArray, 0, newArray, 0, this.internalArray.length);

        // Change the current internalArray for the new one
        this.internalArray = newArray;
    }

    /**
     * Removes an element from the internal collection. The internal hashmap is used to look for the element position in the collection.
     * The element is removed and the last element in the collection is moved to that position in order to fill the gap.
     *
     * @param element the element to remove
     */
    private void removeElementFromInternalArray(final T element)
    {
        // Find the element to remove and remove from the map
        final Integer elementToRemovePosition = this.arrayPositionByElement.remove(element);

        // Reduce the number of elements
        this.numElements--;

        // Move the last element to the empty position if there are still elements in the collection
        if (this.numElements > 0)
        {
            // Get the element to move, it will be the last element of the collection
            final T lastElement = this.internalArray[numElements];
            // Move the last element to the position of the element to remove
            this.internalArray[elementToRemovePosition] = lastElement;
            // Update the moved element position
            this.arrayPositionByElement.put(lastElement, elementToRemovePosition);
        }

        // Remove the reference to the last element of the collection
        this.internalArray[numElements] = null;
    }

    /**
     * Represents a pending change to apply to the collection
     *
     * @param <T> the type of the element in the collection
     */
    private static class PendingChange<T>
    {
        /**
         * The element that is pending action
         */
        private final T element;
        /**
         * The pending change, it can be a removal or an addition
         */
        private final ChangeType changeType;

        /**
         * Creates a new pending change object
         *
         * @param element    the element to add or remove
         * @param changeType the type of the change
         */
        protected PendingChange(final T element, final ChangeType changeType)
        {
            this.element = element;
            this.changeType = changeType;
        }

        /**
         * Get the change type
         */
        protected ChangeType getChangeType()
        {
            return changeType;
        }

        /**
         * Gets the element to add or remove
         */
        protected T getElement()
        {
            return element;
        }
    }

    /**
     * Enum that represent the type of change for a delayed change in the collection
     */
    private enum ChangeType
    {
        /**
         * Add an element in the collection
         */
        ADD,
        /**
         * Remove an element from the collection
         */
        REMOVE
    }
}
