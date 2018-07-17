package com.bbva.kyof.vega.util.collection;

/**
 * Interface for an collection that allows to handle changes in the collection in "delayed mode".
 * <p>
 * Changes on the collection will only be applied when "applyPendingChanges" is called.
 *
 * It should be called by the same thread that will process the internal collection.
 *
 * @param <T> The type of the array stored elements
 */
public interface IDelayedChangesArray<T>
{
    /**
     * @return the internal collection representation. It can be used for iteration considering the "numElements" currently in the collection.
     * <p>
     * The only limitation is that "applyPendingChanges" should never be done at the same time than the iteration! Ideally we should apply
     * the changes, and then iterate over the collection.
     */
    T[] getInternalArray();

    /** @return the number of elements that are currently in the internal collection */
    int getNumElements();

    /**
     * Add a new element to the collection, the addition will be delayed until changes are explicitly applied.
     * <p>
     * Make sure that the element is not in the collection yet or it will cause a problem when applying changes
     *
     * @param element element to add into the collection
     * @return true if the element has been added, false if it already exists
     */
    boolean addElement(final T element);

    /**
     * Remove an element from the collection, the addition will be delayed until changes are explicitly applied
     * <p>
     * Make sure that the element is in the collection or it will cause an exception
     *
     * @param element element to remove from the collection
     * @return true if the element has been removed, false didn't exists
     */
    boolean removeElement(final T element);

    /**
     * Apply the pending changes into the internal Array. Never perform this while the collection is being iterated!
     */
    void applyPendingChanges();

    /**
     * Clear all the internal information
     */
    void clear();
}
