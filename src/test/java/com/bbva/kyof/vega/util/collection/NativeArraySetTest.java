package com.bbva.kyof.vega.util.collection;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by cnebrera on 29/07/16.
 */
public class NativeArraySetTest
{
    @Test
    public void testConstructors() throws Exception
    {
        new NativeArraySet<>(Integer.class, 2, 2);
        new NativeArraySet<>(Integer.class, 2);
    }

    @Test
    public void testAddRemoveContains() throws Exception
    {
        final NativeArraySet<Integer> arraySet = new NativeArraySet<>(Integer.class, 2, 2);

        // The internal array should have size 2
        Assert.assertTrue(arraySet.getInternalArray().length == 2);
        Assert.assertTrue(arraySet.isEmpty());
        Assert.assertTrue(arraySet.getNumElements() == 0);

        // Add one, internal size should be the same
        arraySet.addElement(1);

        Assert.assertTrue(arraySet.getInternalArray().length == 2);
        Assert.assertFalse(arraySet.isEmpty());
        Assert.assertTrue(arraySet.getNumElements() == 1);
        Assert.assertTrue(arraySet.contains(1));

        // Add two more elements, the size now should be 4 since the internal array should have grown
        arraySet.addElement(2);
        arraySet.addElement(3);

        Assert.assertTrue(arraySet.getInternalArray().length == 4);
        Assert.assertFalse(arraySet.isEmpty());
        Assert.assertTrue(arraySet.getNumElements() == 3);
        Assert.assertTrue(arraySet.contains(2));
        Assert.assertTrue(arraySet.contains(3));

        // Add more elements
        arraySet.addElement(4);
        arraySet.addElement(5);
        arraySet.addElement(6);
        Assert.assertTrue(arraySet.getNumElements() == 6);

        // Now remove the first element and check
        arraySet.removeElement(1);
        Assert.assertFalse(arraySet.contains(1));
        Assert.assertTrue(arraySet.contains(2));
        Assert.assertTrue(arraySet.getInternalArray()[1] == 2);
        Assert.assertTrue(arraySet.contains(3));
        Assert.assertTrue(arraySet.getInternalArray()[2] == 3);
        Assert.assertTrue(arraySet.contains(6));
        Assert.assertTrue(arraySet.getInternalArray()[0] == 6);

        Assert.assertTrue(arraySet.getNumElements() == 5);

        // Remove the last element
        // Before deleting the element 1, the element 6 was the last.
        // After deleting the element 1, the element 6 is the first element (it was moved)
        // and now, the last element is the 5
        arraySet.removeElement(5);
        Assert.assertFalse(arraySet.contains(5));
        Assert.assertTrue(arraySet.contains(2));
        Assert.assertTrue(arraySet.getInternalArray()[1] == 2);
        Assert.assertTrue(arraySet.contains(3));
        Assert.assertTrue(arraySet.getInternalArray()[2] == 3);
        Assert.assertTrue(arraySet.contains(4));
        Assert.assertTrue(arraySet.getInternalArray()[3] == 4);
        Assert.assertTrue(arraySet.contains(6));
        Assert.assertTrue(arraySet.getInternalArray()[0] == 6);
        Assert.assertTrue(arraySet.getNumElements() == 4);

        // Remove an element in between
        arraySet.removeElement(3);
        Assert.assertFalse(arraySet.contains(3));
        Assert.assertTrue(arraySet.contains(2));
        Assert.assertTrue(arraySet.getInternalArray()[1] == 2);
        Assert.assertTrue(arraySet.contains(4));
        Assert.assertTrue(arraySet.getInternalArray()[2] == 4);
        Assert.assertTrue(arraySet.contains(6));
        Assert.assertTrue(arraySet.getInternalArray()[0] == 6);
        Assert.assertTrue(arraySet.getNumElements() == 3);

        // 3 elements have been removed, only 3 not null elements should remain
        Assert.assertTrue(this.getNumberOfNotNullElements(arraySet) == 3);

        // Finally remove the rest of the elements
        arraySet.removeElement(2);
        arraySet.removeElement(4);
        arraySet.removeElement(6);

        Assert.assertTrue(arraySet.isEmpty());
        Assert.assertTrue(arraySet.getNumElements() == 0);
        Assert.assertTrue(this.getNumberOfNotNullElements(arraySet) == 0);
    }

    @Test
    public void testAddRemoveTwice() throws Exception
    {
        final NativeArraySet<Integer> arraySet = new NativeArraySet<>(Integer.class, 2, 2);

        Assert.assertTrue(arraySet.addElement(1));
        Assert.assertTrue(arraySet.addElement(2));
        Assert.assertFalse(arraySet.addElement(1));

        Assert.assertTrue(arraySet.removeElement(1));
        Assert.assertFalse(arraySet.removeElement(1));
    }

    @Test
    public void testConsumeAll() throws Exception
    {
        final NativeArraySet<Integer> arraySet = new NativeArraySet<>(Integer.class, 2, 2);
        arraySet.addElement(1);
        arraySet.addElement(2);
        arraySet.addElement(3);

        final AtomicInteger sum = new AtomicInteger(0);
        arraySet.consumeAll(sum::addAndGet);

        Assert.assertTrue(sum.get() == 6);
    }

    @Test
    public void testClear() throws Exception
    {
        final NativeArraySet<Integer> arraySet = new NativeArraySet<>(Integer.class, 2, 2);
        arraySet.addElement(1);
        arraySet.addElement(2);
        arraySet.addElement(3);

        arraySet.clear();
        Assert.assertTrue(this.getNumberOfNotNullElements(arraySet) == 0);
        Assert.assertTrue(arraySet.isEmpty());
        Assert.assertTrue(arraySet.getNumElements() == 0);

        final AtomicInteger sum = new AtomicInteger(0);
        arraySet.consumeAll(sum::addAndGet);

        Assert.assertTrue(sum.get() == 0);
    }

    private int getNumberOfNotNullElements(final NativeArraySet<Integer> arraySet)
    {
        final AtomicInteger numNotNullElements = new AtomicInteger(0);
        Arrays.asList(arraySet.getInternalArray()).forEach((element) ->
        {
            if (element != null)
            {
                numNotNullElements.incrementAndGet();
            }
        });

        return numNotNullElements.get();
    }
}