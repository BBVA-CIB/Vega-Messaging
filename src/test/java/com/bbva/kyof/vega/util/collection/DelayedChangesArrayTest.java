package com.bbva.kyof.vega.util.collection;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Class created to test {@link DelayedChangesArray}
 *
 * Created by XE52727 on 04/07/2016.
 */
public class DelayedChangesArrayTest
{
    /** First element of collection */
    private static final String FIRST_ELEMENT = "1";
    /** Second element of collection */
    private static final String SECOND_ELEMENT = "2";
    /** Third element of collection */
    private static final String THIRD_ELEMENT = "3";
    /** Instance of collection */
    private IDelayedChangesArray<String> delayedChangesArray;

    @Before
    public void setUp() throws Exception
    {
        // Create the collection
        this.delayedChangesArray = new DelayedChangesArray<>(String.class);

        // First apply changes if there are no changes
        this.delayedChangesArray.applyPendingChanges();
        Assert.assertEquals(this.delayedChangesArray.getNumElements(), 0);

        // Add some elements
        this.delayedChangesArray.addElement(FIRST_ELEMENT);
        this.delayedChangesArray.addElement(SECOND_ELEMENT);
        this.delayedChangesArray.addElement(THIRD_ELEMENT);
    }

    @After
    public void tearDown() throws Exception
    {
        // Clear instance
        this.delayedChangesArray.clear();
    }

    @Test
    public void testAddRemoveElements() throws Exception
    {
        // Until changes are applied there should be no elements
        Assert.assertEquals(this.delayedChangesArray.getNumElements(), 0);

        // Apply the changes
        this.delayedChangesArray.applyPendingChanges();

        // Make sure changes have been applied
        Assert.assertEquals(this.delayedChangesArray.getNumElements(), 3);
        Assert.assertEquals(this.delayedChangesArray.getInternalArray()[0], FIRST_ELEMENT);
        Assert.assertEquals(this.delayedChangesArray.getInternalArray()[1], SECOND_ELEMENT);
        Assert.assertEquals(this.delayedChangesArray.getInternalArray()[2], THIRD_ELEMENT);

        // Now test removal of elements
        this.delayedChangesArray.removeElement(SECOND_ELEMENT);

        // There should be no changes yet
        Assert.assertEquals(this.delayedChangesArray.getNumElements(), 3);
        Assert.assertEquals(this.delayedChangesArray.getInternalArray()[0], FIRST_ELEMENT);
        Assert.assertEquals(this.delayedChangesArray.getInternalArray()[1], SECOND_ELEMENT);
        Assert.assertEquals(this.delayedChangesArray.getInternalArray()[2], THIRD_ELEMENT);

        // Apply the change and check
        this.delayedChangesArray.applyPendingChanges();
        Assert.assertEquals(delayedChangesArray.getNumElements(), 2);
        Assert.assertEquals(delayedChangesArray.getInternalArray()[0], FIRST_ELEMENT);
        Assert.assertEquals(delayedChangesArray.getInternalArray()[1], THIRD_ELEMENT);

        // Finally remove the rest
        this.delayedChangesArray.removeElement(FIRST_ELEMENT);
        this.delayedChangesArray.removeElement(THIRD_ELEMENT);

        // There should be no changes yet
        Assert.assertEquals(this.delayedChangesArray.getNumElements(), 2);
        Assert.assertEquals(this.delayedChangesArray.getInternalArray()[0], FIRST_ELEMENT);
        Assert.assertEquals(this.delayedChangesArray.getInternalArray()[1], THIRD_ELEMENT);

        // Apply the change and check
        this.delayedChangesArray.applyPendingChanges();
        Assert.assertEquals(this.delayedChangesArray.getNumElements(), 0);
    }

    @Test
    public void testAddRemoveTwice() throws Exception
    {
        // Apply the changes
        this.delayedChangesArray.applyPendingChanges();

        // Add a repeated element and check
        final boolean repeatedAddition = this.delayedChangesArray.addElement(FIRST_ELEMENT);
        Assert.assertFalse(repeatedAddition);

        // Remove non existing element and check
        final boolean nonExistingRemoval =this.delayedChangesArray.removeElement("other");
        Assert.assertFalse(nonExistingRemoval);
    }

    @Test
    public void testClearList() throws Exception
    {
        // Apply the changes
        this.delayedChangesArray.applyPendingChanges();

        // Clear and check
        this.delayedChangesArray.clear();
        Assert.assertEquals(this.delayedChangesArray.getNumElements(), 0);
    }

    @Test
    public void testForceGrowContentsWith1Element() throws Exception
    {
        // Initial size 1, DEFAULT_GROW_FACTOR = 1.75f , adding 1 element should end with 2 elements in the internal array
        IDelayedChangesArray<Integer> delayedChangesArray = new DelayedChangesArray<>(Integer.class, 1);

        // Add some elements
        delayedChangesArray.addElement(1);
        delayedChangesArray.addElement(2);

        // Apply and check
        delayedChangesArray.applyPendingChanges();
        Assert.assertEquals(delayedChangesArray.getInternalArray().length, 2);
    }

    @Test
    public void testForceGrowContents() throws Exception
    {
        // Initial size 10, grow factor 2, adding 30 elements should end with 40 elements in the internal array
        IDelayedChangesArray<Integer> delayedChangesArray = new DelayedChangesArray<>(Integer.class, 10, 2);

        for (int i = 0; i < 30; i++)
        {
            delayedChangesArray.addElement(i);
        }

        // No grow until changes are applied
        Assert.assertEquals(delayedChangesArray.getInternalArray().length, 10);

        // Apply and check
        delayedChangesArray.applyPendingChanges();
        Assert.assertEquals(delayedChangesArray.getInternalArray().length, 40);
        Assert.assertEquals(delayedChangesArray.getNumElements(), 30);
    }
}