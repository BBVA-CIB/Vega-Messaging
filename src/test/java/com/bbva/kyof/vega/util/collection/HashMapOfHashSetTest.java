package com.bbva.kyof.vega.util.collection;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class created to test {@link HashMapOfHashSet}
 *
 * Created by XE52727 on 12/07/2016.
 */
public class HashMapOfHashSetTest
{
    /** Instance of collection */
    private HashMapOfHashSet<Integer, String> hashMapOfHashSet = new HashMapOfHashSet<>();

    @Before
    public void setUp()
    {
        this.hashMapOfHashSet.put(1, "1");
        this.hashMapOfHashSet.put(1, "2");
        this.hashMapOfHashSet.put(2, "3");
        this.hashMapOfHashSet.put(2, "4");
    }

    @After
    public void tearDown()
    {
        // Clear instance
        this.hashMapOfHashSet.clear();
    }

    @Test
    public void testPutGetContains()
    {
        // Check the keys
        Assert.assertTrue(this.hashMapOfHashSet.containsKey(1));
        Assert.assertTrue(this.hashMapOfHashSet.containsKey(2));
        Assert.assertFalse(this.hashMapOfHashSet.containsKey(3));

        // Check the values
        Assert.assertTrue(this.hashMapOfHashSet.containsValue(1, "1"));
        Assert.assertTrue(this.hashMapOfHashSet.containsValue(2, "4"));
        Assert.assertFalse(this.hashMapOfHashSet.containsValue(1, "3"));
        Assert.assertFalse(this.hashMapOfHashSet.containsValue(2, "1"));

        // Try to put twice
        Assert.assertFalse(this.hashMapOfHashSet.put(1, "1"));
        Assert.assertTrue(this.hashMapOfHashSet.put(1, "10"));
    }

    @Test
    public void testRemovals()
    {
        // Remove one element from the set for key 1
        Assert.assertTrue(this.hashMapOfHashSet.remove(1, "1"));
        Assert.assertTrue(this.hashMapOfHashSet.containsValue(1, "2"));
        Assert.assertFalse(this.hashMapOfHashSet.containsValue(1, "1"));
        Assert.assertTrue(this.hashMapOfHashSet.containsKey(1));

        // Remove the other element
        Assert.assertTrue(this.hashMapOfHashSet.remove(1, "2"));
        Assert.assertFalse(this.hashMapOfHashSet.containsValue(1, "2"));
        Assert.assertFalse(this.hashMapOfHashSet.containsValue(1, "1"));
        Assert.assertFalse(this.hashMapOfHashSet.containsKey(1));

        // Check the values of the other key
        Assert.assertTrue(this.hashMapOfHashSet.containsKey(2));
        Assert.assertTrue(this.hashMapOfHashSet.containsValue(2, "3"));
        Assert.assertTrue(this.hashMapOfHashSet.containsValue(2, "4"));

        // Try to remove twice when there is no internal set
        Assert.assertFalse(this.hashMapOfHashSet.remove(1, "1"));

        // Try to remove twice when there is internal set
        Assert.assertTrue(this.hashMapOfHashSet.remove(2, "3"));
        Assert.assertFalse(this.hashMapOfHashSet.remove(2, "3"));

        // Add and use remove key
        this.hashMapOfHashSet.put(1, "1");
        Assert.assertTrue(this.hashMapOfHashSet.removeKey(1));
        Assert.assertFalse(this.hashMapOfHashSet.removeKey(1));
    }

    @Test
    public void testClearList()
    {
        // Clear all the internal information
        this.hashMapOfHashSet.clear();

        // Make sure changes have been applied
        Assert.assertFalse(this.hashMapOfHashSet.containsKey(1));
        Assert.assertFalse(this.hashMapOfHashSet.containsKey(2));
    }

    @Test
    public void testConsumeIfKeyEquals()
    {
        AtomicInteger sum = new AtomicInteger(0);
        this.hashMapOfHashSet.consumeIfKeyEquals(1, (value) -> sum.addAndGet(Integer.valueOf(value)));
        Assert.assertEquals(sum.get(), 3);

        sum.set(0);
        this.hashMapOfHashSet.consumeIfKeyEquals(2, (value) -> sum.addAndGet(Integer.valueOf(value)));
        Assert.assertEquals(sum.get(), 7);
    }

    @Test
    public void testRemoveAndConsumeIfKeyEquals()
    {
        AtomicInteger sum = new AtomicInteger(0);
        this.hashMapOfHashSet.removeAndConsumeIfKeyEquals(1, (value) -> sum.addAndGet(Integer.valueOf(value)));
        Assert.assertEquals(sum.get(), 3);
        Assert.assertFalse(this.hashMapOfHashSet.containsValue(1, "2"));
        Assert.assertFalse(this.hashMapOfHashSet.containsValue(1, "1"));
        Assert.assertFalse(this.hashMapOfHashSet.containsKey(1));

        sum.set(0);
        this.hashMapOfHashSet.removeAndConsumeIfKeyEquals(2, (value) -> sum.addAndGet(Integer.valueOf(value)));
        Assert.assertEquals(sum.get(), 7);
        Assert.assertFalse(this.hashMapOfHashSet.containsValue(2, "3"));
        Assert.assertFalse(this.hashMapOfHashSet.containsValue(2, "4"));
        Assert.assertFalse(this.hashMapOfHashSet.containsKey(2));

        sum.set(0);
        this.hashMapOfHashSet.removeAndConsumeIfKeyEquals(2, (value) -> sum.addAndGet(Integer.valueOf(value)));
        Assert.assertEquals(sum.get(), 0);
        Assert.assertFalse(this.hashMapOfHashSet.containsKey(2));
    }

    @Test
    public void testConsumeIfKeyMatchFilter()
    {
        AtomicInteger sum = new AtomicInteger(0);
        this.hashMapOfHashSet.consumeIfKeyMatchFilter((key) -> key % 2 == 0, (value) -> sum.addAndGet(Integer.valueOf(value)));
        Assert.assertEquals(sum.get(), 7);
    }

    @Test
    public void anyValueForKeyMatchFilter()
    {
        Assert.assertTrue(this.hashMapOfHashSet.anyValueForKeyMatchFilter(1, (value) -> value.equals("1")));
        Assert.assertTrue(this.hashMapOfHashSet.anyValueForKeyMatchFilter(1, (value) -> value.equals("1")));
        Assert.assertTrue(this.hashMapOfHashSet.anyValueForKeyMatchFilter(1, (value) -> value.equals("2")));
        Assert.assertFalse(this.hashMapOfHashSet.anyValueForKeyMatchFilter(1, (value) -> value.equals("4")));
        Assert.assertFalse(this.hashMapOfHashSet.anyValueForKeyMatchFilter(24, (value) -> value.equals("1")));
    }
}