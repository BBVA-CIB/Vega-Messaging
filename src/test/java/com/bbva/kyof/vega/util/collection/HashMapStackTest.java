package com.bbva.kyof.vega.util.collection;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class create to test {@link HashMapStack}
 *
 * Created by XE52727 on 13/07/2016.
 */
public class HashMapStackTest
{
    /** Instance of collection */
    private HashMapStack<Integer, String> hashMapStack;

    @Before
    public void setUp()
    {
        // Create the collection
        this.hashMapStack = new HashMapStack<>();

        // Add some elements
        this.hashMapStack.put(1, "1");
        this.hashMapStack.put(2, "2");
        this.hashMapStack.put(3, "3");
        this.hashMapStack.put(4, "4");
    }

    @After
    public void tearDown()
    {
        // Clear instance
        this.hashMapStack.clear();
    }

    @Test
    public void tesConstructors()
    {
        new HashMapStack<>();
        new HashMapStack<>(5);
        new HashMapStack<>(5, 2);
    }

    @Test
    public void testGet()
    {
        Assert.assertEquals(this.hashMapStack.get(1), "1");
        Assert.assertEquals(this.hashMapStack.get(4), "4");
        Assert.assertNull(this.hashMapStack.remove(5));
        Assert.assertNull(this.hashMapStack.get(5));
    }

    @Test
    public void testPutAndRemoveTwice()
    {
        Assert.assertFalse(this.hashMapStack.put(1, "1"));
        Assert.assertTrue(this.hashMapStack.put(5, "5"));
        Assert.assertEquals(this.hashMapStack.remove(5), "5");
        Assert.assertNull(this.hashMapStack.remove(5));
    }

    @Test
    public void testGetEldestNewestValue()
    {
        // Check eldest entry
        Assert.assertEquals("1", this.hashMapStack.getEldestValue());

        // Check newest entry
        Assert.assertEquals("4", this.hashMapStack.getNewestValue());
    }

    @Test
    public void testSingleElement()
    {
        final HashMapStack<Integer, String> map = new HashMapStack<>();

        // Add a single element
        map.put(1, "1");
        Assert.assertEquals("1", map.getEldestValue());
        Assert.assertEquals("1", map.getNewestValue());
        Assert.assertTrue(1 == map.getEldestKey());
        Assert.assertTrue(1 == map.getNewestKey());

        map.put(2, "2");

        // Remove the all element
        map.remove(2);
        map.remove(1);
        Assert.assertNull(map.getEldestValue());
        Assert.assertNull(map.getNewestValue());
        Assert.assertNull(map.getEldestKey());
        Assert.assertNull(map.getNewestKey());
        Assert.assertTrue(map.isEmpty());
    }

    @Test
    public void testRemoveAddAndEldestNewest()
    {
        // Remove the oldest element
        this.hashMapStack.remove(1);

        // The oldest element now should be 2 and newest 4
        Assert.assertEquals("2", this.hashMapStack.getEldestValue());
        Assert.assertEquals("4", this.hashMapStack.getNewestValue());
        Assert.assertTrue(2 == this.hashMapStack.getEldestKey());
        Assert.assertTrue(4 == this.hashMapStack.getNewestKey());

        // Now remove the newest element
        this.hashMapStack.remove(4);

        // The oldest element now should be 2 and newest 3
        Assert.assertEquals("2", this.hashMapStack.getEldestValue());
        Assert.assertEquals("3", this.hashMapStack.getNewestValue());
        Assert.assertTrue(2 == this.hashMapStack.getEldestKey());
        Assert.assertTrue(3 == this.hashMapStack.getNewestKey());

        // Reset
        this.tearDown();
        this.setUp();

        // Now remove an element in between
        this.hashMapStack.remove(3);

        // The oldest element now should be 1 and newest 4
        Assert.assertEquals("1", this.hashMapStack.getEldestValue());
        Assert.assertEquals("4", this.hashMapStack.getNewestValue());
        Assert.assertTrue(1 == this.hashMapStack.getEldestKey());
        Assert.assertTrue(4 == this.hashMapStack.getNewestKey());

        // Now add a new element and check that is the newest
        this.hashMapStack.put(5, "5");
        Assert.assertEquals("1", this.hashMapStack.getEldestValue());
        Assert.assertEquals("5", this.hashMapStack.getNewestValue());
        Assert.assertTrue(1 == this.hashMapStack.getEldestKey());
        Assert.assertTrue(5 == this.hashMapStack.getNewestKey());
    }

    @Test
    public void testRemoveAndPut()
    {
        // Remove an element, should have been moved to be the newest
        this.hashMapStack.removeAndPut(3, "3");

        // The oldest element now should be 2 and newest 4
        Assert.assertEquals("1", this.hashMapStack.getEldestValue());
        Assert.assertEquals("3", this.hashMapStack.getNewestValue());
        Assert.assertTrue(1 == this.hashMapStack.getEldestKey());
        Assert.assertTrue(3 == this.hashMapStack.getNewestKey());
        Assert.assertTrue(this.hashMapStack.containsKey(3));
    }

    @Test
    public void testConsumeAll()
    {
        final AtomicInteger sum = new AtomicInteger(0);
        this.hashMapStack.consumeAllValues((value) -> sum.addAndGet(Integer.valueOf(value)));
        Assert.assertEquals(sum.get(), 10);
    }

    @Test
    public void testClearMap()
    {
        // Clear instance
        this.hashMapStack.clear();

        // Check instance is empty
        Assert.assertTrue(this.hashMapStack.isEmpty());
    }

}