package com.bbva.kyof.vega.autodiscovery.publisher;

import org.junit.Assert;
import org.junit.Test;

/**
 * Class created to test {@link RegisteredInfo}
 * Created by XE52727 on 07/07/2016.
 */
public class RegisteredInfoTest
{
    @Test
    public void shouldSend() throws Exception
    {
        // Create registered info instance
        final RegisteredInfo<Integer> registeredInfo = new RegisteredInfo<>(128, 100);

        // Check getter
        Assert.assertEquals(registeredInfo.getInfo(), Integer.valueOf(128));

        // Should send should be false
        Assert.assertFalse(registeredInfo.checkIfshouldSendAndResetIfRequired(System.currentTimeMillis()));

        // Get if should send should be null
        Assert.assertNull(registeredInfo.getIfShouldSendAndResetIfRequired(System.currentTimeMillis()));

        // Wait a bit
        Thread.sleep(200);

        // Now should send should be true and false next time since the send time is reseted
        Assert.assertTrue(registeredInfo.checkIfshouldSendAndResetIfRequired(System.currentTimeMillis()));
        Assert.assertFalse(registeredInfo.checkIfshouldSendAndResetIfRequired(System.currentTimeMillis()));

        // Wait a bit
        Thread.sleep(200);

        // Get if should send should not be null, the second time should be null since the time has been reset
        Assert.assertNotNull(registeredInfo.getIfShouldSendAndResetIfRequired(System.currentTimeMillis()));
        Assert.assertNull(registeredInfo.getIfShouldSendAndResetIfRequired(System.currentTimeMillis()));

        // Now reset the next expected send
        registeredInfo.resetNextExpectedSent(System.currentTimeMillis());

        // Should send now should be false again
        Assert.assertFalse(registeredInfo.checkIfshouldSendAndResetIfRequired(System.currentTimeMillis()));
    }

    @Test
    public void equalsAndHashCode() throws Exception
    {
        // Create registered info instances
        final RegisteredInfo<Integer> registeredInfo1 = new RegisteredInfo<>(128, 100);
        final RegisteredInfo<Integer> registeredInfo2 = new RegisteredInfo<>(128, 90);
        final RegisteredInfo<Integer> registeredInfo3 = new RegisteredInfo<>(12, 80);

        Assert.assertEquals(registeredInfo1, registeredInfo1);
        Assert.assertEquals(registeredInfo1, registeredInfo2);
        Assert.assertNotEquals(registeredInfo1, registeredInfo3);
        Assert.assertNotEquals(registeredInfo1, null);
        Assert.assertNotEquals(registeredInfo1, new Object());

        Assert.assertTrue(registeredInfo1.hashCode() == registeredInfo2.hashCode());
        Assert.assertFalse(registeredInfo1.hashCode() == registeredInfo3.hashCode());
    }
}