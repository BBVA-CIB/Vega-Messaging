package com.bbva.kyof.vega.autodiscovery.publisher;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 09/08/16.
 */
public class VariableSendRegisteredInfoTest
{
    @Test
    public void shouldSend() throws Exception
    {
        // Create registered info instance
        final VariableSendRegisteredInfo<Integer> registeredInfo = new VariableSendRegisteredInfo<>(128, 100, 1000, 2);

        // Check getter
        Assert.assertEquals(registeredInfo.getInfo(), Integer.valueOf(128));

        // Wait a bit, NEXT TIME SHOULD BE IN 100 millis
        long startTime = System.currentTimeMillis();

        long sendTime = this.waitForSendTime(registeredInfo) - startTime;
        System.out.println(sendTime);
        Assert.assertTrue(sendTime >= 95 && sendTime <= 105);

        startTime = System.currentTimeMillis();
        sendTime = this.waitForSendTime(registeredInfo) - startTime;
        System.out.println(sendTime);
        Assert.assertTrue(sendTime >= 195 && sendTime <= 205);

        startTime = System.currentTimeMillis();
        sendTime = this.waitForSendTime(registeredInfo) - startTime;
        System.out.println(sendTime);
        Assert.assertTrue(sendTime >= 395 && sendTime <= 405);

        startTime = System.currentTimeMillis();
        sendTime = this.waitForSendTime(registeredInfo) - startTime;
        System.out.println(sendTime);
        Assert.assertTrue(sendTime >= 795 && sendTime <= 805);

        startTime = System.currentTimeMillis();
        sendTime = this.waitForSendTime(registeredInfo) - startTime;
        System.out.println(sendTime);
        Assert.assertTrue(sendTime >= 995 && sendTime <= 1005);

        startTime = System.currentTimeMillis();
        sendTime = this.waitForSendTime(registeredInfo) - startTime;
        System.out.println(sendTime);
        Assert.assertTrue(sendTime >= 995 && sendTime <= 1005);
    }

    private long waitForSendTime(final VariableSendRegisteredInfo<Integer> registeredInfo) throws Exception
    {
        while(registeredInfo.getIfShouldSendAndResetIfRequired(System.currentTimeMillis()) == null);

        return System.currentTimeMillis();
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