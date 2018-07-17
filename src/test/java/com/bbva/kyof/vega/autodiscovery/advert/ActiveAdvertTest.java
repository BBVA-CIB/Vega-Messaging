package com.bbva.kyof.vega.autodiscovery.advert;

import com.bbva.kyof.vega.autodiscovery.advert.ActiveAdvert;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 04/08/16.
 */
public class ActiveAdvertTest
{
    @Test
    public void testCreateTimeoutUpdate() throws Exception
    {
        // Create active advert instance
        final ActiveAdvert<Integer> registeredInfo = new ActiveAdvert<>(128, 100);

        // Check getter
        Assert.assertEquals(registeredInfo.getAutoDiscInfo(), Integer.valueOf(128));

        // It should have not timed out yet
        Assert.assertFalse(registeredInfo.hasTimedOut());

        // Wait a bit
        Thread.sleep(200);
        Assert.assertTrue(registeredInfo.hasTimedOut());

        // Update
        registeredInfo.updateLastUpdateReceived();
        Assert.assertFalse(registeredInfo.hasTimedOut());

    }

    @Test
    public void equalsAndHashCode() throws Exception
    {
        // Create registered info instances
        final ActiveAdvert<Integer> advert1 = new ActiveAdvert<>(128, 100);
        final ActiveAdvert<Integer> advert2 = new ActiveAdvert<>(128, 90);
        final ActiveAdvert<Integer> advert3 = new ActiveAdvert<>(12, 80);

        Assert.assertEquals(advert1, advert1);
        Assert.assertEquals(advert1, advert2);
        Assert.assertNotEquals(advert1, advert3);
        Assert.assertNotEquals(advert1, null);
        Assert.assertNotEquals(advert1, new Object());

        Assert.assertTrue(advert1.hashCode() == advert2.hashCode());
        Assert.assertFalse(advert1.hashCode() == advert3.hashCode());
    }
}