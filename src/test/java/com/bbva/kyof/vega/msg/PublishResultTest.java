package com.bbva.kyof.vega.msg;

import com.bbva.kyof.vega.msg.PublishResult;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cnebrera on 11/08/16.
 */
public class PublishResultTest
{
    @Test
    public void testValues()
    {
        Assert.assertTrue(PublishResult.fromAeronResult(456) == PublishResult.OK);
        Assert.assertTrue(PublishResult.fromAeronResult(0) == PublishResult.OK);
        Assert.assertTrue(PublishResult.fromAeronResult(-2) == PublishResult.BACK_PRESSURED);
        Assert.assertTrue(PublishResult.fromAeronResult(-3) == PublishResult.OK);
        Assert.assertTrue(PublishResult.fromAeronResult(-1) == PublishResult.OK);
    }
}