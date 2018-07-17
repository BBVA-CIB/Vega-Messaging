/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bbva.kyof.vega.msg.lost;
import java.util.Random;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by dtm on 17/05/2018
 */
public class MsgLostReportTest
{
    @Test
    public void tryGettersAndSetters()
    {
        // Create the lost report values
        final UUID instanceId = UUID.randomUUID();
        final String topicName = "TopicName";
        final long numberLostMessages = new Random().nextLong();
        final UUID topicPublisherId = UUID.randomUUID();

        final MsgLostReport msgLostReport = new MsgLostReport(
                instanceId,
                topicName,
                numberLostMessages,
                topicPublisherId
        );
              
        // Get
         Assert.assertEquals(msgLostReport.getInstanceId(), instanceId);
         Assert.assertEquals(msgLostReport.getNumberLostMessages(), numberLostMessages);
         Assert.assertEquals(msgLostReport.getTopicName(), "TopicName");
         Assert.assertEquals(msgLostReport.getTopicPublisherId(), topicPublisherId);
    }
}
