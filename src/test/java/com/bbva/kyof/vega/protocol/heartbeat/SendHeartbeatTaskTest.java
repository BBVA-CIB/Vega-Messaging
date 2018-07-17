package com.bbva.kyof.vega.protocol.heartbeat;

import com.bbva.kyof.vega.msg.IResponseListener;
import com.bbva.kyof.vega.msg.RcvResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.UUID;

/**
 * Created by cnebrera on 05/10/2016.
 */
public class SendHeartbeatTaskTest
{
    private Timer timer;
    final UUID app1Id = UUID.randomUUID();
    final RcvResponse responseApp1 = new RcvResponse();
    final UUID app2Id = UUID.randomUUID();
    final RcvResponse responseApp2 = new RcvResponse();

    @Before
    public void before()
    {
        this.timer = new Timer("TestTimer");
        responseApp1.setInstanceId(app1Id);
        responseApp2.setInstanceId(app2Id);
    }

    @After
    public void after()
    {
        this.timer.cancel();
        this.timer.purge();
    }

    @Test
    public void testHeartbeatController() throws Exception
    {
        final Sender sender = new Sender();
        final ClientConnectionListener listener = new ClientConnectionListener();
        final SendHeartbeatTask task = new SendHeartbeatTask("topic", HeartbeatParameters.builder().build(), sender, listener);

        this.timer.scheduleAtFixedRate(task, 0, 200);

        // Wait a bit
        Thread.sleep(1000);

        // It should have discovered the client 1
        Assert.assertTrue(listener.clients.contains(app1Id));

        // Change behaviour to force responses only from app 2
        sender.behaviour = Behaviour.APP2;

        // Wait a bit
        Thread.sleep(1000);

        // It should have discovered the client 2 and loss client 1
        Assert.assertFalse(listener.clients.contains(app1Id));
        Assert.assertTrue(listener.clients.contains(app2Id));

        // Finally stop responding, should also loss client 2
        sender.behaviour = Behaviour.NONE;
        Thread.sleep(1000);

        Assert.assertFalse(listener.clients.contains(app1Id));
        Assert.assertFalse(listener.clients.contains(app2Id));

        // Just for coverage
        task.onRequestTimeout(null);
    }

    enum Behaviour
    {
        APP1,
        APP2,
        NONE
    }

    private class Sender implements IHeartbeatSender
    {
        public volatile Behaviour behaviour = Behaviour.APP1;

        @Override
        public void sendHeartbeat(IResponseListener responseListener, long timeout)
        {
            // Simulate a response
            if (behaviour == Behaviour.APP1)
            {
                responseListener.onResponseReceived(null, responseApp1);
            }
            else if (behaviour == Behaviour.APP2)
            {
                responseListener.onResponseReceived(null, responseApp2);
            }
            else
            {
                // Do nothing
            }
        }
    }

    private class ClientConnectionListener implements IClientConnectionListener
    {
        final Set<UUID> clients = new HashSet<>();

        @Override
        public void onClientConnected(String topicName, UUID clientInstanceId)
        {
            clients.add(clientInstanceId);
            Assert.assertEquals(topicName, "topic");
        }

        @Override
        public void onClientDisconnected(String topicName, UUID clientInstanceId)
        {
            clients.remove(clientInstanceId);
            Assert.assertEquals(topicName, "topic");
        }
    }
}