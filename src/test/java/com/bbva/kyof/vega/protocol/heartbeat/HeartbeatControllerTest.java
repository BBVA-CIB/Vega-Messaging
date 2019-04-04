package com.bbva.kyof.vega.protocol.heartbeat;

import com.bbva.kyof.vega.msg.IResponseListener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by cnebrera on 05/10/2016.
 */
public class HeartbeatControllerTest
{
    private Timer timer;

    @Before
    public void before()
    {
        this.timer = new Timer("TestTimer");
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
        final HeartbeatController controller = new HeartbeatController(this.timer, "topic", sender, listener, HeartbeatParameters.builder().build());

        Thread.sleep(2500);

        Assert.assertTrue(sender.requestSent.get() == 3);

        controller.stop();

        Thread.sleep(1000);

        Assert.assertTrue(sender.requestSent.get() == 3);
    }

    private class Sender implements IHeartbeatSender
    {
        final AtomicInteger requestSent = new AtomicInteger(0);

        @Override
        public void sendHeartbeat(IResponseListener responseListener, long timeout)
        {
            requestSent.getAndIncrement();
        }
    }

    private class ClientConnectionListener implements IClientConnectionListener
    {
        @Override
        public void onClientConnected(String topicName, UUID clientInstanceId) {}

        @Override
        public void onClientDisconnected(String topicName, UUID clientInstanceId) {}
    }
}