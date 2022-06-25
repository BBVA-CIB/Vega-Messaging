package com.bbva.kyof.vega.protocol.control;

import com.bbva.kyof.vega.config.general.GlobalConfiguration;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.msg.MsgSecurityErrorResp;
import com.bbva.kyof.vega.msg.MsgSecurityReq;
import com.bbva.kyof.vega.msg.MsgSecurityResp;
import com.bbva.kyof.vega.msg.MsgType;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.serialization.UnsafeBufferSerializer;
import com.bbva.kyof.vega.util.crypto.RSACrypto;
import com.bbva.kyof.vega.util.crypto.RSAKeysHelper;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import lombok.Getter;
import org.agrona.CloseHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by cnebrera on 11/11/2016.
 */
public class ControlMsgsPollerAndPublisherByHostnameTest
{
    private final static UUID OWN_INSTANCE_ID = UUID.randomUUID();
    private final static UUID TARGET_INSTANCE_ID = UUID.randomUUID();
    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static ControlSubscriber CONTROL_SUB;
    private static ControlPublisher CONTROL_PUB;
    private static RSACrypto RSACrypto1;
    private static RSACrypto RSACrypto2;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        final KeyPair keyPair1 = RSAKeysHelper.generateKeyPair();
        final KeyPair keyPair2 = RSAKeysHelper.generateKeyPair();
        final KeyPair keyPair3 = RSAKeysHelper.generateKeyPair();

        // RSA Crypto public keys map
        Map<Integer, PublicKey> publicKeysMap = new HashMap<>();
        publicKeysMap.put(1, keyPair1.getPublic());
        publicKeysMap.put(2, keyPair2.getPublic());
        publicKeysMap.put(3, keyPair3.getPublic());

        // Now add the public keys to ensure the trusts in the applications following the Test Class description
        RSACrypto1 = new RSACrypto(keyPair1.getPrivate(), publicKeysMap);
        RSACrypto2 = new RSACrypto(keyPair2.getPrivate(), publicKeysMap);

        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx1 = new Aeron.Context();
        ctx1.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON = Aeron.connect(ctx1);

        final SubnetAddress subnetAddress = InetUtil.getDefaultSubnet();
        final VegaContext vegaContext = new VegaContext(AERON, new GlobalConfiguration());

        Thread.sleep(1000);

        final int ucastIp = InetUtil.convertIpAddressToInt(subnetAddress.getIpAddres().getHostAddress());

        final ControlSubscriberParams controlSubscriberParams = new ControlSubscriberParams(ucastIp, 29333, 2, subnetAddress);
        CONTROL_SUB = new ControlSubscriber(vegaContext, controlSubscriberParams);

        final ControlPublisherParams controlPubParams = new ControlPublisherParams(ucastIp, 29333, 2, subnetAddress);
        CONTROL_PUB = new ControlPublisher(vegaContext, controlPubParams);

        Assert.assertEquals(CONTROL_PUB.getParams(), controlPubParams);

        Thread.sleep(1000);
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        CONTROL_PUB.close();
        CONTROL_PUB.close();

        // Send on closed publisher, it should not fail since it wont event try
        CONTROL_PUB.sendMessage(MsgType.CONTROL_SECURITY_REQ, null, 0, 200);


        CONTROL_SUB.close();
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Test
    public void createPollerAndPoll() throws Exception
    {
        // Create and start the poller
        final Listener listener = new Listener();

        final ControlMsgsPoller poller = new ControlMsgsPoller(CONTROL_SUB, listener, listener, OWN_INSTANCE_ID);
        poller.start();

        Thread.sleep(100);

        final UnsafeBufferSerializer sendBufferSerializer = new UnsafeBufferSerializer();
        sendBufferSerializer.wrap(ByteBuffer.allocate(1024));

        this.testSendRequest(listener, sendBufferSerializer);
        this.testSendResponse(listener, sendBufferSerializer);
        this.testSendErrorResponse(listener, sendBufferSerializer);

        poller.close();
    }

    private void testSendRequest(Listener listener, UnsafeBufferSerializer sendBufferSerializer) throws VegaException, InterruptedException
    {
        // Send a request, a response and an error response
        final MsgSecurityReq msgSecurity = new MsgSecurityReq();
        msgSecurity.setInstanceId(OWN_INSTANCE_ID);
        msgSecurity.setSenderSecurityId(1);
        msgSecurity.setRequestId(UUID.randomUUID());
        msgSecurity.setTopicPublisherId(UUID.randomUUID());
        msgSecurity.setTargetVegaInstanceId(TARGET_INSTANCE_ID);

        // Sign and serialize
        sendBufferSerializer.setOffset(0);
        msgSecurity.signAndSerialize(sendBufferSerializer, RSACrypto1);

        // Send
        CONTROL_PUB.sendMessage(MsgType.CONTROL_SECURITY_REQ, sendBufferSerializer.getInternalBuffer(), 0, sendBufferSerializer.getOffset());
        Thread.sleep(500);

        // It should have not arrived because the target instance Id is wrong
        Assert.assertNull(listener.getRcvRequest());

        // Set the right id and try again
        msgSecurity.setTargetVegaInstanceId(OWN_INSTANCE_ID);
        sendBufferSerializer.setOffset(0);
        msgSecurity.signAndSerialize(sendBufferSerializer, RSACrypto1);
        CONTROL_PUB.sendMessage(MsgType.CONTROL_SECURITY_REQ, sendBufferSerializer.getInternalBuffer(), 0, sendBufferSerializer.getOffset());

        Thread.sleep(500);

        // It should have arrived because the target instance Id is correct now
        Assert.assertNotNull(listener.getRcvRequest());
    }

    private void testSendResponse(Listener listener, UnsafeBufferSerializer sendBufferSerializer) throws VegaException, InterruptedException
    {
        // Send a request, a response and an error response
        final MsgSecurityResp msgSecurity = new MsgSecurityResp();
        msgSecurity.setInstanceId(OWN_INSTANCE_ID);
        msgSecurity.setSenderSecurityId(1);
        msgSecurity.setRequestId(UUID.randomUUID());
        msgSecurity.setTopicPublisherId(UUID.randomUUID());
        msgSecurity.setTargetVegaInstanceId(TARGET_INSTANCE_ID);
        msgSecurity.setEncodedSessionKey(new byte[128]);

        // Sign and serialize
        sendBufferSerializer.setOffset(0);
        msgSecurity.signAndSerialize(sendBufferSerializer, RSACrypto1);

        // Send
        CONTROL_PUB.sendMessage(MsgType.CONTROL_SECURITY_RESP, sendBufferSerializer.getInternalBuffer(), 0, sendBufferSerializer.getOffset());
        Thread.sleep(500);

        // It should have not arrived because the target instance Id is wrong
        Assert.assertNull(listener.getRcvResp());

        // Set the right id and try again
        msgSecurity.setTargetVegaInstanceId(OWN_INSTANCE_ID);
        sendBufferSerializer.setOffset(0);
        msgSecurity.signAndSerialize(sendBufferSerializer, RSACrypto1);
        CONTROL_PUB.sendMessage(MsgType.CONTROL_SECURITY_RESP, sendBufferSerializer.getInternalBuffer(), 0, sendBufferSerializer.getOffset());

        Thread.sleep(500);

        // It should have arrived because the target instance Id is correct now
        Assert.assertNotNull(listener.getRcvResp());
    }

    private void testSendErrorResponse(Listener listener, UnsafeBufferSerializer sendBufferSerializer) throws VegaException, InterruptedException
    {
        // Send a request, a response and an error response
        final MsgSecurityErrorResp msgSecurity = new MsgSecurityErrorResp();
        msgSecurity.setInstanceId(OWN_INSTANCE_ID);
        msgSecurity.setSenderSecurityId(1);
        msgSecurity.setRequestId(UUID.randomUUID());
        msgSecurity.setTopicPublisherId(UUID.randomUUID());
        msgSecurity.setTargetVegaInstanceId(TARGET_INSTANCE_ID);
        msgSecurity.setErrorCode(MsgSecurityErrorResp.SIGNATURE_ERROR);

        // Sign and serialize
        sendBufferSerializer.setOffset(0);
        msgSecurity.signAndSerialize(sendBufferSerializer, RSACrypto1);

        // Send
        CONTROL_PUB.sendMessage(MsgType.CONTROL_SECURITY_ERROR_RESP, sendBufferSerializer.getInternalBuffer(), 0, sendBufferSerializer.getOffset());
        Thread.sleep(500);

        // It should have not arrived because the target instance Id is wrong
        Assert.assertNull(listener.getRcvErrorResp());

        // Set the right id and try again
        msgSecurity.setTargetVegaInstanceId(OWN_INSTANCE_ID);
        sendBufferSerializer.setOffset(0);
        msgSecurity.signAndSerialize(sendBufferSerializer, RSACrypto1);
        CONTROL_PUB.sendMessage(MsgType.CONTROL_SECURITY_ERROR_RESP, sendBufferSerializer.getInternalBuffer(), 0, sendBufferSerializer.getOffset());

        Thread.sleep(500);

        // It should have arrived because the target instance Id is correct now
        Assert.assertNotNull(listener.getRcvErrorResp());
    }


    private static class Listener implements ISecurityRequestListener, ISecurityResponseListener
    {
        @Getter
        MsgSecurityReq rcvRequest = null;
        @Getter
        MsgSecurityResp rcvResp = null;
        @Getter
        MsgSecurityErrorResp rcvErrorResp = null;

        @Override
        public void onSecurityRequestReceived(MsgSecurityReq securityReq)
        {
            this.rcvRequest = securityReq;
        }

        @Override
        public void onSecuirtyResponseReceived(MsgSecurityResp responseMsg)
        {
            this.rcvResp = responseMsg;
        }

        @Override
        public void onSecurityErrorResponseReceived(MsgSecurityErrorResp errorResponseMsg)
        {
            this.rcvErrorResp = errorResponseMsg;
        }
    }
}