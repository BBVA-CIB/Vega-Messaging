package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.config.general.ConfigReader;
import com.bbva.kyof.vega.config.general.ConfigReaderTest;
import com.bbva.kyof.vega.msg.MsgReqHeader;
import com.bbva.kyof.vega.msg.RcvMessage;
import com.bbva.kyof.vega.msg.RcvRequest;
import com.bbva.kyof.vega.msg.RcvResponse;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.junit.*;

import java.util.UUID;

/**
 * Created by cnebrera on 11/08/16.
 */
public class SubscribersPollersManagerTest
{
    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static SubnetAddress SUBNET_ADDRESS;
    private static VegaContext VEGA_CONTEXT;

    /** File containing the configuration */
    private static final String validConfigFile = ConfigReaderTest.class.getClassLoader().getResource("config/pollersManagerTestConfig.xml").getPath();
    private SubscribersPollersManager pollersManager;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx1 = new Aeron.Context();
        ctx1.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON = Aeron.connect(ctx1);

        SUBNET_ADDRESS = InetUtil.getDefaultSubnet();
        VEGA_CONTEXT = new VegaContext(AERON, ConfigReader.readConfiguration(validConfigFile));
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Before
    public void before()
    {
        final Listener listener = new Listener();
        pollersManager = new SubscribersPollersManager(VEGA_CONTEXT, listener);
    }

    @After
    public void after()
    {
        pollersManager.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnknownPoller() throws Exception
    {
       pollersManager.getPoller("unknownPoller");
    }

    @Test
    public void testRightPollers() throws Exception
    {
        final SubcribersPoller poller1 = pollersManager.getPoller("poller1");
        Assert.assertNotNull(poller1);

        // If we ask for the same poller should return the same object
        Assert.assertEquals(poller1, pollersManager.getPoller("poller1"));

        Assert.assertNotNull(pollersManager.getPoller("poller2"));
    }

    private class Listener implements ISubscribersPollerListener
    {
        @Override
        public void onDataMsgReceived(RcvMessage msg)
        {
        }

        @Override
        public void onEncryptedDataMsgReceived(RcvMessage msg)
        {

        }

        @Override
        public void onDataRequestMsgReceived(RcvRequest request)
        {
        }

        @Override
        public void onDataResponseMsgReceived(RcvResponse response)
        {
        }

        @Override
        public void onHeartbeatRequestMsgReceived(MsgReqHeader heartbeatReqMsgHeader)
        {

        }
    }
}