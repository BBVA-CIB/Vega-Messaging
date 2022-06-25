package com.bbva.kyof.vega.protocol.control;

import com.bbva.kyof.vega.TestConstants;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.config.general.ControlRcvConfig;
import com.bbva.kyof.vega.config.general.GlobalConfiguration;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

/**
 * Created by cnebrera on 11/11/2016.
 */
public class ControlPublishersTest
{
    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static SubnetAddress SUBNET_ADDRESS;
    private static int SUBNET_INT_ADDRESS;
    private static VegaContext VEGA_CONTEXT;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx1 = new Aeron.Context();
        ctx1.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON = Aeron.connect(ctx1);

        SUBNET_ADDRESS = InetUtil.getDefaultSubnet();
        SUBNET_INT_ADDRESS = InetUtil.convertIpAddressToInt(SUBNET_ADDRESS.getIpAddres());

        // Create the control receiver configuration
        final ControlRcvConfig controlRcvConfig = new ControlRcvConfig();
        controlRcvConfig.completeAndValidateConfig();

        final GlobalConfiguration globalConfiguration = GlobalConfiguration.builder().controlRcvConfig(controlRcvConfig).build();

        VEGA_CONTEXT = new VegaContext(AERON, globalConfiguration);

        Thread.sleep(1000);
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Test
    public void testAddRemove()
    {
        final ControlPublishers controlPublishers = new ControlPublishers(VEGA_CONTEXT);

        // Notify about a new instance
        final AutoDiscInstanceInfo instance1Info = new AutoDiscInstanceInfo(
                "instance1",
                UUID.randomUUID(),
                0, 0, 0, null, // We don't care about the responses info for this test
                SUBNET_INT_ADDRESS,
                28888,
                2,
                TestConstants.EMPTY_HOSTNAME);

        controlPublishers.onNewAutoDiscInstanceInfo(instance1Info);

        // It should have created a new ControlPublisher
        final ControlPublisher instance1ControlPub = controlPublishers.getControlPublisherForInstance(instance1Info.getUniqueId());
        Assert.assertNotNull(instance1ControlPub);

        // Now insert the same one, nothing should happen
        controlPublishers.onNewAutoDiscInstanceInfo(instance1Info);

        // Create a new one with same parameters
        final AutoDiscInstanceInfo instance2Info = new AutoDiscInstanceInfo(
                "instance2",
                UUID.randomUUID(),
                0, 0, 0, null,// We don't care about the responses info for this test
                SUBNET_INT_ADDRESS,
                28888,
                2,
                TestConstants.EMPTY_HOSTNAME);

        controlPublishers.onNewAutoDiscInstanceInfo(instance2Info);
        final ControlPublisher instance2ControlPub = controlPublishers.getControlPublisherForInstance(instance2Info.getUniqueId());
        Assert.assertNotNull(instance2ControlPub);

        // It should have created a single ControlPublisher since the params are the same
        Assert.assertNotNull(controlPublishers.getControlPublisherForInstance(instance1Info.getUniqueId()));
        Assert.assertNotNull(controlPublishers.getControlPublisherForInstance(instance2Info.getUniqueId()));
        Assert.assertEquals(controlPublishers.getControlPublisherForInstance(instance1Info.getUniqueId()),
                controlPublishers.getControlPublisherForInstance(instance2Info.getUniqueId()));

        // Now create another one with different parameters
        final AutoDiscInstanceInfo instance3Info = new AutoDiscInstanceInfo(
                "instance3",
                UUID.randomUUID(),
                0, 0, 0, null,// We don't care about the responses info for this test
                SUBNET_INT_ADDRESS,
                28888,
                5,
                TestConstants.EMPTY_HOSTNAME);

        controlPublishers.onNewAutoDiscInstanceInfo(instance3Info);
        final ControlPublisher instance3ControlPub = controlPublishers.getControlPublisherForInstance(instance3Info.getUniqueId());
        Assert.assertNotNull(instance3ControlPub);

        // It should have created a new one this time
        Assert.assertNotNull(controlPublishers.getControlPublisherForInstance(instance3Info.getUniqueId()));
        Assert.assertNotEquals(controlPublishers.getControlPublisherForInstance(instance1Info.getUniqueId()),
                controlPublishers.getControlPublisherForInstance(instance3Info.getUniqueId()));

        // Remove the one just created
        controlPublishers.onTimedOutAutoDiscInstanceInfo(instance3Info);
        Assert.assertNull(controlPublishers.getControlPublisherForInstance(instance3Info.getUniqueId()));
        Assert.assertTrue(instance3ControlPub.isClosed());

        // Remove the first one, it should not close the ControlPublisher
        controlPublishers.onTimedOutAutoDiscInstanceInfo(instance1Info);
        Assert.assertNull(controlPublishers.getControlPublisherForInstance(instance1Info.getUniqueId()));
        Assert.assertFalse(instance1ControlPub.isClosed());

        // Remove the last one
        controlPublishers.onTimedOutAutoDiscInstanceInfo(instance2Info);
        Assert.assertNull(controlPublishers.getControlPublisherForInstance(instance2Info.getUniqueId()));
        Assert.assertTrue(instance2ControlPub.isClosed());

        // Call again, nothing should happen
        controlPublishers.onTimedOutAutoDiscInstanceInfo(instance2Info);

        // Close
        controlPublishers.close();
    }

    @Test
    public void testClose()
    {
        final ControlPublishers controlPublishers = new ControlPublishers(VEGA_CONTEXT);

        // Create 3 instances
        final AutoDiscInstanceInfo instance1Info = new AutoDiscInstanceInfo(
                "instance1",
                UUID.randomUUID(),
                0, 0, 0, null, // We don't care about the responses info for this test
                SUBNET_INT_ADDRESS,
                28889,
                2,
                TestConstants.EMPTY_HOSTNAME);
        final AutoDiscInstanceInfo instance2Info = new AutoDiscInstanceInfo(
                "instance2",
                UUID.randomUUID(),
                0, 0, 0, null, // We don't care about the responses info for this test
                SUBNET_INT_ADDRESS,
                28889,
                2,
                TestConstants.EMPTY_HOSTNAME);
        final AutoDiscInstanceInfo instance3Info = new AutoDiscInstanceInfo(
                "instance3",
                UUID.randomUUID(),
                0, 0, 0, null,// We don't care about the responses info for this test
                SUBNET_INT_ADDRESS,
                28889,
                3,
                TestConstants.EMPTY_HOSTNAME);

        controlPublishers.onNewAutoDiscInstanceInfo(instance1Info);
        controlPublishers.onNewAutoDiscInstanceInfo(instance2Info);
        controlPublishers.onNewAutoDiscInstanceInfo(instance3Info);

        // Test that the 3 of them have been created
        final ControlPublisher instance1ControlPub = controlPublishers.getControlPublisherForInstance(instance1Info.getUniqueId());
        Assert.assertNotNull(instance1ControlPub);
        final ControlPublisher instance2ControlPub = controlPublishers.getControlPublisherForInstance(instance2Info.getUniqueId());
        Assert.assertNotNull(instance2ControlPub);
        final ControlPublisher instance3ControlPub = controlPublishers.getControlPublisherForInstance(instance3Info.getUniqueId());
        Assert.assertNotNull(instance3ControlPub);

        // Close, everything should be closed
        controlPublishers.close();

        Assert.assertNull(controlPublishers.getControlPublisherForInstance(instance1Info.getUniqueId()));
        Assert.assertTrue(instance1ControlPub.isClosed());
        Assert.assertNull(controlPublishers.getControlPublisherForInstance(instance2Info.getUniqueId()));
        Assert.assertTrue(instance2ControlPub.isClosed());
        Assert.assertNull(controlPublishers.getControlPublisherForInstance(instance3Info.getUniqueId()));
        Assert.assertTrue(instance3ControlPub.isClosed());
    }
}