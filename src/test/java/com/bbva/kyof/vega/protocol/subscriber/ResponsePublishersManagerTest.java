package com.bbva.kyof.vega.protocol.subscriber;

import com.bbva.kyof.vega.TestConstants;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.config.general.ConfigReader;
import com.bbva.kyof.vega.config.general.ConfigReaderTest;
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
 * Created by cnebrera on 18/08/16.
 */
public class ResponsePublishersManagerTest
{
    private static final String validConfigFile = ConfigReaderTest.class.getClassLoader().getResource("config/subscribersManagerTestConfig.xml").getPath();

    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static SubnetAddress SUBNET_ADDRESS;
    private static int IP_ADDRESS;
    private static String HOSTNAME;
    private static VegaContext VEGA_CONTEXT;

    private static ResponsePublishersManager RESPONSE_PUB_MANAGER;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx1 = new Aeron.Context();
        ctx1.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());

        AERON = Aeron.connect(ctx1);

        SUBNET_ADDRESS = InetUtil.getDefaultSubnet();
        IP_ADDRESS = InetUtil.convertIpAddressToInt(SUBNET_ADDRESS.getIpAddres().getHostAddress());
        HOSTNAME = SUBNET_ADDRESS.getIpAddres().getHostName();
        VEGA_CONTEXT = new VegaContext(AERON, ConfigReader.readConfiguration(validConfigFile));

        RESPONSE_PUB_MANAGER = new ResponsePublishersManager(VEGA_CONTEXT);

        // Give it time to start
        Thread.sleep(1000);
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        RESPONSE_PUB_MANAGER.close();
        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Test
    public void testNewAutodiscInstanceInfo() throws Exception
    {
        final AutoDiscInstanceInfo instanceInfo1 = new AutoDiscInstanceInfo("instName1", UUID.randomUUID(), IP_ADDRESS, 33333, 2, HOSTNAME, IP_ADDRESS, 33333, 10, HOSTNAME);
        final AutoDiscInstanceInfo instanceInfo2 = new AutoDiscInstanceInfo("instName2", UUID.randomUUID(), IP_ADDRESS, 33333, 2, HOSTNAME, IP_ADDRESS, 33333, 10, HOSTNAME);
        final AutoDiscInstanceInfo instanceInfo3 = new AutoDiscInstanceInfo("instName3", UUID.randomUUID(), IP_ADDRESS, 33333, 5, HOSTNAME, IP_ADDRESS, 33333, 15, HOSTNAME);

        // Add response publishers and check
        RESPONSE_PUB_MANAGER.onNewAutoDiscInstanceInfo(instanceInfo1);
        Assert.assertNotNull(RESPONSE_PUB_MANAGER.getResponsePublisherForInstance(instanceInfo1.getUniqueId()));
        Assert.assertEquals(1, RESPONSE_PUB_MANAGER.getNumRemoteInstancesInfo());
        Assert.assertEquals(1, RESPONSE_PUB_MANAGER.getNumResponsePublishers());

        RESPONSE_PUB_MANAGER.onNewAutoDiscInstanceInfo(instanceInfo2);
        Assert.assertNotNull(RESPONSE_PUB_MANAGER.getResponsePublisherForInstance(instanceInfo2.getUniqueId()));
        Assert.assertEquals(2, RESPONSE_PUB_MANAGER.getNumRemoteInstancesInfo());
        Assert.assertEquals(1, RESPONSE_PUB_MANAGER.getNumResponsePublishers());

        RESPONSE_PUB_MANAGER.onNewAutoDiscInstanceInfo(instanceInfo3);
        Assert.assertNotNull(RESPONSE_PUB_MANAGER.getResponsePublisherForInstance(instanceInfo3.getUniqueId()));
        Assert.assertEquals(3, RESPONSE_PUB_MANAGER.getNumRemoteInstancesInfo());
        Assert.assertEquals(2, RESPONSE_PUB_MANAGER.getNumResponsePublishers());

        // Remove response publishers and check
        RESPONSE_PUB_MANAGER.onTimedOutAutoDiscInstanceInfo(instanceInfo1);
        Assert.assertNull(RESPONSE_PUB_MANAGER.getResponsePublisherForInstance(instanceInfo1.getUniqueId()));
        Assert.assertEquals(2, RESPONSE_PUB_MANAGER.getNumRemoteInstancesInfo());
        Assert.assertEquals(2, RESPONSE_PUB_MANAGER.getNumResponsePublishers());

        RESPONSE_PUB_MANAGER.onTimedOutAutoDiscInstanceInfo(instanceInfo2);
        Assert.assertNull(RESPONSE_PUB_MANAGER.getResponsePublisherForInstance(instanceInfo2.getUniqueId()));
        Assert.assertEquals(1, RESPONSE_PUB_MANAGER.getNumRemoteInstancesInfo());
        Assert.assertEquals(1, RESPONSE_PUB_MANAGER.getNumResponsePublishers());

        RESPONSE_PUB_MANAGER.onTimedOutAutoDiscInstanceInfo(instanceInfo3);
        Assert.assertNull(RESPONSE_PUB_MANAGER.getResponsePublisherForInstance(instanceInfo3.getUniqueId()));
        Assert.assertEquals(0, RESPONSE_PUB_MANAGER.getNumRemoteInstancesInfo());
        Assert.assertEquals(0, RESPONSE_PUB_MANAGER.getNumResponsePublishers());
    }

    @Test
    public void testDuplicatedEvents() throws Exception
    {
        final AutoDiscInstanceInfo instanceInfo1 = new AutoDiscInstanceInfo("instName1", UUID.randomUUID(), IP_ADDRESS, 33333, 2, HOSTNAME, IP_ADDRESS, 33333, 10, HOSTNAME);

        // Add response publishers and check
        RESPONSE_PUB_MANAGER.onNewAutoDiscInstanceInfo(instanceInfo1);
        Assert.assertNotNull(RESPONSE_PUB_MANAGER.getResponsePublisherForInstance(instanceInfo1.getUniqueId()));
        Assert.assertEquals(1, RESPONSE_PUB_MANAGER.getNumRemoteInstancesInfo());
        Assert.assertEquals(1, RESPONSE_PUB_MANAGER.getNumResponsePublishers());

        RESPONSE_PUB_MANAGER.onNewAutoDiscInstanceInfo(instanceInfo1);
        Assert.assertNotNull(RESPONSE_PUB_MANAGER.getResponsePublisherForInstance(instanceInfo1.getUniqueId()));
        Assert.assertEquals(1, RESPONSE_PUB_MANAGER.getNumRemoteInstancesInfo());
        Assert.assertEquals(1, RESPONSE_PUB_MANAGER.getNumResponsePublishers());

        // Remove response publishers and check
        RESPONSE_PUB_MANAGER.onTimedOutAutoDiscInstanceInfo(instanceInfo1);
        Assert.assertNull(RESPONSE_PUB_MANAGER.getResponsePublisherForInstance(instanceInfo1.getUniqueId()));
        Assert.assertEquals(0, RESPONSE_PUB_MANAGER.getNumRemoteInstancesInfo());
        Assert.assertEquals(0, RESPONSE_PUB_MANAGER.getNumResponsePublishers());

        RESPONSE_PUB_MANAGER.onTimedOutAutoDiscInstanceInfo(instanceInfo1);
        Assert.assertNull(RESPONSE_PUB_MANAGER.getResponsePublisherForInstance(instanceInfo1.getUniqueId()));
        Assert.assertEquals(0, RESPONSE_PUB_MANAGER.getNumRemoteInstancesInfo());
        Assert.assertEquals(0, RESPONSE_PUB_MANAGER.getNumResponsePublishers());
    }
}