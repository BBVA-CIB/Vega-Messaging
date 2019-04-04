package com.bbva.kyof.vega.protocol.common;

import com.bbva.kyof.vega.autodiscovery.AutodiscManager;
import com.bbva.kyof.vega.autodiscovery.IAutodiscManager;
import com.bbva.kyof.vega.config.general.GlobalConfiguration;
import com.bbva.kyof.vega.exception.VegaException;
import io.aeron.Aeron;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by cnebrera on 11/08/16.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Aeron.class)
@PowerMockIgnore({ "javax.xml.*", "org.xml.sax.*", "org.w3c.*", "javax.crypto.*" })
public class VegaContextTest
{
    private static final String KEYS_DIR = SecurityParamsTest.class.getClassLoader().getResource("keys").getPath();
    private VegaContext vegaContext;

    @Before
    public void before()
    {
        final Aeron aeron = PowerMock.createNiceMock(Aeron.class);
        PowerMock.replayAll(aeron);

        final Set<Integer> topicSecurityIds = new HashSet<>();
        topicSecurityIds.add(11111);
        topicSecurityIds.add(22222);

        GlobalConfiguration globalConfig = EasyMock.createNiceMock(GlobalConfiguration.class);
        EasyMock.expect(globalConfig.getAllSecureTopicsSecurityIds()).andReturn(topicSecurityIds).anyTimes();
        EasyMock.replay(globalConfig);

        this.vegaContext = new VegaContext(aeron, globalConfig);

        Assert.assertEquals(aeron, vegaContext.getAeron());
        Assert.assertEquals(globalConfig, vegaContext.getInstanceConfig());
        Assert.assertNull(vegaContext.getAutodiscoveryManager());
        Assert.assertNull(vegaContext.getAsyncRequestManager());
        Assert.assertNotNull(vegaContext.getInstanceUniqueId());
    }

    @Test
    public void testConstructorGettersAndSetters()
    {
        // Now set the missing parameters
        IAutodiscManager autoDiscManager = PowerMock.createNiceMock(AutodiscManager.class);
        AsyncRequestManager reqManager = PowerMock.createNiceMock(AsyncRequestManager.class);
        PowerMock.replayAll(autoDiscManager, reqManager);

        vegaContext.setAsyncRequestManager(reqManager);
        vegaContext.setAutodiscoveryManager(autoDiscManager);

        Assert.assertEquals(autoDiscManager, vegaContext.getAutodiscoveryManager());
        Assert.assertEquals(reqManager, vegaContext.getAsyncRequestManager());
    }

    @Test
    public void testStopHeartbeatsTimer()
    {
        vegaContext.stopHeartsbeatTimer();
    }

    @Test
    public void testInitializeSecurity() throws VegaException
    {
        final SecurityParams plainParams = SecurityParams.builder().keySecurityType(KeySecurityType.PLAIN_KEY_FILE).securityId(11111).privateKeyDirPath(KEYS_DIR).publicKeysDirPath(KEYS_DIR).build();
        plainParams.validateParams();

        vegaContext.initializeSecurity(plainParams);
    }
}