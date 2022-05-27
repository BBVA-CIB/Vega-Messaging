package com.bbva.kyof.vega.protocol.control;

import com.bbva.kyof.vega.TestConstants;
import com.bbva.kyof.vega.autodiscovery.AutodiscManager;
import com.bbva.kyof.vega.autodiscovery.daemon.CommandLineParserTest;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscInstanceInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTransportType;
import com.bbva.kyof.vega.config.general.ControlRcvConfig;
import com.bbva.kyof.vega.config.general.GlobalConfiguration;
import com.bbva.kyof.vega.config.general.TopicSecurityTemplateConfig;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.protocol.common.KeySecurityType;
import com.bbva.kyof.vega.protocol.common.SecurityParams;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.util.crypto.AESCrypto;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.easymock.EasyMock;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Created by cnebrera on 11/11/2016.
 */
public class ControlMsgsManagerTest
{
    private static final String KEYS_DIR_PATH = CommandLineParserTest.class.getClassLoader().getResource("keys").getPath();

    private static MediaDriver MEDIA_DRIVER;
    private static Aeron AERON;
    private static VegaContext VEGA_CONTEXT1;
    private static VegaContext VEGA_CONTEXT2;
    private static ControlMsgsManager CONTRL_MSGS_MNG_1;
    private static ControlMsgsManager CONTRL_MSGS_MNG_2;
    private static final UUID SUB_TOPIC1_ID = UUID.randomUUID();
    private static TopicSecurityTemplateConfig SUB_TOPIC1_SEC_CONFIG;
    private static TopicSecurityTemplateConfig PUB_TOPIC1_SEC_CONFIG;
    private static TopicSecurityTemplateConfig NO_PERM_PUB_TOPIC1_SEC_CONFIG;
    private static AutoDiscTopicInfo PUB_TOPIC_1_INFO;
    private static byte[] PUB_TOPIC1_SES_KEY;
    private static AutoDiscInstanceInfo INSTANCE1_INFO;
    private static AutoDiscInstanceInfo INSTANCE2_INFO;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        // Create a media driver
        MEDIA_DRIVER = MediaDriver.launchEmbedded();

        final Aeron.Context ctx1 = new Aeron.Context();
        ctx1.aeronDirectoryName(MEDIA_DRIVER.aeronDirectoryName());
        AERON = Aeron.connect(ctx1);

        // Create an auto-disc manager
        final AutodiscManager autodiscManager = EasyMock.createNiceMock(AutodiscManager.class);
        EasyMock.replay(autodiscManager);

        // Create the set with the secure topic ids
        final Set<Integer> secureTopicIds = new HashSet<>();
        secureTopicIds.add(11111);
        secureTopicIds.add(22222);

        // Create the configuration
        final ControlRcvConfig controlRcvConfig = new ControlRcvConfig();
        controlRcvConfig.completeAndValidateConfig();
        final GlobalConfiguration globalConfiguration = EasyMock.createNiceMock(GlobalConfiguration.class);
        EasyMock.expect(globalConfiguration.getControlRcvConfig()).andReturn(controlRcvConfig).anyTimes();
        EasyMock.expect(globalConfiguration.getAllSecureTopicsSecurityIds()).andReturn(secureTopicIds).anyTimes();
        EasyMock.replay(globalConfiguration);

        VEGA_CONTEXT1 = new VegaContext(AERON, globalConfiguration);
        VEGA_CONTEXT1.setAutodiscoveryManager(autodiscManager);
        VEGA_CONTEXT2 = new VegaContext(AERON, globalConfiguration);
        VEGA_CONTEXT2.setAutodiscoveryManager(autodiscManager);

        // Initialize RSA Cryptos security
        initializeSecurity();

        // Wait a bit
        Thread.sleep(1000);

        // Create 2 control msgs managers
        CONTRL_MSGS_MNG_1 = new ControlMsgsManager(VEGA_CONTEXT1);
        CONTRL_MSGS_MNG_2 = new ControlMsgsManager(VEGA_CONTEXT2);

        // Create the resources for the rest of the tests
        final Set<Integer> subTopic1SecureSubs = new HashSet<>(Collections.singletonList(11111));
        final Set<Integer> subTopic1SecurePubs = new HashSet<>(Collections.singletonList(22222));
        SUB_TOPIC1_SEC_CONFIG = new TopicSecurityTemplateConfig("topic1", 100L, subTopic1SecureSubs, subTopic1SecurePubs);

        final Set<Integer> pubTopic1SecureSubs = new HashSet<>(Collections.singletonList(22222));
        final Set<Integer> pubTopic1SecurePubs = new HashSet<>(Collections.singletonList(11111));
        PUB_TOPIC1_SEC_CONFIG = new TopicSecurityTemplateConfig("topic1", 100L, pubTopic1SecureSubs, pubTopic1SecurePubs);

        final Set<Integer> noPermPubTopic1SecureSubs = new HashSet<>();
        NO_PERM_PUB_TOPIC1_SEC_CONFIG = new TopicSecurityTemplateConfig("topic1", 100L, pubTopic1SecurePubs, noPermPubTopic1SecureSubs);

        PUB_TOPIC_1_INFO = new AutoDiscTopicInfo(VEGA_CONTEXT2.getInstanceUniqueId(), AutoDiscTransportType.PUB_UNI, UUID.randomUUID(), "topic1", 22222);

        final AESCrypto aesCrypto = AESCrypto.createNewInstance();
        PUB_TOPIC1_SES_KEY = aesCrypto.getAESKey();

        // Create the instance info for the simulated apps
        INSTANCE1_INFO = new AutoDiscInstanceInfo("instance1",
                VEGA_CONTEXT1.getInstanceUniqueId(),
                0, 0, 0, TestConstants.EMPTY_HOSTNAME,
                CONTRL_MSGS_MNG_1.getControlMsgsSubscriberParams().getIpAddress(),
                CONTRL_MSGS_MNG_1.getControlMsgsSubscriberParams().getPort(),
                CONTRL_MSGS_MNG_1.getControlMsgsSubscriberParams().getStreamId(),
                TestConstants.EMPTY_HOSTNAME);

        INSTANCE2_INFO = new AutoDiscInstanceInfo("instance2",
                VEGA_CONTEXT2.getInstanceUniqueId(),
                0, 0, 0, TestConstants.EMPTY_HOSTNAME,
                CONTRL_MSGS_MNG_2.getControlMsgsSubscriberParams().getIpAddress(),
                CONTRL_MSGS_MNG_2.getControlMsgsSubscriberParams().getPort(),
                CONTRL_MSGS_MNG_2.getControlMsgsSubscriberParams().getStreamId(),
                TestConstants.EMPTY_HOSTNAME);
    }

    private static void initializeSecurity() throws VegaException
    {
        final SecurityParams securityParams1 = SecurityParams.builder().keySecurityType(KeySecurityType.PLAIN_KEY_FILE).privateKeyDirPath(KEYS_DIR_PATH).
                publicKeysDirPath(KEYS_DIR_PATH).securityId(11111).build();
        final SecurityParams securityParams2 = SecurityParams.builder().keySecurityType(KeySecurityType.PLAIN_KEY_FILE).privateKeyDirPath(KEYS_DIR_PATH).
                publicKeysDirPath(KEYS_DIR_PATH).securityId(22222).build();

        // Set in the vega contexts
        VEGA_CONTEXT1.initializeSecurity(securityParams1);
        VEGA_CONTEXT2.initializeSecurity(securityParams2);
    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        CONTRL_MSGS_MNG_1.close();
        CONTRL_MSGS_MNG_2.close();

        // Try to call after close
        CONTRL_MSGS_MNG_1.onNewAutoDiscInstanceInfo(INSTANCE2_INFO);
        CONTRL_MSGS_MNG_1.onTimedOutAutoDiscInstanceInfo(INSTANCE2_INFO);

        AERON.close();
        CloseHelper.quietClose(MEDIA_DRIVER);
    }

    @Test
    public void testWhenNoConnections() throws Exception
    {
        CONTRL_MSGS_MNG_1.getRecurityRequestsNotifier().addedPubForSubTopic(PUB_TOPIC_1_INFO, SUB_TOPIC1_ID, SUB_TOPIC1_SEC_CONFIG);

        // It should start trying to get the topic information, but nothing should happen
        Thread.sleep(1000);
        Assert.assertNull(this.findDecoder(CONTRL_MSGS_MNG_1, PUB_TOPIC_1_INFO.getUniqueId()));

        // Add again, it will ignore it
        CONTRL_MSGS_MNG_1.getRecurityRequestsNotifier().addedPubForSubTopic(PUB_TOPIC_1_INFO, SUB_TOPIC1_ID, SUB_TOPIC1_SEC_CONFIG);

        // Now remove the publisher, twice, the second one should be ignored
        CONTRL_MSGS_MNG_1.getRecurityRequestsNotifier().removedPubForSubTopic(PUB_TOPIC_1_INFO, SUB_TOPIC1_ID);
        CONTRL_MSGS_MNG_1.getRecurityRequestsNotifier().removedPubForSubTopic(PUB_TOPIC_1_INFO, SUB_TOPIC1_ID);

        Thread.sleep(1000);
        Assert.assertNull(this.findDecoder(CONTRL_MSGS_MNG_1, PUB_TOPIC_1_INFO.getUniqueId()));

        // Now try adding and removing the whole SUB TOPIC
        CONTRL_MSGS_MNG_1.getRecurityRequestsNotifier().addedPubForSubTopic(PUB_TOPIC_1_INFO, SUB_TOPIC1_ID, SUB_TOPIC1_SEC_CONFIG);

        Thread.sleep(1000);
        Assert.assertNull(this.findDecoder(CONTRL_MSGS_MNG_1, PUB_TOPIC_1_INFO.getUniqueId()));

        CONTRL_MSGS_MNG_1.getRecurityRequestsNotifier().removedSecureSubTopic(SUB_TOPIC1_ID);

        Thread.sleep(1000);
        Assert.assertNull(this.findDecoder(CONTRL_MSGS_MNG_1, PUB_TOPIC_1_INFO.getUniqueId()));
    }

    private AESCrypto findDecoder(ControlMsgsManager manager, UUID pubTopicId)
    {
        return manager.getSecureMessagesDecoder().getAesCryptoForSecPub(pubTopicId);
    }

    @Test
    public void testBothWorking() throws Exception
    {
        // First connect both managers with the new instance info adverts
        CONTRL_MSGS_MNG_1.onNewAutoDiscInstanceInfo(INSTANCE2_INFO);
        CONTRL_MSGS_MNG_2.onNewAutoDiscInstanceInfo(INSTANCE1_INFO);

        // Wait for the connections to be performed
        Thread.sleep(2000);

        // Register secure topic publisher 1 in instance 2
        CONTRL_MSGS_MNG_2.getOwnPubSecureChangesNotifier().onOwnSecureTopicPublisherAdded(PUB_TOPIC_1_INFO.getUniqueId(), PUB_TOPIC1_SES_KEY, PUB_TOPIC1_SEC_CONFIG);

        // Now try to get the topic publisher information from instance 1
        CONTRL_MSGS_MNG_1.getRecurityRequestsNotifier().addedPubForSubTopic(PUB_TOPIC_1_INFO, SUB_TOPIC1_ID, SUB_TOPIC1_SEC_CONFIG);

        // After some time we should have the decoder
        Thread.sleep(1000);
        Assert.assertNotNull(this.findDecoder(CONTRL_MSGS_MNG_1, PUB_TOPIC_1_INFO.getUniqueId()));

        // Cancel everything
        CONTRL_MSGS_MNG_2.getOwnPubSecureChangesNotifier().onOwnSecuredTopicPublisherRemoved(PUB_TOPIC_1_INFO.getUniqueId());
        CONTRL_MSGS_MNG_1.getRecurityRequestsNotifier().removedSecureSubTopic(SUB_TOPIC1_ID);

        CONTRL_MSGS_MNG_1.onTimedOutAutoDiscInstanceInfo(INSTANCE2_INFO);
        CONTRL_MSGS_MNG_2.onTimedOutAutoDiscInstanceInfo(INSTANCE1_INFO);
    }

    @Test
    public void testNoPermissions() throws Exception
    {
        // First connect both managers with the new instance info adverts
        CONTRL_MSGS_MNG_1.onNewAutoDiscInstanceInfo(INSTANCE2_INFO);
        CONTRL_MSGS_MNG_2.onNewAutoDiscInstanceInfo(INSTANCE1_INFO);

        // Wait for the connections to be performed
        Thread.sleep(2000);

        // Register secure topic publisher 1 in instance 2
        CONTRL_MSGS_MNG_2.getOwnPubSecureChangesNotifier().onOwnSecureTopicPublisherAdded(PUB_TOPIC_1_INFO.getUniqueId(), PUB_TOPIC1_SES_KEY, NO_PERM_PUB_TOPIC1_SEC_CONFIG);

        // Now try to get the topic publisher information from instance 1
        CONTRL_MSGS_MNG_1.getRecurityRequestsNotifier().addedPubForSubTopic(PUB_TOPIC_1_INFO, SUB_TOPIC1_ID, SUB_TOPIC1_SEC_CONFIG);

        // After some time we should have the decoder
        Thread.sleep(1000);
        Assert.assertNull(this.findDecoder(CONTRL_MSGS_MNG_1, PUB_TOPIC_1_INFO.getUniqueId()));

        // Cancel everything
        CONTRL_MSGS_MNG_2.getOwnPubSecureChangesNotifier().onOwnSecuredTopicPublisherRemoved(PUB_TOPIC_1_INFO.getUniqueId());
        CONTRL_MSGS_MNG_1.getRecurityRequestsNotifier().removedSecureSubTopic(SUB_TOPIC1_ID);

        CONTRL_MSGS_MNG_1.onTimedOutAutoDiscInstanceInfo(INSTANCE2_INFO);
        CONTRL_MSGS_MNG_2.onTimedOutAutoDiscInstanceInfo(INSTANCE1_INFO);
    }
}