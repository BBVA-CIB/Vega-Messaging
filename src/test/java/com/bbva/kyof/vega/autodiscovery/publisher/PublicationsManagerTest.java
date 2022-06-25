package com.bbva.kyof.vega.autodiscovery.publisher;

import com.bbva.kyof.vega.autodiscovery.model.AutoDiscDaemonServerInfo;
import com.bbva.kyof.vega.config.general.AutoDiscoType;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.config.general.UnicastInfo;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.util.collection.NativeArraySet;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Class to test PublicationsManager
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Aeron.class)
public class PublicationsManagerTest
{
    /** Number of autodiscovery unicast daemons for High Availability*/
    private final static int numAutodiscHA = 3;
    /** Number of the first port for the unicast daemons for High Availability*/
    private final static int autodiscPortHA = 40300;
    private final static SubnetAddress SUBNET_ADDRESS_HA = InetUtil.getDefaultSubnet();
    private final static String ucastIpHA = SUBNET_ADDRESS_HA.getIpAddres().getHostAddress();

    /**
     * Get a reference to the enabledPublicationsInfo structure inside the publicationsManager
     * @param publicationsManager publicationsManager with the reference
     * @return the searched reference
     */
    private NativeArraySet<PublicationInfo> getEnabledPublicationsInfoReference(PublicationsManager publicationsManager)
            throws NoSuchFieldException, IllegalAccessException
    {
        Field enabledPublicationsInfoField = publicationsManager.getClass().getDeclaredField("enabledPublicationsInfo");
        enabledPublicationsInfoField.setAccessible(true);
        return (NativeArraySet<PublicationInfo>)enabledPublicationsInfoField.get(publicationsManager);
    }

    /**
     * Get a reference to the publicationInfoArray structure inside the publicationsManager
     * @param publicationsManager publicationsManager with the reference
     * @return the searched reference
     */
    private PublicationInfo[] getPublicationInfoArrayReference(PublicationsManager publicationsManager)
            throws NoSuchFieldException, IllegalAccessException
    {
        Field publicationsInfoArrayField = publicationsManager.getClass().getDeclaredField("publicationsInfoArray");
        publicationsInfoArrayField.setAccessible(true);
        return (PublicationInfo[])publicationsInfoArrayField.get(publicationsManager);
    }

    /**
     * Get a reference to the publicationsInfoByUUID structure inside the publicationsManager
     * @param publicationsManager publicationsManager with the reference
     * @return the searched reference
     */
    private Map<UUID, PublicationInfo> getPublicationsInfoByUUIDReference(PublicationsManager publicationsManager)
            throws NoSuchFieldException, IllegalAccessException
    {
        Field publicationsInfoByUUIDField = publicationsManager.getClass().getDeclaredField("publicationsInfoByUUID");
        publicationsInfoByUUIDField.setAccessible(true);
        return (Map<UUID, PublicationInfo>)publicationsInfoByUUIDField.get(publicationsManager);
    }

    /**
     * Method to create an array with the corresponding AutoDiscDaemonServerInfo, to simulate the
     * entry from the unicast daemons
     * @return Array with the servers discovery information
     */
    private AutoDiscDaemonServerInfo[] createAutoDiscDaemonServerInfoArray()
    {
        AutoDiscDaemonServerInfo[] autoDiscDaemonServerInfos = new AutoDiscDaemonServerInfo[numAutodiscHA];
        for (int i = 0; i < numAutodiscHA; i++)
        {
            //Inserts the discovery info for each unicast daemon
            autoDiscDaemonServerInfos[i] = new AutoDiscDaemonServerInfo(
                    UUID.randomUUID(),
                    InetUtil.convertIpAddressToInt(ucastIpHA),
                    autodiscPortHA+i,
                    null
            );

        }
        return autoDiscDaemonServerInfos;
    }

    /**
     * Create a list with all the IPs and Ports of the daemon discovery configured in this test
     * @return the list with address and ips
     */
    private List<UnicastInfo> createUnicastInfoArray()
    {
        List<UnicastInfo> list = new ArrayList<>();
        for (int i = 0; i < numAutodiscHA; i++)
        {
            list.add(new UnicastInfo(ucastIpHA, autodiscPortHA+i));
        }
        return list;
    }
    /**
     * Create the configuration for all the unicast daemon configured in this test
     * @return AutoDiscoveryConfig
     * @throws VegaException VegaException
     */
    private AutoDiscoveryConfig createAutoDiscoveryConfig() throws VegaException
    {
        AutoDiscoveryConfig autoDiscoveryConfig = null;

            autoDiscoveryConfig = AutoDiscoveryConfig.builder()
                    .autoDiscoType(AutoDiscoType.UNICAST_DAEMON)
                    .unicastInfoArray(createUnicastInfoArray())
                    .unicastResolverRcvPortMin(35012)
                    .unicastResolverRcvPortMax(35013)
                    .build();

        autoDiscoveryConfig.completeAndValidateConfig();
        return autoDiscoveryConfig;
    }

    /**
     * Create the PublicationsManager
     * @return PublicationsManager
     * @throws VegaException VegaException
     */
    private PublicationsManager createPublication() throws VegaException
    {
        final AutoDiscoveryConfig config = createAutoDiscoveryConfig();
        final Aeron aeron = PowerMock.createNiceMock(Aeron.class);
        final ConcurrentPublication publication = EasyMock.createNiceMock(ConcurrentPublication.class);
        EasyMock.expect(aeron.addPublication(EasyMock.anyObject(), EasyMock.anyInt()))
                .andReturn(publication).anyTimes();
        EasyMock.replay(publication);
        PowerMock.replayAll(aeron);

        PublicationsManager publicationsManager = new PublicationsManager(aeron, config);
        return publicationsManager;
    }

    /**
     * Test the PublicationsManagerConstructor
     * @throws VegaException VegaException
     */
    @Test
    public void PublicationsManagerConstructorTest() throws VegaException, NoSuchFieldException, IllegalAccessException
    {
        PublicationsManager publicationsManager = createPublication();

        //Assertions
        PublicationInfo[] publicationsInfoArray = getPublicationInfoArrayReference(publicationsManager);
        assertEquals(numAutodiscHA, publicationsInfoArray.length);

        //Assert that publicationsInfoArray contains all the elements, and all ones are correct
        Arrays.stream(publicationsInfoArray).forEach(pi-> {
            assertFalse(pi.getEnabled());
            assertNotNull(pi.getPublication());
            assertEquals(ucastIpHA, InetUtil.convertIntToIpAddress(pi.getUnicastResolverServerIp()));
            assertTrue(pi.getUnicastResolverServerPort() >= autodiscPortHA);
            assertTrue(pi.getUnicastResolverServerPort() < numAutodiscHA + autodiscPortHA);
        });

        //Assert that enabledPublicationsInfo has all the elements
        NativeArraySet<PublicationInfo> enabledPublicationsInfo = getEnabledPublicationsInfoReference(publicationsManager);
        assertEquals(0, enabledPublicationsInfo.getNumElements());

        Map<UUID, PublicationInfo> publicationsInfoByUUIDReference = getPublicationsInfoByUUIDReference(publicationsManager);
        assertEquals(0, publicationsInfoByUUIDReference.size());
    }
    /**
     * Test that the method returns null when all the publications are disabled
     * This test does not assert that the publications are randomly selected, but asserts that all
     * the publications are used (for load balancing)
     * @throws VegaException
     */
    @Test
    public void getRandomPublicationInfoWhenAllDisabledTest() throws VegaException
    {
        PublicationsManager publicationsManager = createPublication();

        //Create an array with random publications
        PublicationInfo publicationInfo[] = new PublicationInfo[10*numAutodiscHA];
        for (int i = 0; i < publicationInfo.length; i++)
        {
            publicationInfo[i] = publicationsManager.getRandomPublicationInfo();
        }

        //Assert that all the publicationInfo are null
        Arrays.stream(publicationInfo).forEach(pi -> assertNull(pi));
    }
    /**
     * Test that the method returns all the publications
     * This test does not assert that the publications are randomly selected, but asserts that all
     * the publications are used (for load balancing)
     * @throws VegaException
     */
    @Test
    public void getRandomPublicationInfoTest() throws VegaException
    {
        PublicationsManager publicationsManager = createPublication();

        //Mocks for AutoDiscDaemonServerInfo to enable publications
        final AutoDiscDaemonServerInfo[] autoDiscDaemonServerInfos = createAutoDiscDaemonServerInfoArray();

        //Simulate to enable all publications
        Arrays.stream(autoDiscDaemonServerInfos).forEach(publicationsManager::enablePublication);

        //Create an array with random publications
        PublicationInfo publicationInfo[] = new PublicationInfo[10*numAutodiscHA];
        for (int i = 0; i < publicationInfo.length; i++)
        {
            publicationInfo[i] = publicationsManager.getRandomPublicationInfo();
        }

        //Assert that all the publicationInfo are correct
        Arrays.stream(publicationInfo).forEach(pi -> {
            assertTrue(pi.getEnabled());
            assertNotNull(pi.getPublication());
            assertEquals(ucastIpHA, InetUtil.convertIntToIpAddress(pi.getUnicastResolverServerIp()));
            assertTrue(pi.getUnicastResolverServerPort() >= autodiscPortHA);
            assertTrue(pi.getUnicastResolverServerPort() < numAutodiscHA + autodiscPortHA);
        });

        //Create a set with all the publications
        Set<PublicationInfo> publicationInfoSet = new HashSet<>(Arrays.asList(publicationInfo));

        //Assert that all the unicast info has been used, if not, the get is not random!
        assertEquals(numAutodiscHA, publicationInfoSet.size());
    }

    /**
     * Method to test that the publications are enabled and disabled correctly
     * @throws VegaException VegaException
     * @throws NoSuchFieldException NoSuchFieldException
     * @throws IllegalAccessException IllegalAccessException
     */
    @Test
    public void enableDisablePublicationTest() throws VegaException, NoSuchFieldException, IllegalAccessException
    {
        PublicationsManager publicationsManager = createPublication();

        //Create an array with the server discovery info to simulate unicast daemons entries
        AutoDiscDaemonServerInfo[] autoDiscDaemonServerInfos = createAutoDiscDaemonServerInfoArray();

        //Get references for the structures
        Map<UUID, PublicationInfo> publicationsInfoByUUID = getPublicationsInfoByUUIDReference(publicationsManager);
        NativeArraySet<PublicationInfo> enabledPublicationsInfo = getEnabledPublicationsInfoReference(publicationsManager);

        //Case 1
        //Enable ALL the publications
        Arrays.stream(autoDiscDaemonServerInfos).forEach(publicationsManager::enablePublication);

        //The first time a unicast discovery msg is received, the server data is inserted into
        //the map publicationsInfoByUUID.
        assertEquals(numAutodiscHA, publicationsInfoByUUID.size());

        //And all the daemons are enabled
        assertEquals(numAutodiscHA, enabledPublicationsInfo.getNumElements());

        //Case 2
        //Disable all the publications
        Arrays.stream(autoDiscDaemonServerInfos).forEach(publicationsManager::disablePublication);

        //Assert that all the publications are disabled into the publicationsInfoByUUID
        publicationsInfoByUUID.values().stream().forEach(publicationInfo -> assertFalse(publicationInfo.getEnabled()));

        //All the daemons are disabled, so the enabledPublicationsInfo is empty
        assertEquals(0, enabledPublicationsInfo.getNumElements());

        //Case 3
        //Enable one element, the position 0 (the order element added is not important)
        publicationsManager.enablePublication(autoDiscDaemonServerInfos[0]);

        //Assert that is enabled
        assertTrue(publicationsInfoByUUID.get(autoDiscDaemonServerInfos[0].getUniqueId()).getEnabled());
        assertEquals(1, enabledPublicationsInfo.getNumElements());

        //enabledPublicationsInfo has only one element, so this is the returned element with getRandomElement
        assertEquals(autoDiscDaemonServerInfos[0].getUniqueId(), enabledPublicationsInfo.getRandomElement().getUniqueId());

        //Case 4
        //Enable another element, the position 1
        publicationsManager.enablePublication(autoDiscDaemonServerInfos[1]);

        //Assert that is enabled
        assertTrue(publicationsInfoByUUID.get(autoDiscDaemonServerInfos[1].getUniqueId()).getEnabled());
        assertEquals(2, enabledPublicationsInfo.getNumElements());

        //Case 5
        //Disable one element, the position 0 (the order element disabled is not important)
        publicationsManager.disablePublication(autoDiscDaemonServerInfos[0]);

        //Assert that is disabled
        assertFalse(publicationsInfoByUUID.get(autoDiscDaemonServerInfos[0].getUniqueId()).getEnabled());
        assertEquals(1, enabledPublicationsInfo.getNumElements());

        //enabledPublicationsInfo has only one element, and is not the deleted one
        assertNotEquals(autoDiscDaemonServerInfos[0].getUniqueId(), enabledPublicationsInfo.getRandomElement().getUniqueId());

        //Case 6
        //Disable one element, the position 1 (the order element disabled is not important)
        publicationsManager.disablePublication(autoDiscDaemonServerInfos[1]);

        //Assert that is disabled and all the publications are disabled
        assertFalse(publicationsInfoByUUID.get(autoDiscDaemonServerInfos[1].getUniqueId()).getEnabled());
        assertEquals(0, enabledPublicationsInfo.getNumElements());
    }

    /**
     * Test the hasEnabledPublications
     * @throws VegaException VegaException
     */
    @Test
    public void hasEnabledPublicationsTest() throws VegaException
    {
        PublicationsManager publicationsManager = createPublication();

        //At initialization time, all the publications are enabled
        assertFalse(publicationsManager.hasEnabledPublications());

        //Create an array with the server discovery info to simulate unicast daemons entries
        AutoDiscDaemonServerInfo[] autoDiscDaemonServerInfos = createAutoDiscDaemonServerInfoArray();

        //Enable all the publications
        Arrays.stream(autoDiscDaemonServerInfos).forEach(publicationsManager::enablePublication);

        assertTrue(publicationsManager.hasEnabledPublications());

        //Disable all the publications
        Arrays.stream(autoDiscDaemonServerInfos).forEach(publicationsManager::disablePublication);

        assertFalse(publicationsManager.hasEnabledPublications());
    }

    /**
     * Method to test that the daemons that are reset and create a new UUID are deleted from the publicationsInfoByUUID
     * @throws VegaException
     */
    @Test
    public void checkOldDaemonServerInfoTest() throws VegaException, NoSuchFieldException, IllegalAccessException
    {

        PublicationsManager publicationsManager = createPublication();

        //Get references for the structures
        Map<UUID, PublicationInfo> publicationsInfoByUUID = getPublicationsInfoByUUIDReference(publicationsManager);

        //Create an array with the server discovery info to simulate unicast daemons entries
        AutoDiscDaemonServerInfo[] autoDiscDaemonServerInfos = createAutoDiscDaemonServerInfoArray();

        //Enable all the publications
        Arrays.stream(autoDiscDaemonServerInfos).forEach(publicationsManager::enablePublication);

        //Assert that publicationsInfoByUUID contains all the daemons
        assertEquals(numAutodiscHA, publicationsInfoByUUID.size());

        //Simulate a discovery unicast daemon reset => A new change autoDiscDaemonServerInfo with the new UUID is
        // received, so enable it
        Field uniqueId = autoDiscDaemonServerInfos[0].getClass().getDeclaredField("uniqueId");
        uniqueId.setAccessible(true);
        uniqueId.set(autoDiscDaemonServerInfos[0], UUID.randomUUID());
        publicationsManager.enablePublication(autoDiscDaemonServerInfos[0]);

        //Assert that the new server has been inserted into the publicationsInfoByUUID Map
        //At this point, into publicationsInfoByUUID there are two keys that references to the same object
        assertEquals(numAutodiscHA+1, publicationsInfoByUUID.size());

        //Execute the cleaning
        publicationsManager.checkOldDaemonServerInfo();

        //Assert that the old element has beer deleted
        assertEquals(numAutodiscHA, publicationsInfoByUUID.size());
    }
}
