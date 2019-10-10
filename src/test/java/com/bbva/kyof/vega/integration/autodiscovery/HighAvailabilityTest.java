package com.bbva.kyof.vega.integration.autodiscovery;

import com.bbva.kyof.vega.autodiscovery.exception.AutodiscException;
import com.bbva.kyof.vega.exception.VegaException;
import lombok.extern.slf4j.Slf4j;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.Random;

/**
 * This test will generate a random number of Publishers, Subscribers and Unicast Daemons to test High Availability.
 *
 * To create Publisher ands Subscribers, it is necessary to create a PublicationsManager with the unicast address of
 * all the unicast daemons. All this code is in the AbstractAutodiscoveryTest
 *
 */
@Slf4j
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HighAvailabilityTest extends AbstractAutodiscoveryTest
{
	// Duration of one daemon up test
	private static final int DURATION_ONE_DAEMON_UP_TEST= 10000;
	// Number of executions of one daemon up test with a random index
	private static final int EXECUTIONS_ONE_DAEMON_UP_TEST= 5;

    /**
     * Test to send and receive all the messages with all the unicast daemons up at starting point
     * @throws InterruptedException InterruptedException
     */
    @Test
    public void highAvailability1UnicastDaemonsUpAtStartTest()
            throws InterruptedException, AutodiscException, NoSuchFieldException, IllegalAccessException, VegaException,
            IOException
    {
        //Initialize All
        log.debug("STEP 1: Starting highAvailability1UnicastDaemonsUpAtStartTest...\n\n");
        initializeAllDaemonsAndClients();

        //Restart the unicast daemons
        log.debug("STEP 2: Restarting the unicast daemons to force a daemon discovery change...\n\n");
        int notRestarted1 = restartAlmostAllDaemons();
        int notRestarted2 = restartAlmostAllDaemons();
        while(numAutodiscHA > 1 && notRestarted1 == notRestarted2)
        {
            log.debug("Repeat restart notRestarted1={} notRestarted2={}", notRestarted1, notRestarted2);
            notRestarted2 = restartAlmostAllDaemons();
        }

        //Restart all the daemons except the first one to test that the old publication works correctly
        log.debug("STEP 3: Recovering first daemon: notRestarted1={} notRestarted2={}\n\n", notRestarted1, notRestarted2);
        restartAllDaemonsExceptOne(notRestarted1);

        //Stop all threads
        log.debug("STEP 4: Finishing highAvailability1UnicastDaemonsUpAtStartTest\n\n");
        stopTest();

        //Test that all the messages sent was received
        assertNotMessagesLost();

        //Close All the resources
        closeAllDaemonsAndClients();
    }

	/**
	 * Test to send and receive all the messages with only one of the unicast daemons up
	 * @param index the index of the unicast daemon to get up
	 * @throws InterruptedException InterruptedException
	 */
    private void highAvailability2OneUnicastDaemonUpAtStartTest(int index)
			throws InterruptedException, AutodiscException, NoSuchFieldException, IllegalAccessException, VegaException,
			IOException
	{
		//Initialize only publisher and subscriber with one Unicast Daemon (with the passed index)
		log.debug("STEP: Starting highAvailability2OneUnicastDaemonUpAtStartTest with index={}\n\n", index);
		initializeOneDaemonAndAllClients(index);

		Thread.sleep(DURATION_ONE_DAEMON_UP_TEST);

		//Stop all threads
		log.debug("STEP: Finishing highAvailability2OneUnicastDaemonUpAtStartTest with index={}\n\n", index);
		stopTest();

		//Test that all the messages sent was received
		assertNotMessagesLost();

		closeAllClients();
		closeOneDaemon(index);
	}

    /**
     * Test to send and receive all the messages with only one of the unicast daemons up
	 * This test executes EXECUTIONS_ONE_DAEMON_UP_TEST times the highAvailability2OneUnicastDaemonUpAtStartTest
	 * with a random index, to test that when not all the configured daemons are up, the
	 * clients choose the correct one (all de daemons are disable by default, and only enabled when a pkg is received
	 * from the daemon server)
     * @throws InterruptedException InterruptedException
     */
    @Test
    public void highAvailability2OneRandomUnicastDaemonUpAtStartTest()
            throws InterruptedException, AutodiscException, NoSuchFieldException, IllegalAccessException, VegaException,
            IOException
    {
    	for (int i = 0; i < EXECUTIONS_ONE_DAEMON_UP_TEST; i++)
		{
			log.debug("STEP {}: Starting highAvailability2OneRandomUnicastDaemonUpAtStartTest\n\n", i);

			//Execute the test with a random index (index of numAutodiscHA)
			int index = new Random().nextInt(numAutodiscHA);
			highAvailability2OneUnicastDaemonUpAtStartTest(index);

			//Initialize the status of the Abstract class to start another test
			initializeTestAttributes();
		}

		log.debug("STEP FINAL: Finished highAvailability2OneRandomUnicastDaemonUpAtStartTest\n\n");
    }


	/**
	 * Test to send and receive all the messages with only one of the unicast daemons up
	 * This test gets up and down Unicast Daemons having always one of then down
	 * @throws InterruptedException InterruptedException
	 */
	@Test
	public void highAvailability3OneRandomUnicastDaemonUpTest()
			throws AutodiscException, VegaException, IllegalAccessException, InterruptedException, NoSuchFieldException,
			IOException
	{
		//Initialize only publisher and subscriber with the Unicast Daemon 0
		log.debug("STEP 1: Starting highAvailability3OneRandomUnicastDaemonUpTest with Daemon index=0\n\n");
		initializeOneDaemonAndAllClients(0);

		Thread.sleep(DURATION_ONE_DAEMON_UP_TEST);

		//Change the Unicast Daemon from index 0 to 1 (in autodiscDaemons Array)
		log.debug("STEP 2: highAvailability3OneRandomUnicastDaemonUpTest changing Daemon index from 0 to 1\n\n");
		//Start Unicast Daemon 1 and wait
		launchAutodiscDaemon(1);
		Thread.sleep(TIME_TO_OPEN_UNICAST_DAEMONS);
		//Stop the Unicast Daemon 0
		closeOneDaemon(0);

		Thread.sleep(DURATION_ONE_DAEMON_UP_TEST);

		//Change the Unicast Daemon from index 1 to 2 (in autodiscDaemons Array)
		log.debug("STEP 3: highAvailability3OneRandomUnicastDaemonUpTest changing Daemon index from 1 to 2\n\n");
		//Start Unicast Daemon 2 and wait
		launchAutodiscDaemon(2);
		Thread.sleep(TIME_TO_OPEN_UNICAST_DAEMONS);
		//Stop the Unicast Daemon 1
		closeOneDaemon(1);

		Thread.sleep(DURATION_ONE_DAEMON_UP_TEST);

		//Change the Unicast Daemon from index 2 to 0 (in autodiscDaemons Array)
		log.debug("STEP 4: highAvailability3OneRandomUnicastDaemonUpTest changing Daemon index from 2 to 0\n\n");
		//Start Unicast Daemon 0 and wait
		launchAutodiscDaemon(0);
		Thread.sleep(TIME_TO_OPEN_UNICAST_DAEMONS);
		//Stop the Unicast Daemon 2
		closeOneDaemon(2);

		//Stop all threads
		log.debug("STEP 5: Finishing highAvailability3OneRandomUnicastDaemonUpTest\n\n");
		stopTest();

		//Test that all the messages sent was received
		assertNotMessagesLost();

		closeAllClients();

		//Stop the daemon 0
		closeOneDaemon(0);
	}
}
