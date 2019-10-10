package com.bbva.kyof.vega.integration.autodiscovery;

import com.bbva.kyof.vega.autodiscovery.AutodiscManager;
import com.bbva.kyof.vega.autodiscovery.daemon.DaemonParameters;
import com.bbva.kyof.vega.autodiscovery.daemon.UnicastDaemon;
import com.bbva.kyof.vega.autodiscovery.exception.AutodiscException;
import com.bbva.kyof.vega.autodiscovery.publisher.AbstractAutodiscSender;
import com.bbva.kyof.vega.autodiscovery.publisher.AutodiscUnicastSender;
import com.bbva.kyof.vega.autodiscovery.publisher.PublicationsManager;
import com.bbva.kyof.vega.autodiscovery.subscriber.AbstractAutodiscReceiver;
import com.bbva.kyof.vega.autodiscovery.subscriber.AutodiscUnicastReceiver;
import com.bbva.kyof.vega.config.general.AutoDiscoType;
import com.bbva.kyof.vega.config.general.AutoDiscoveryConfig;
import com.bbva.kyof.vega.config.general.GlobalConfiguration;
import com.bbva.kyof.vega.config.general.UnicastInfo;
import com.bbva.kyof.vega.exception.VegaException;
import com.bbva.kyof.vega.msg.IRcvMessage;
import com.bbva.kyof.vega.msg.IRcvRequest;
import com.bbva.kyof.vega.msg.PublishResult;
import com.bbva.kyof.vega.protocol.IVegaInstance;
import com.bbva.kyof.vega.protocol.VegaInstance;
import com.bbva.kyof.vega.protocol.common.VegaContext;
import com.bbva.kyof.vega.protocol.common.VegaInstanceParams;
import com.bbva.kyof.vega.protocol.publisher.ITopicPublisher;
import com.bbva.kyof.vega.protocol.subscriber.ITopicSubListener;
import com.bbva.kyof.vega.util.net.InetUtil;
import com.bbva.kyof.vega.util.net.SubnetAddress;
import io.aeron.Aeron;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * This class contains all the logic for setting up the unicast daemons, publishers and subscribers for integration testing.
 *
 * This abstract class allow to generate a random number of Publishers, Subscribers and Unicast Daemons to test High Availability.
 *
 * VegaInstance is used, and this class only accept a xml configuration file.
 *
 * Because the Ip address is not known, the xml config files can not contain it. So, the xml config files contains
 * the MULTICAST configuration for autodisc_config, and in runtime, the AbstractAutodiscReceiver and AbstractAutodiscSender
 * are replaced from their multicast implementation to their unicast implementation (AutodiscUnicastReceiver and
 * AutodiscUnicastSender respectively).
 *
 */
@Slf4j
abstract class AbstractAutodiscoveryTest
{
    //TIMERS FOR TESTING  ***********************************************************************
    //Wait for all the threads up
    private static final int INIT_DURATION= 20000;
    //Time to wait for publisher and subscriber threads to finnish with stop variable
    private static final int TIME_TO_WAIT_FOR_STOPPING_PUBLISHER_SUBSCRIBER_THREADS = 5000;
    //Time to wait for the publisher & subscriber to set down the unicast daemon before to finnish the test
    private static final int TIME_TO_CLOSE_PUBLISHER_SUBSCRIBER = 3000;
    //Time to wait for the unicast daemon to get down before to finnish the test
    private static final int TIME_TO_CLOSE_UNICAST_DAEMONS = 3000;
    //Time to wait for the unicast daemon to get up
    static final int TIME_TO_OPEN_UNICAST_DAEMONS = 3000;

    //TIMER FOR SINCRONIZE PUBLISHERS, SUBSCRIBERS  *********************************************
    //Waits for initializacions before start activity on the subscriber & publisher
    private static final int TIME_TO_WAIT_DAEMON_PUBLISHER_SUBSCRIBER_THREADS_UP = 10000;
    private static final int TIME_TO_ALL_SUBSCRIBED_TO_TOPICS = 10000;
    //Time to wait between two transmisions
    private static final int TIME_BETWEEN_SENT_MESSAGES = 10;
    //Time to sleep the subscriber (if not set, it uses all cpu)
    private static final int TIME_TO_SLEEP_SUBSCRIBER_THREAD = 1;

    //TIMERS FOR DAEMONS ************************************************************************
    //Time to maintain down the daemons
    private static final int TIME_DAEMONS_DOWN = 20000;
    //Time to get up the daemons
    private static final int TIME_DAEMONS_UP = 5000;

    //Newtowk parameters
    private final SubnetAddress SUBNET_ADDRESS_HA = InetUtil.getDefaultSubnet();
    private final String ucastIpHA = SUBNET_ADDRESS_HA.getIpAddres().getHostAddress();

    private static final String PUBLISHER = "Publisher";
    private static final String SUBSCRIBER = "Subscriber";

    //The numAutodisc integer must coincide with the number of <autodisc_config> configurations inside the publisher's
    // and subscriber's xml configuration file
    final static int numAutodiscHA = 3;
    private final static int autodiscPortHA = 40300;

    //Topics number to register in
    private final static int numTopics = 10;

    private final UnicastDaemon[] autodiscDaemons = new UnicastDaemon[numAutodiscHA];
    private final Thread[] autodiscThreads = new Thread[numAutodiscHA];

    //Publishers & subscribers threads arrays
    private final int numPublishers = 1;
    private final int numSubscribers = 1;
    private final Thread[] publishersThreads = new Thread[numPublishers];
    private final Thread[] subscribersThreads = new Thread[numSubscribers];
    private final Publisher[] publishers = new Publisher[numPublishers];
    private final Subscriber[] subscribers = new Subscriber[numSubscribers];

    //stop All the publishers and subscribers
    private boolean stop = false;

    //Map to save all the messages and controls that all the sent messages are received
    private final Map<Integer, Message> messages = new ConcurrentHashMap<>();
    //Atomic integer to create an unique ID for each message
    private AtomicInteger messageId = new AtomicInteger();

    /**
     * Method to initialize all the attributes of this class to repeat a test
     */
    void initializeTestAttributes()
    {
        stop = false;
        messages.clear();
        messageId.set(0);
    }

    /*********************************************LAUNCHERS************************************************************/
    /**
     * Launch one AutodiscDaemon with the index passed by parameters
     * @param index the daemon to get up inside the autodiscDaemons array
     */
    void launchAutodiscDaemon(int index) throws AutodiscException
    {
        log.debug("Starting unicast daemon {}", index);
        int port = autodiscPortHA + index;

        DaemonParameters daemonParameters = DaemonParameters.builder().
                subnet(SUBNET_ADDRESS_HA.toString()).
                port(port).
                aeronDriverType(DaemonParameters.AeronDriverType.EMBEDDED).
                build();
        daemonParameters.completeAndValidateParameters();

        autodiscDaemons[index] = new UnicastDaemon(daemonParameters);
        autodiscThreads[index] = new Thread(autodiscDaemons[index]);
        autodiscThreads[index].start();

    }

    /**
     * Launch the unicast daemons
     * If exception is passed, the daemon of this iteration will not be get up.
     * This is because this method is used at initialization time (with
     * exception = -1 to getting up all the daemons) and at restart time (where only one daemon is up, with the
     * exception value as the index,
     * and this method have to get up all the others)
     * @param exception if the exception is with a valid index (exception < numAutodiscHA), this daemon is not get up
     *                  (autodiscDaemons array index)
     */
    private void launchAutodiscDaemons(int exception) throws AutodiscException
    {
        for(int index = 0; index < numAutodiscHA; index++)
        {
            if(index != exception)
            {
                launchAutodiscDaemon(index);
            }
        }
    }

    /**
     * Launch the publishers
     */
    private void launchPublishers() throws NoSuchFieldException, IllegalAccessException, VegaException
    {
        for(int i = 0; i < numPublishers; i++)
        {
            publishers[i] = new Publisher();
            publishersThreads[i] = new Thread(publishers[i]);
            publishersThreads[i].start();
        }
    }

    /**
     * Launch the subscribers
     */
    private void launchSubscribers() throws NoSuchFieldException, IllegalAccessException, VegaException
    {
        for(int i = 0; i < numSubscribers; i++)
        {
            subscribers[i] = new Subscriber();
            subscribersThreads[i] = new Thread(subscribers[i]);
            subscribersThreads[i].start();
        }
    }

    /**
     * Launch all the daemons, publishers and subscribers
     * @throws AutodiscException AutodiscException
     * @throws VegaException VegaException
     * @throws NoSuchFieldException NoSuchFieldException
     * @throws IllegalAccessException IllegalAccessException
     */
    void initializeAllDaemonsAndClients()
            throws AutodiscException, VegaException, NoSuchFieldException, IllegalAccessException, InterruptedException
    {
        //Pass -1 as parameter to start all the unicast daemons
        launchAutodiscDaemons(-1);
        launchSubscribers();
        launchPublishers();
        //Wait for all the threads up
        Thread.sleep(INIT_DURATION);
    }

    /**
     * Launch all the daemons, publishers and subscribers
     * @throws AutodiscException AutodiscException
     * @throws VegaException VegaException
     * @throws NoSuchFieldException NoSuchFieldException
     * @throws IllegalAccessException IllegalAccessException
     */
    void initializeOneDaemonAndAllClients(int daemonIndex)
            throws AutodiscException, VegaException, NoSuchFieldException, IllegalAccessException, InterruptedException
    {
        //Pass -1 as parameter to start all the unicast daemons
        launchAutodiscDaemon(daemonIndex);
        launchSubscribers();
        launchPublishers();
        //Wait for all the threads up
        Thread.sleep(INIT_DURATION);
    }

    /**
     * Close all the clients
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    void closeAllClients() throws IOException, InterruptedException {
        for(int i = 0; i < numPublishers; i++)
        {
            publishers[i].close();
        }
        for(int i = 0; i < numSubscribers; i++)
        {
            subscribers[i].close();
        }
        Thread.sleep(TIME_TO_CLOSE_PUBLISHER_SUBSCRIBER);
    }

    /**
     * Close the daemon with index passed as param
     * @param index the Unicast Daemon to close
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    public void closeOneDaemon(int index) throws IOException, InterruptedException {
        autodiscDaemons[index].cleanUp();
        Thread.sleep(TIME_TO_CLOSE_UNICAST_DAEMONS);
    }

    /**
     * Close all the daemons and clients
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    void closeAllDaemonsAndClients() throws IOException, InterruptedException {
        closeAllClients();
        for (int i = 0; i < numAutodiscHA; i++)
        {
            autodiscDaemons[i].cleanUp();
        }
        Thread.sleep(TIME_TO_CLOSE_UNICAST_DAEMONS);
    }

    /**
     * Stop the test
     * @throws InterruptedException
     */
    void stopTest() throws InterruptedException
    {
        stop = true;
        //Time to wait for publisher and subscriber threads to finnish with stop variable
        Thread.sleep(TIME_TO_WAIT_FOR_STOPPING_PUBLISHER_SUBSCRIBER_THREADS);
        log.debug("Ending...\n\n");
    }

    /**
     * Check that all the messages sent was received
     */
    void assertNotMessagesLost()
    {
        //first check using aeron responses
        long totalSentMessages = Arrays.stream(publishers).mapToInt(p -> p.getSentMsgs().get()).reduce(0, Integer::sum);
        long totalErrorMessages = Arrays.stream(publishers).map(p -> p.getErrorMsgs().get()).reduce(0, Integer::sum);
        long totalReceivedMessages = Arrays.stream(subscribers).map(s -> s.getReceivedMsgs().get()).reduce(0, Integer::sum);
        log.debug("Results: totalSentMessages={} totalReceivedMessages={} totalErrorMessages={}\n",totalSentMessages, totalReceivedMessages, totalErrorMessages);
        assertEquals(totalSentMessages, totalReceivedMessages);
        assertEquals(0, totalErrorMessages);
        //second check using time of request and responses
        long errors = messages.values().stream().filter(m -> m.getTimeRx()==0).count();
        assertEquals(0l, errors);
    }

    /**
     * Forces all the unicast daemons except one (selected randomly) to get down
     * It finds that the client change the unicast daemon that uses to publicate the topics
     * Search the comment "Returning a new Random publication to unicast discovery daemon server with uuid" in the logs
     * to see the moment it happends
     * @return The unicast daemon that was not restarted
     * @throws InterruptedException InterruptedException
     * @throws AutodiscException AutodiscException
     */
    int restartAlmostAllDaemons() throws InterruptedException, AutodiscException
    {
        int daemon = new Random().nextInt(numAutodiscHA);
        restartAllDaemonsExceptOne(daemon);
        return daemon;
    }

    /**
     * Forces all the unicast daemons except one (passed by parameter) to get down
     * It finds that the client change the unicast daemon that uses to publicate the topics
     * Search the comment "Returning a new Random publication to unicast discovery daemon server with uuid" in the logs
     * to see the moment it happends
     * @param daemon The index of the daemon that will not become reset
     * @return The unicast daemon that was not restarted
     * @throws InterruptedException InterruptedException
     * @throws AutodiscException AutodiscException
     */
    void restartAllDaemonsExceptOne(int daemon) throws InterruptedException, AutodiscException
    {
        log.info("Stopping all daemons except {}", daemon);
        for (int i = 0; i < numAutodiscHA; i++)
        {
            if(i != daemon)
            {
                log.info("Stopping daemon {}", i);
                autodiscDaemons[i].cleanUp();
            }
        }
        Thread.sleep(TIME_DAEMONS_DOWN);
        //Pass daemon as argument to start all the unicast daemons except the one that is up
        launchAutodiscDaemons(daemon);
        Thread.sleep(TIME_DAEMONS_UP);
    }

    /*********************************************PUBLISHER && SUBSCRIBER COMMON METHODS*******************************/
    /**
     * Using reflection, take the object field from an origin
     * @param origin the object that contains the searched attribute
     * @param fieldName the name of the searched atribute
     * @return the object searched
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    private static Object getObjectByReflection(Object origin, String fieldName)
            throws NoSuchFieldException, IllegalAccessException
    {
        Field field = origin.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(origin);
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
     * @throws VegaException
     */
    private AutoDiscoveryConfig createAutoDiscoveryConfig(String type) throws VegaException
    {
        AutoDiscoveryConfig autoDiscoveryConfig = null;
        if(PUBLISHER.equals(type)) {
            autoDiscoveryConfig = AutoDiscoveryConfig.builder()
                    .autoDiscoType(AutoDiscoType.UNICAST_DAEMON)
                    .unicastInfoArray(createUnicastInfoArray())
                    .unicastResolverRcvPortMin(35012)
                    .unicastResolverRcvPortMax(35013)
                    .build();
        }
        if(SUBSCRIBER.equals(type)) {
            autoDiscoveryConfig = AutoDiscoveryConfig.builder()
                    .autoDiscoType(AutoDiscoType.UNICAST_DAEMON)
                    .unicastInfoArray(createUnicastInfoArray())
                    .unicastResolverRcvPortMin(35022)
                    .unicastResolverRcvPortMax(35023)
                    .build();
        }
        autoDiscoveryConfig.completeAndValidateConfig();
        return autoDiscoveryConfig;
    }

    /**
     * VegaInstance is used, and this class only accept a xml configuration file.
     * Because the Ip address is not known, the xml config files can not contain it. So, the xml config files contains
     * the MULTICAST configuration for autodisc_config, and in runtime, the AbstractAutodiscReceiver and AbstractAutodiscSender
     * are replaced from their multicast implementation to their unicast implementation (AutodiscUnicastReceiver and
     * AutodiscUnicastSender respectively).
     *
     * This method uses reflection to substitute the multicast AbstractAutodiscReceiver and AbstractAutodiscSender
     * configured by the xml configuration file to theis unicast implementation
     * @param instance
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     * @throws VegaException
     */
    public void changePublicationsManager(final IVegaInstance instance, String type)
            throws NoSuchFieldException, IllegalAccessException, VegaException
    {
        //Get necessary instances
        VegaContext vegaContext = (VegaContext)getObjectByReflection(instance, "vegaContext");
        Aeron aeron = (Aeron)getObjectByReflection(vegaContext, "aeron");
        GlobalConfiguration globalConfiguration = (GlobalConfiguration) getObjectByReflection(vegaContext, "instanceConfig");

        //Replace the configuration from multicast to unicast
        Field autodiscConfigField = globalConfiguration.getClass().getDeclaredField("autodiscConfig");
        autodiscConfigField.setAccessible(true);
        AutoDiscoveryConfig autoDiscoveryConfig = createAutoDiscoveryConfig(type);
        autodiscConfigField.set(globalConfiguration, autoDiscoveryConfig);

        //Create the publicationsManager for unicast, to use in the AutodiscUnicastReceiver and AutodiscUnicastSender
        PublicationsManager publicationsManager = new PublicationsManager(aeron, autoDiscoveryConfig);
        AutodiscManager autodiscoveryManager = (AutodiscManager)getObjectByReflection(vegaContext, "autodiscoveryManager");

        //Create the AutodiscUnicastReceiver and AutodiscUnicastSender
        AbstractAutodiscReceiver autodiscSub = new AutodiscUnicastReceiver(instance.getInstanceId(), aeron, autoDiscoveryConfig, autodiscoveryManager, publicationsManager);
        AbstractAutodiscSender autodiscPub = new AutodiscUnicastSender(aeron, autoDiscoveryConfig, ((AutodiscUnicastReceiver)autodiscSub).getDaemonClientInfo(), publicationsManager);

        //Close the multicast receiver & sender, they will not been used
        log.debug("\n");
        log.debug("Closing multicast receiver & sender");
        AbstractAutodiscReceiver abstractAutodiscReceiver = (AbstractAutodiscReceiver) getObjectByReflection(autodiscoveryManager, "autodiscSub");
        abstractAutodiscReceiver.close();
        AbstractAutodiscSender abstractAutodiscSender = (AbstractAutodiscSender) getObjectByReflection(autodiscoveryManager, "autodiscPub");
        abstractAutodiscSender.close();
        log.debug("Closed multicast receiver & sender\n");

        //Replace the implementations from multicast to unicast with AutodiscUnicastReceiver and AutodiscUnicastSender
        Field autodiscSubField = autodiscoveryManager.getClass().getDeclaredField("autodiscSub");
        Field autodiscPubField = autodiscoveryManager.getClass().getDeclaredField("autodiscPub");
        autodiscSubField.setAccessible(true);
        autodiscPubField.setAccessible(true);
        autodiscSubField.set(autodiscoveryManager, autodiscSub);
        autodiscPubField.set(autodiscoveryManager, autodiscPub);

        //At this moment, the IVegaInstance instance contains the unicast dyscovery mechanism enabled
    }

    /*********************************************Publisher************************************************************/
    /**
     * Class with a Publisher
     */
    class Publisher implements Runnable
    {
        private final String configFile;
        private final VegaInstanceParams vegaInstanceParams;
        private final IVegaInstance instance;
        @Getter private AtomicInteger sentMsgs = new AtomicInteger();
        @Getter private AtomicInteger errorMsgs = new AtomicInteger();

        /**
         * Constructor to create the Publisher
         * @throws VegaException VegaException
         * @throws NoSuchFieldException NoSuchFieldException
         * @throws IllegalAccessException IllegalAccessException
         */
        public Publisher() throws VegaException, NoSuchFieldException, IllegalAccessException
        {
            super();

            configFile = Publisher.class.getClassLoader().getResource("integrationConfig/publicationConfig.xml").getPath();

            vegaInstanceParams  = VegaInstanceParams.builder().
                    instanceName("PublisherInstance").
                    configurationFile(configFile).build();
            instance = VegaInstance.createNewInstance(vegaInstanceParams);

            //Change AbstractAutodiscReceiver and AbstractAutodiscSender from multicast form to
            // their unicast form using PublicationsManager
            changePublicationsManager(instance, PUBLISHER);
        }

        /**
         * Method to close the publisher
         * @throws IOException IOException
         */
        public void close() throws IOException {
            log.debug("Closing publisher");
            instance.close();
            log.debug("Closed tested publisher with sendedMsgs={} and errorMsgs={}",sentMsgs, errorMsgs);
        }

        /**
         * Method to send a message and save it into the messages structure
         * @param iTopicPublisherList
         */
        private void sendMsg(List<ITopicPublisher> iTopicPublisherList) {
            //Get random publisher
            ITopicPublisher topicPublisher = iTopicPublisherList.get(new Random().nextInt(numTopics));
            Integer messageIdaux = messageId.getAndIncrement();
            messages.put(messageIdaux, new Message(topicPublisher.getTopicName(), System.currentTimeMillis(), 0l));

            // Create the message to send
            UnsafeBuffer sendBuffer = new UnsafeBuffer(ByteBuffer.allocate(128));
            //sendBuffer.putInt(0, messageIdaux.intValue());
            //sendBuffer.putInt(4, 26);
            //sendBuffer.putInt(8, 07);
            //sendBuffer.putInt(12, 19);
            //sendBuffer.putInt(16, 80);
            //PublishResult result = topicPublisher.sendMsg(sendBuffer, 0, 20);
            sendBuffer.putInt(0, messageIdaux.intValue());
            PublishResult result = topicPublisher.sendMsg(sendBuffer, 0, 4);

            if (result == PublishResult.BACK_PRESSURED || result == PublishResult.UNEXPECTED_ERROR) {
                errorMsgs.incrementAndGet();
            }
            else{
                sentMsgs.incrementAndGet();
            }
        }

        @Override
        public void run()
        {
            try
            {
                //Time to get the Daemon Servers, Publisher and Subscriber up
                Thread.sleep(TIME_TO_WAIT_DAEMON_PUBLISHER_SUBSCRIBER_THREADS_UP);

                List<ITopicPublisher> iTopicPublisherList = new ArrayList<>();
                for(int i = 0; i < numTopics; i++)
                {
                    iTopicPublisherList.add( instance.createPublisher("Topic"+i) );
                }
                //Time to having all the subscribers subscribed to all the topics, before start sending messages
                Thread.sleep(TIME_TO_ALL_SUBSCRIBED_TO_TOPICS);
                while(!stop)
                {
                    sendMsg(iTopicPublisherList);
                    Thread.sleep(TIME_BETWEEN_SENT_MESSAGES);
                }
            }
            catch (InterruptedException | VegaException e)
            {
                e.printStackTrace();
            }
        }
    }

    /*********************************************Subscriber************************************************************/
    /**
     * Class to instance a Subscriber
     */
    class Subscriber implements Runnable
    {
        private final String configFile;
        private final VegaInstanceParams vegaInstanceParams;
        private final IVegaInstance instance;
        @Getter
        private AtomicInteger receivedMsgs = new AtomicInteger();

        /**
         * Create the Subscribers
         * @throws VegaException VegaException
         * @throws NoSuchFieldException NoSuchFieldException
         * @throws IllegalAccessException IllegalAccessException
         */
        public Subscriber() throws VegaException, NoSuchFieldException, IllegalAccessException
        {
            super();

            configFile = Subscriber.class.getClassLoader().getResource("integrationConfig/subscriptionConfig.xml").getPath();

            vegaInstanceParams  = VegaInstanceParams.builder().
                    instanceName("SubscriberInstance").
                    configurationFile(configFile).build();
            instance = VegaInstance.createNewInstance(vegaInstanceParams);

            //Change AbstractAutodiscReceiver and AbstractAutodiscSender from multicast form to
            // their unicast form using PublicationsManager
            changePublicationsManager(instance, SUBSCRIBER);
        }

        /**
         * Method to close the subscriber
         * @throws IOException
         */
        public void close() throws IOException {
            log.debug("Closing subscriber");
            instance.close();
            log.debug("Closed tested subscriber with receivedMsgs={}", receivedMsgs);
        }

        @Override
        public void run()
        {
            // Create a listener
            ITopicSubListener listener = new ITopicSubListener()
            {
                @Override
                public void onMessageReceived(IRcvMessage receivedMessage)
                {
                    //log.info("RECIBIDO MSJ: "+receivedMessage.getTopicName()+" -> "+receivedMessage.getContents());
                    receivedMsgs.incrementAndGet();

                    // Get the binary length of the message
                    int messageLentgh = receivedMessage.getContentLength();
                    // Get the offset of the message in the buffer
                    int msgOffset = receivedMessage.getContentOffset();
                    // Create a new byte buffer to copy the contents into
                    ByteBuffer byteBufferContents = ByteBuffer.allocate(messageLentgh);
                    //Sets the order to the pc's one (LITTLE_ENDIAN or BIG_ENDIAN)
                    //byteBufferContents.order(ByteOrder.LITTLE_ENDIAN);
                    byteBufferContents.order(ByteOrder.nativeOrder());
                    // Copy the contents into the created buffer
                    receivedMessage.getContents().getBytes(msgOffset, byteBufferContents, messageLentgh);
                    /*log.info("Received: "+receivedMessage
                            + " [0]=" + byteBufferContents.getInt(0)
                            + " [4]=" + byteBufferContents.getInt(4)
                            + " [8]=" + byteBufferContents.getInt(8)
                            + " [12]=" + byteBufferContents.getInt(12)
                            + " [16]=" + byteBufferContents.getInt(16));*/
                    // Process the contents, it can be processes on a separate thread also without promotions
                    //Integer receivedId = byteBufferContents.getInt(receivedMessage.getContentOffset());
                    Integer receivedId = byteBufferContents.getInt(0);
                    messages.get(receivedId).setTimeRx(System.currentTimeMillis());
                }

                @Override
                public void onRequestReceived(IRcvRequest receivedRequest) {
                }
            };

            try
            {
                //Time to get the Daemon Servers, Publisher and Subscriber up
                Thread.sleep(TIME_TO_WAIT_DAEMON_PUBLISHER_SUBSCRIBER_THREADS_UP);

                for (int i = 0; i < numTopics; i++ ) {
                    // Subscribe to topic
                    try {
                        instance.subscribeToTopic("Topic"+i, listener);
                    } catch (VegaException e) {
                        e.printStackTrace();
                    }
                }

                while(!stop)
                {
                    Thread.sleep(TIME_TO_SLEEP_SUBSCRIBER_THREAD);
                }
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }

    /**
     * Class that saves the state of a Message
     */
    @Data
    @AllArgsConstructor
    public class Message
    {
        private String topic;
        private long timeTxVega;
        private long timeRx;
    }
}
