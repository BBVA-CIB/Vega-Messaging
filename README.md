# Vega-Messaging

¿What is Vega Messaging Library?

The VegaMessaging libraries offers a low latency communication mechanism based on topis.

The objective is to provide a low latency communication mechanism with similar or better performance that the best commercial solutions out there at a fraction of the cost.

In order to attain this objective we have developed a messaging library on top of Aeron to provide it with additional features.

¿What is Aeron?

Aeron is an Open Source low latency messaging library that provides basic capabilities for publication / subscription in multiple protocols.

The supported protocols are:
- Reliable UDP
- Reliable Multicast
 -IPC
 
The reliable protocols are light weight and allows for very fast transfer with minimum overhead and error control and recovery. 

There is no compromise in Aeron between latency and throughput, the library handles it automatically and there is no need to specify if we need to flush or not the messages like we do for Ultra Messaging and other mechanism.

Aeron does not support topics, request / response, auto-discovery, file configuration, or other similar features, this has motivated the creation of a library on top of Aeron in order to provide this necessary features.

¿Why Aeron?

Aeron is fully written in Java, what means we don't need to install native libraries like would need in other low latency messaging solutions.

Aeron is the fastest Open Source messaging solution on the market and even better than most commercial low latency solutions.

The usage of centralised "Aeron Media Drivers" allows a very efficient usage of resources and quick and simple communication for non-monolitic architectures where there are several applications per physical machine.

There is more information about how the Media Driver works in the "Wiki". 

Vega Messaging Library Features

- Topic based

The logical communication is based on topics instead of ip's and ports
Support for thousands of topics

- Autodiscovery

Fully auto-discovery between nodes
Automatic topic/transport match based on configuration and auto-discovery information

  Types of auto-discovery
    Based on multicast without any centralised node.
    Based on unicast using a centralised auto-discovery daemon.
    
- Communication modes
    Publication / Subscription based on topic
        Broadcast publication and subscription based on topic names
    Subscription based on topic patterns
        A regular expression can be used to receive from any topic in the system that match the expression.
    Request / Response based on topic
        Asynchronous requests with timeout
        Multiple responses allowed
        Keep track of number of responses
        Timeouts can be modified at any time to keep request alive if required
        
- Transport types
      Reliable UDP
      Reliable Multicast
      IPC

- Flow Control
      The library detects "back pressure" problems allowing the user to know when a receiver is overflow.

- Thread management and thread safety
      The public part of the library is 100% thread safe.
      The management of polling threads to receive messages or requests is transparent.

- Reuse of resources
      The library reuse all the resources when possible, allowing a very small impact in memory allocation and therefore reducing jitter due to GC issues.
      The configuration mechanism ensures that the same topic will always use the same transport configuration, this allows sharing resources between applications on the same machine through the "Media Driver".

- Configuration
    Simple XML configuration mechanism is provided in which the most basic and advanced parameters of the framework can be easily tuned.
    The same configuration file can be shared by all the applications on the same cluster, simplifying the configuration management between applications.

- Low Latency and Throughput
      The library is designed to achieve very low latency and high throughput without the need to do any fine tuning.
      
- Heartbeats and client connection control
      The library has a built-in heartbeat mechanism system to detect when a topic connection is fully available and when new clients are connected to a topic.
      The heartbeats are activated individually per topic on the topic publishers.

- Message encryption with session keys
      The library allows for message encryption using a shared session key and AES encryption.
      The session key is shared by a double handshake protocol based on Public / Private key communication between the applications.
      Security configuration is fine grained at topic level, choosing which applications can publish or subscribe in an specific topic.
      The library contains Keys generator to create the Public and Private keys for each application.
