
Frequently Asked Questions.
===========================


I already use ELK, why would I need to use LogIsland ?
------------------------------------------------------
Well, at first one could say that that both stacks are overlapping, 
but the real purpose of the LogIsland framework is the abstraction of scalability of log aggregation.

In fact if you already have an ELK stack you'll likely want to make it scale (without pain) in both volume and features ways. 
LogIsland will be used for this purpose as an EOM (Event Oriented Middleware) based on Kafka & Spark, where you can plug advanced features
with ease.

So you just have to route your logs from the Logstash (or Flume, or Collectd, ...) agents to Kafka topics and launch parsers and processors.


Do I need Hadoop to play with LogIsland ?
-----------------------------------------

No, if your goal is simply to aggregate a massive amount of logs in an Elasticsearch cluster, 
and to define complex event processing rules to generate new events you definitely don't need an Hadoop cluster. 

Kafka topics can be used as an high throughput log buffer for sliding-windows event processing. 
But if you need advanced batch analytics, it's really easy to dump your logs into an hadoop cluster to build machine learning models.


How do I make it scale ?
------------------------
LogIsland is made for scalability, it relies on Spark and Kafka which are both scalable by essence, to scale LogIsland just have to add more kafka brokers and more Spark slaves.
This is the *manual* way, but we've planned in further releases to provide auto-scaling either Docker Swarn support or Mesos Marathon.


What's the difference between Apache NIFI and LogIsland ?
---------------------------------------------------------
Apache NIFI is a powerful ETL very well suited to process incoming data such as logs file, process & enrich them and send them out to any datastore.
You can do that as well with LogIsland but LogIsland is an event oriented framework designed to process huge amount of events in a Complex Event Processing
manner not a Single Event Processing as NIFI does. **LogIsland** is not an ETL or a DataFlow, the main goal is to extract information from realtime data.

Anyway you can use Apache NIFI to process your logs and send them to Kafka in order to be processed by LogIsland


Error : realpath not found
--------------------------

If you don't have the ``realpath`` command on you system you may need to install it::

    brew install coreutils
    sudo apt-get install coreutils

How to deploy LogIsland as a Single node Docker container
----------------------------------------------------------
The easy way : you start a small Docker container with all you need inside (Elasticsearch, Kibana, Kafka, Spark, LogIsland + some usefull tools)

`Docker <https://www.docker.com>`_ is becoming an unavoidable tool to isolate a complex service component. It's easy to manage, deploy and maintain. That's why you can start right away to play with LogIsland through the Docker image provided from `Docker HUB <https://hub.docker.com/r/hurence/logisland/>`_

.. code-block:: sh

    # Get the LogIsland image
    docker pull hurence/logisland

    # Run the container
    docker run \
          -it \
          -p 80:80 \
          -p 9200-9300:9200-9300 \
          -p 5601:5601 \
          -p 2181:2181 \
          -p 9092:9092 \
          -p 9000:9000 \
          -p 4050-4060:4050-4060 \
          --name logisland \
          -h sandbox \
          hurence/logisland:latest bash

    # Connect a shell to your LogIsland container
    docker exec -ti logisland bash


How to deploy LogIsland in an Hadoop cluster ?
----------------------------------------------
When it comes to scale, you'll need a cluster. **logisland** is just a framework that facilitates running sparks jobs over Kafka topics so if you already have a cluster you just have to get the latest logisland binaries and unzip them to a edge node of your hadoop cluster.

For now Log-Island is fully compatible with HDP 2.4 but it should work well on any cluster running Kafka and Spark.
Get the latest release and build the package.

You can download the `latest release build <https://github.com/Hurence/logisland/releases/download/v0.9.5/logisland-0.9.5-bin.tar.gz>`_

.. code-block:: bash

    git clone git@github.com:Hurence/logisland.git
    cd logisland-0.9.5
    mvn clean install -DskipTests

This will produce a ``logisland-assembly/target/logisland-0.9.5-bin.tar.gz`` file that you can untar into any folder of your choice in a edge node of your cluster.



Please read this excellent article on spark long running job setup : `http://mkuthan.github.io/blog/2016/09/30/spark-streaming-on-yarn/ <http://mkuthan.github.io/blog/2016/09/30/spark-streaming-on-yarn/>`_


How can I configure Kafka to avoid irrecoverable exceptions ?
-------------------------------------------------------------
If the message must be reliable published on Kafka cluster, Kafka producer and Kafka cluster needs to be configured with care. It needs to be done independently of chosen streaming framework.

Kafka producer buffers messages in memory before sending. When our memory buffer is exhausted, Kafka producer must either stop accepting new records (block) or throw errors. By default Kafka producer blocks and this behavior is legitimate for stream processing. The processing should be delayed if Kafka producer memory buffer is full and could not accept new messages. Ensure that block.on.buffer.full Kafka producer configuration property is set.

With default configuration, when Kafka broker (leader of the partition) receive the message, store the message in memory and immediately send acknowledgment to Kafka producer. To avoid data loss the message should be replicated to at least one replica (follower). Only when the follower acknowledges the leader, the leader acknowledges the producer.

This guarantee you will get with ack=all property in Kafka producer configuration. This guarantees that the record will not be lost as long as at least one in-sync replica remains alive.

But this is not enough. The minimum number of replicas in-sync must be defined. You should configure min.insync.replicas property for every topic. I recommend to configure at least 2 in-sync replicas (leader and one follower). If you have datacenter with two zones, I also recommend to keep leader in the first zone and 2 followers in the second zone. This configuration guarantees that every message will be stored in both zones.

We are almost done with Kafka cluster configuration. When you set min.insync.replicas=2 property, the topic should be replicated with factor 2 + N. Where N is the number of brokers which could fail, and Kafka producer will still be able to publish messages to the cluster. I recommend to configure replication factor 3 for the topic (or more).

With replication factor 3, the number of brokers in the cluster should be at least 3 + M. When one or more brokers are unavailable, you will get underreplicated partitions state of the topics. With more brokers in the cluster than replication factor, you can reassign underreplicated partitions and achieve fully replicated cluster again. I recommend to build the 4 nodes cluster at least for topics with replication factor 3.

The last important Kafka cluster configuration property is unclean.leader.election.enable. It should be disabled (by default it is enabled) to avoid unrecoverable exceptions from Kafka consumer. Consider the situation when the latest committed offset is N, but after leader failure, the latest offset on the new leader is M < N. M < N because the new leader was elected from the lagging follower (not in-sync replica). When the streaming engine ask for data from offset N using Kafka consumer, it will get an exception because the offset N does not exist yet. Someone will have to fix offsets manually.

So the minimal recommended Kafka setup for reliable message processing is:

.. code-block:: bash

    4 nodes in the cluster
    unclean.leader.election.enable=false in the brokers configuration
    replication factor for the topics – 3
    min.insync.replicas=2 property in topic configuration
    ack=all property in the producer configuration
    block.on.buffer.full=true property in the producer configuration

With the above setup your configuration should be resistant to single broker failure, and Kafka consumers will survive new leader election.

You could also take look at replica.lag.max.messages and replica.lag.time.max.ms properties for tuning when the follower is removed from ISR by the leader. But this is out of this blog post scope.



How to purge a Kafka queue ?
----------------------------
Temporarily update the retention time on the topic to one second:

.. code-block:: bash

    kafka-topics.sh --zookeeper localhost:13003 --alter --topic MyTopic --config retention.ms=1000

then wait for the purge to take effect (about one minute). Once purged, restore the previous retention.ms value.


You can also try to delete the topic :

add one line to server.properties file under config folder:

.. code-block:: bash

    delete.topic.enable=true

then, you can run this command:

.. code-block:: bash

    bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic test