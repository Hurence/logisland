Bro/Logisland integration - Indexing Bro events
===============================================

Bro an Logisland
----------------

`Bro <https://www.bro.org>`_ is a Network IDS
(`Intrusion Detection System <https://en.wikipedia.org/wiki/Intrusion_detection_system>`_) that
can be deployed to monitor your infrastructure. Bro listens to the packets of your network
and generates high level events from them. It can for instance generate an event each time there is a
connection, a file transfer, a DNS query...anything that can be deduced from packet analysis.

Through its out-of-the-box Bro processor, Logisland integrates with Bro and is able to receive and handle Bro events and notices coming from Bro.
By analyzing those events with Logisland, you may do some correlations and for instance generate some higher level alarms or do whatever
you want, in a scalable manner, like monitoring a huge infrastructure with hundreds of machines.

Bro comes with a scripting language that allows to also generate some higher level events from other events correlations.
Bro calls such events 'notices'. For instance a notice can be generated when a user or bot tries to guess a password with brute forcing.
Logisland is also able to receive and handle those notices.

For the purpose of this tutorial, we will show you how to receive Bro events and notices in Logisland and how to index them in
ElasticSearch for network audit purpose. But you can imagine to plug any Logisland processors after the Bro processor to build
your own monitoring system or any other application based on Bro events and notices handling.

Tutorial environment
--------------------

This tutorial will give you a better understanding of how Bro and Logisland integrate together.

We will start two Docker containers:

- 1 container hosting all the LogIsland services
- 1 container hosting Bro pre-loaded with Bro-Kafka plugin

We will launch two streaming processes and configure Bro to send events and notices to the Logisland system so that they
are indexed in ElasticSearch.

It is important to understand that in a production environment Bro would be installed on machines where he is relevant for
your infrastructure and will be configured to remotely point to the Logisland service (Kafka). But for easiness of this tutorial, we
provide you with an easy mean of generating Bro events through our Bro Docker image.

This tutorial will guide you through the process of configuring Logisland for treating Bro events, and configuring Bro of the
second container to send the events and notices to the Logisland service in the first container.

.. note::

    You can download the `latest release <https://github.com/Hurence/logisland/releases>`_ of Logisland and the `YAML configuration file <https://github.com/Hurence/logisland/blob/master/logisland-framework/logisland-resources/src/main/resources/conf/index-bro-events.yml>`_
    for this tutorial which can be also found under `$LOGISLAND_HOME/conf` directory in the Logsiland container.

1. Start the Docker container with LogIsland
--------------------------------------------

LogIsland is packaged as a Docker image that you can `build yourself <https://github.com/Hurence/logisland/tree/master/logisland-docker#build-your-own>`_ or pull from Docker Hub.
The docker image is built from a CentOs image with the following components already installed (among some others not useful for this tutorial):

- Kafka
- Spark
- Elasticsearch
- LogIsland

Pull the image from Docker Repository (it may take some time)

.. code-block:: sh

    docker pull hurence/logisland

You should be aware that this Docker container is quite eager in RAM and will need at leat 8G of memory to run smoothly.
Now run the container

.. code-block:: sh

    # run container
    docker run \
        -it \
        -p 80:80 \
        -p 8080:8080 \
        -p 3000:3000 \
        -p 9200-9300:9200-9300 \
        -p 5601:5601 \
        -p 2181:2181 \
        -p 9092:9092 \
        -p 9000:9000 \
        -p 4050-4060:4050-4060 \
        --name logisland \
        -h sandbox \
        hurence/logisland bash

    # get container ip
    docker inspect logisland | grep IPAddress

    # or if your are on mac os
    docker-machine ip default

You should add an entry for **sandbox** (with the container ip) in your ``/etc/hosts`` as it will be easier to access to all web services in Logisland running container.
Or you can use 'localhost' instead of 'sandbox' where applicable.

.. note::

    If you have your own Spark and Kafka cluster, you can download the `latest release <https://github.com/Hurence/logisland/releases>`_ and unzip on an edge node.

2. Transform Bro events into Logisland records
----------------------------------------------

For this tutorial we will receive Bro events and notices and send them to Elastiscearch. The configuration file for this tutorial is
already present in the container at ``$LOGISLAND_HOME/conf/index-bro-events.yml`` and its content can be viewed
`here <https://github.com/Hurence/logisland/blob/master/logisland-framework/logisland-resources/src/main/resources/conf/index-bro-events.yml>`_
. Within the following steps, we will go through this configuration file and detail the sections and what they do.

Connect a shell to your Logisland container to launch a Logisland instance with the following streaming jobs:

.. code-block:: sh

    docker exec -ti logisland bash
    cd $LOGISLAND_HOME
    bin/logisland.sh --conf conf/index-bro-events.yml
    
.. note::

    Logisland is now started. If you want to go straight forward and do not care for the moment about the configuration file details, you can now skip the
    following sections and directly go to the :ref:`StartBroContainer` section.   

Setup Spark/Kafka streaming engine
__________________________________

An Engine is needed to handle the stream processing. The ``conf/index-bro-events.yml`` configuration file defines a stream processing job setup.
The first section configures the Spark engine, we will use a `KafkaStreamProcessingEngine <../plugins.html#kafkastreamprocessingengine>`_

.. code-block:: yaml


    engine:
      component: com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine
      type: engine
      documentation: Index Bro events with LogIsland
      configuration:
        spark.app.name: IndexBroEventsDemo
        spark.master: local[4]
        spark.driver.memory: 1G
        spark.driver.cores: 1
        spark.executor.memory: 2G
        spark.executor.instances: 4
        spark.executor.cores: 2
        spark.yarn.queue: default
        spark.yarn.maxAppAttempts: 4
        spark.yarn.am.attemptFailuresValidityInterval: 1h
        spark.yarn.max.executor.failures: 20
        spark.yarn.executor.failuresValidityInterval: 1h
        spark.task.maxFailures: 8
        spark.serializer: org.apache.spark.serializer.KryoSerializer
        spark.streaming.batchDuration: 4000
        spark.streaming.backpressure.enabled: false
        spark.streaming.unpersist: false
        spark.streaming.blockInterval: 500
        spark.streaming.kafka.maxRatePerPartition: 3000
        spark.streaming.timeout: -1
        spark.streaming.unpersist: false
        spark.streaming.kafka.maxRetries: 3
        spark.streaming.ui.retainedBatches: 200
        spark.streaming.receiver.writeAheadLog.enable: false
        spark.ui.port: 4050
      streamConfigurations:

Stream 1: Parse incoming Bro events
___________________________________

Inside this engine you will run a Kafka stream of processing, so we setup input/output topics and Kafka/Zookeeper hosts.
Here the stream will read all the Bro events and notices sent in the ``bro`` topic and push the processing output into the ``logisland_events`` topic.

.. code-block:: yaml

    # Parsing
    - stream: parsing_stream
      component: com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing
      type: stream
      documentation: A processor chain that transforms Bro events into Logisland records
      configuration:
        kafka.input.topics: bro
        kafka.output.topics: logisland_events
        kafka.error.topics: logisland_errors
        kafka.input.topics.serializer: none
        kafka.output.topics.serializer: com.hurence.logisland.serializer.KryoSerializer 
        kafka.error.topics.serializer: com.hurence.logisland.serializer.JsonSerializer
        kafka.metadata.broker.list: sandbox:9092
        kafka.zookeeper.quorum: sandbox:2181
        kafka.topic.autoCreate: true
        kafka.topic.default.partitions: 2
        kafka.topic.default.replicationFactor: 1
      processorConfigurations:

Within this stream there is a single processor in the processor chain: the ``Bro`` processor. It takes an incoming Bro event/notice JSON document computes a Logisland ``Record`` as a sequence of fields
that were contained in the JSON document.

.. code-block:: yaml

    # Transform Bro events into Logisland records
    - processor: Bro adaptor
      component: com.hurence.logisland.processor.bro.BroProcessor
      type: parser
      documentation: A processor that transforms Bro events into LogIsland events
          
This stream will process Bro events as soon as they will be queued into the ``bro`` Kafka topic. Each log will
be parsed as an event which will be pushed back to Kafka in the ``logisland_events`` topic.

Stream 2: Index the processed records into Elasticsearch
________________________________________________________

The second Kafka stream will handle ``Records`` pushed into the ``logisland_events`` topic to index them into ElasticSearch.
So there is no need to define an output topic. The input topic is enough:

.. code-block:: yaml

    # Indexing
    - stream: indexing_stream
      component: com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing
      type: processor
      documentation: A processor chain that pushes bro events to ES
      configuration:
        kafka.input.topics: logisland_events
        kafka.output.topics: none
        kafka.error.topics: logisland_errors
        kafka.input.topics.serializer: com.hurence.logisland.serializer.KryoSerializer 
        kafka.output.topics.serializer: none
        kafka.error.topics.serializer: com.hurence.logisland.serializer.JsonSerializer
        kafka.metadata.broker.list: sandbox:9092
        kafka.zookeeper.quorum: sandbox:2181
        kafka.topic.autoCreate: true
        kafka.topic.default.partitions: 2
        kafka.topic.default.replicationFactor: 1
      processorConfigurations:
      
The only processor in the processor chain of this stream is the ``PutElasticsearch`` processor.

.. code-block:: yaml

    # Put into ElasticSearch
    - processor: ES Publisher
      component: com.hurence.logisland.processor.elasticsearch.PutElasticsearch
      type: processor
      documentation: A processor that pushes Bro events into ES
      configuration:
        default.index: bro
        default.type: events
        hosts: sandbox:9300
        cluster.name: elasticsearch
        batch.size: 2000
        timebased.index: yesterday
        es.index.field: search_index
        es.type.field: record_type

The ``default.index: bro`` configuration parameter tells the processor to index events into an index starting with the ``bro`` string.
The ``timebased.index: yesterday`` configuration parameter tells the processor to use a date after the index prefix. Thus the index name
is of the form ``/bro.2017.02.20``.

Finally, the ``es.type.field: record_type`` configuration parameter tells the processor to use the 
record field ``record_type`` of the incoming record to determine the ElasticSearch type to use within the index.

We will come back to these settings and what they do in the section where we see examples of events to illustrate the workflow.

As an example, here is an incoming (JSON) Bro Connection event received in the ``bro`` Kafka topic:

.. code-block:: json

    {
      "dns": {
        "AA": false,
        "TTLs": [599],
        "id.resp_p": 53,
        "rejected": false,
        "query": "www.wikipedia.org",
        "answers": ["91.198.174.192"],
        "trans_id": 56307,
        "rcode": 0,
        "id.orig_p": 60606,
        "rcode_name": "NOERROR",
        "TC": false,
        "RA": true,
        "uid": "CJkHd3UABb4W7mx8b",
        "RD": false,
        "id.orig_h": "172.17.0.2",
        "proto": "udp",
        "id.resp_h": "8.8.8.8",
        "Z": 0,
        "ts": 1487785523.12837
      }
    }
    
Then here is the matching ElasticSearch document indexed in ``/bro.XXXX.XX.XX/dns``:
    
.. code-block:: json

    {
      "@timestamp": "2017-02-22T17:45:36Z",
      "AA": false,
      "RA": true,
      "RD": false,
      "TC": false,
      "TTLs": [599],
      "Z": 0,
      "answers": ["91.198.174.192"],
      "id_orig_h": "172.17.0.2",
      "id_orig_p": 60606,
      "id_resp_h": "8.8.8.8",
      "id_resp_p": 53,
      "proto": "udp",
      "query": "www.wikipedia.org",
      "rcode": 0,
      "rcode_name": "NOERROR",
      "record_id": "1947d1de-a65e-42aa-982f-33e9c66bfe26",
      "record_time": 1487785536027,
      "record_type": "dns",
      "rejected": false,
      "trans_id": 56307,
      "ts": 1487785523.12837,
      "uid": "CJkHd3UABb4W7mx8b"
    }

Here, as the Bro event is of type dns, the
event has been indexed using the ``dns`` ES type in the index. This allows to easily search only among events of a particular
type. For instance:

.. code-block:: sh

    curl -X GET http://sandbox:9200/bro.2017.02.20/dns/_search -d @query_among_dns_events.json
    
You can also query the whole types of events using the index without type like this:

.. code-block:: sh

    curl -X GET http://sandbox:9200/bro.2017.02.20/_search -d @query_among_all_events.json
    
TODO TODO TODO parler de part from @timestamp, ils sont c'est le format du record. aussi parler des notifications
ajouter une note qui dit qu'on peut plugger les processeurs qu on veut en utilisant ce formt parler aussi du renommage des . en _

 .. _StartBroContainer:

3. Start the Docker container with Bro
--------------------------------------

For this tutorial, we provide Bro as a Docker image that you can `build yourself <https://github.com/Hurence/logisland/tree/master/logisland-docker/bro>`_ or pull from Docker Hub.
The docker image is built from an Ubuntu image with the following components already installed:

- Bro
- Bro-Kafka plugin

.. note::

    Due to the fact that Bro requires a Kafka plugin to be able to send events to Kafka and that building the Bro-Kafka plugin requires
    some substantial steps (need Bro sources), for this tutorial, we are only focusing on configuring Bro, and consider it already compiled and installed
    with its Bro-Kafka plugin (this is the case in our Bro docker image). But looking at the Dockerfile we made to build the Bro Docker
    image and which is located `here <https://github.com/Hurence/logisland/tree/master/logisland-docker/bro/Dockerfile>`_,
    you will have an idea on how to install Bro and Bro-Kafka plugin binaries on your own systems.

Pull the Bro image from Docker Repository:

**WARNING: The Bro image is not yet available in the Docker Hub: please build our Bro Docker image yourself as described in the link above for the moment.**

.. code-block:: sh

    docker pull hurence/bro
    
Start a Bro container from the Bro image:

.. code-block:: sh

    # run container
    docker run -it --name bro -h bro hurence/bro:0.9.8

    # get container ip
    docker inspect bro | grep IPAddress

    # or if your are on mac os
    docker-machine ip default

4. Configure Bro to send events to Kafka
----------------------------------------

In the following steps, if you want a new shell to your running bro container, do as necessary:

.. code-block:: sh

    docker exec -ti bro bash

Edit the Bro config file
________________________

We will configure Bro so that it loads the Bro-Kafka plugin at startup. We will also point to Kafka of the Logisland container
and define the event types we want to push to Logisland.

Edit the config file of bro: 

.. code-block:: sh

    vi $BRO_HOME/share/bro/site/local.bro

At the beginning of the file, add the following section (take care to respect
indentation):

.. note::

    WARNING: replace the ``172.17.0.2`` IP address with the address of the Logisland container if different.

.. code-block:: bro

    @load Bro/Kafka/logs-to-kafka.bro
        redef Kafka::kafka_conf = table(
            ["metadata.broker.list"] = "172.17.0.2:9092",
            ["client.id"] = "bro"
        );
        redef Kafka::topic_name = "bro";
        redef Kafka::logs_to_send = set(Conn::LOG, HTTP::LOG, DNS::LOG, Notice::LOG);
        redef Kafka::tag_json = T;

Let's detail a bit what we did:
 
This line tells Bro to load the Bro-Kafka plugin at startup (the next lines are configuration for the Bro-Kafka plugin):
 
.. code-block:: bro

    @load Bro/Kafka/logs-to-kafka.bro

These lines make the Bro-Kafka plugin point to the Kafka instance in the Logisland
container (host, port, client id to use). These are communication settings:
 
.. code-block:: bro

    redef Kafka::kafka_conf = table(
        ["metadata.broker.list"] = "172.17.0.2:9092",
        ["client.id"] = "bro"

This line tells the Kafka topic name to use. It is important that it is the same as the
input topic of the Bro processor in Logisland:

.. code-block:: bro    
        
    redef Kafka::topic_name = "bro";
        
This line tells the Bro-Kafka plugin what type of events should be intercepted and sent to Kafka. For this tutorial we
send connections, HTTP and DNS events. We are also interested in any notice (alarm) that Bro can generate.
For a complete list of possibilities, see the Bro documentation for `events <https://www.bro.org/sphinx/script-reference/log-files.html>`_
and `notices <https://www.bro.org/sphinx/bro-noticeindex.html>`_:
 
.. code-block:: bro

    redef Kafka::logs_to_send = set(Conn::LOG, HTTP::LOG, DNS::LOG, Notice::LOG);

This line tells the Bro-Kafka plugin to add the event type in the Bro JSON document it sends.
This is required and expected by the Bro Processor as it uses this field to tag the record with his type.
This also tells Logisland which ElasticSearch index type to use for storing the event:
 
.. code-block:: bro

    redef Kafka::tag_json = T;

broctl
   install
   start
   


ls $BRO_HOME/logs/current

curl http://172.17.0.3:9200

kafkacat -b localhost:9092 -t bro -o end

cd $PCAP_HOME
bro -r ssh.pcap local

Now we're going to send some logs to ``logisland_raw`` Kafka topic.

We could setup a logstash or flume agent to load some apache logs into a kafka topic
but there's a super useful tool in the Kafka ecosystem : `kafkacat <https://github.com/edenhill/kafkacat>`_,
a *generic command line non-JVM Apache Kafka producer and consumer* which can be easily installed.


If you don't have your own httpd logs available, you can use some freely available log files from
`NASA-HTTP <http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html>`_ web site access:

- `Jul 01 to Jul 31, ASCII format, 20.7 MB gzip compressed <ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz>`_
- `Aug 04 to Aug 31, ASCII format, 21.8 MB gzip compressed <ftp://ita.ee.lbl.gov/traces/NASA_access_logAug95.gz>`_

Let's send the first 500000 lines of NASA hhtp access over July 1995 to LogIsland with kafkacat to ``logisland_raw`` Kafka topic

.. code-block:: sh

    docker exec -ti logisland bash
    cd /tmp
    wget ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
    gunzip NASA_access_log_Jul95.gz
    head 500000 NASA_access_log_Jul95 | kafkacat -b sandbox:9092 -t logisland_raw

5. Generate some Bro events and notices
---------------------------------------

6. Mine your events in ElasticSearch
------------------------------------
