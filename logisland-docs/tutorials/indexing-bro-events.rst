Bro/Logisland integration - Indexing Bro events
===============================================

Bro and Logisland
-----------------

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
        timebased.index: today
        es.index.field: search_index
        es.type.field: record_type

The ``default.index: bro`` configuration parameter tells the processor to index events into an index starting with the ``bro`` string.
The ``timebased.index: today`` configuration parameter tells the processor to use the current date after the index prefix. Thus the index name
is of the form ``/bro.2017.02.23``.

Finally, the ``es.type.field: record_type`` configuration parameter tells the processor to use the 
record field ``record_type`` of the incoming record to determine the ElasticSearch type to use within the index.

We will come back to these settings and what they do in the section where we see examples of events to illustrate the workflow.

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

.. warning::

   If the Bro image is not yet available in the Docker Hub: please build our Bro Docker image yourself as described in the link above for the moment.

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

    Replace the ``172.17.0.2`` IP address with the address of the Logisland container if different.

.. code-block:: bro

    @load Bro/Kafka/logs-to-kafka.bro
        redef Kafka::kafka_conf = table(
            ["metadata.broker.list"] = "172.17.0.2:9092",
            ["client.id"] = "bro"
        );
        redef Kafka::topic_name = "bro";
        redef Kafka::logs_to_send = set(Conn::LOG, DNS::LOG, SSH::LOG, Notice::LOG);
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
send Connections, DNS and SSH events. We are also interested in any notice (alert) that Bro can generate.
For a complete list of possibilities, see the Bro documentation for `events <https://www.bro.org/sphinx/script-reference/log-files.html>`_
and `notices <https://www.bro.org/sphinx/bro-noticeindex.html>`_:
 
.. code-block:: bro

    redef Kafka::logs_to_send = set(Conn::LOG, DNS::LOG, SSH::LOG, Notice::LOG);

This line tells the Bro-Kafka plugin to add the event type in the Bro JSON document it sends.
This is required and expected by the Bro Processor as it uses this field to tag the record with his type.
This also tells Logisland which ElasticSearch index type to use for storing the event:
 
.. code-block:: bro

   redef Kafka::tag_json = T;
    
Start Bro
_________

To start bro, we use the ``broctl`` command that is already in the path of the container.
It starts an interactive session to control bro:

.. code-block:: sh

   broctl

Then start the bro service: use the ``deploy`` command in broctl session:

.. code-block:: sh

   Welcome to BroControl 1.5-9

   Type "help" for help.

   [BroControl] > deploy
   checking configurations ...
   installing ...
   removing old policies in /usr/local/bro/spool/installed-scripts-do-not-touch/site ...
   removing old policies in /usr/local/bro/spool/installed-scripts-do-not-touch/auto ...
   creating policy directories ...
   installing site policies ...
   generating standalone-layout.bro ...
   generating local-networks.bro ...
   generating broctl-config.bro ...
   generating broctl-config.sh ...
   stopping ...
   bro not running
   starting ...
   starting bro ...

.. note::

   The ``deploy`` command is a shortcut to the ``check``, ``install`` and ``restart`` commands.
   Everytime you modify the ``$BRO_HOME/share/bro/site/local.bro`` configuration file, you must re-issue a ``deploy`` command so that
   changes are taken into account.

5. Generate some Bro events and notices
---------------------------------------

Now that everything is in place you can generate some network activity in the Bro container to generate some events and see them indexed in ElasticSearch.

Monitor Kafka topic
___________________

We will generate some events but first we want to see them in Kafka to be sure Bro has forwarded them to Kafka.
Connect to the Logisland container:

.. code-block:: sh

   docker exec -ti logisland bash
   
Then use the ``kafkacat`` command to listen to messages incoming in the ``bro`` topic:
   
.. code-block:: sh

   kafkacat -b localhost:9092 -t bro -o end
   
Let the command run. From now on, any incoming event from Bro and entering Kafka will be also displayed in this shell.

Issue a DNS query
_________________

Open a shell to the Bro container:

.. code-block:: sh

   docker exec -ti bro bash
   
Then use the ``ping`` command to trigger an underlying DNS query:
   
.. code-block:: sh

   ping www.wikipedia.org
   
You should see in the listening ``kafkacat`` shell an incoming  JSON Bro event of type ``dns``.

Here is a pretty print version of this event. It should look like this one:

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

The Bro Processor should have processed this event which should have been handled next by the PutElasticsearch processor and
finally the event should have been stored in ElasticSearch in the Logisland container.

To see this stored event, we will query ElasticSearch with the ``curl`` command. Let's browse the ``dns`` type in any index starting with ``bro``:

.. code-block:: sh

   curl http://sandbox:9200/bro*/dns/_search?pretty

.. note::

   Do not forget to change sandbox with the IP address of the Logisland container if needed.
   
You should be able to localize in the response from ElasticSearch a DNS event which looks like:

.. code-block:: json

    {
      "_index" : "bro.2017.02.23",
      "_type" : "dns",
      "_id" : "6aecfa3a-6a9e-4911-a869-b4e4599a69c1",
      "_score" : 1.0,
      "_source" : {
        "@timestamp": "2017-02-23T17:45:36Z",
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
    }

You should see that this JSON document is stored in a indexed of the form ``/bro.XXXX.XX.XX/dns``:

.. code-block:: json

      "_index" : "bro.2017.02.23",
      "_type" : "dns",

Here, as the Bro event is of type ``dns``, the event has been indexed using the ``dns`` ES
type in the index. This allows to easily search only among events of a particular
type.

The Bro processor has used the first level field ``dns`` of the incoming JSON event from Bro to add
a ``record_type`` field to the record he has created. This field has been used by the PutElasicsearch processor
to determine the index type to use for storing the record.

The ``@timestamp`` field is added by the PutElasticsearch processor before pushing the record into ES. Its value is
derived from the ``record_time`` field which has been added with also the ``record_id`` field by Logisland
when the event entered Logisland. The ``ts`` field is the Bro timestamp which is the time when the event
was generated in the Bro system.

Other second level fields of the incoming JSON event from Bro have been set as first level fields in the record
created by the Bro Processor. Also any field that had a "." chacracter has been transformed to use a "_" character.
For instance the ``id.orig_h`` field has been renamed into ``id_orig_h``.

That is basically all the job the Bro Processor does. It's a small adaptation layer for Bro events. Now if you look in the
Bro documentation and know the Bro event format, you can be able to know the format of a matching record going out of
the Bro processor. You should then be able to write some Logsisland processors to handle any record going out of the Bro Processor.

Issue a Bro Notice
__________________

As a Bro notice is the result of analysis of many events, generating a real notice event with Bro is a bit more complicated if
you want to generate it with real traffic. Fortunately, Bro has the ability to generate events also from ``pcap`` files.
These are "*packect capture*" files. They hold the recording of a real network traffic. Bro analyzes the packets in those
files and generate events as if he was listening to real traffic.

In the Bro container, we have preloaded some ``pcap`` files in the ``$PCAP_HOME`` directory. Go into this directory:

.. code-block:: sh

   cd $PCAP_HOME
   
The ``ssh.pcap`` file in this directory is a capture of a network traffic in which there is some SSH traffic with an
attempt to guess a user password. The analysis of such traffic generates a Bro ``SSH::Password_Guessing`` notice.
   
Let's launch the following command to make Bro analyze the packets in the ``ssh.pcap`` file and generate this notice:

.. code-block:: sh
 
   bro -r ssh.pcap local
   
In your the previous ``kafkacat`` shell, you should see some ``ssh`` events that represent the SSH traffic. You shoul also see
a ``notice`` event event like this one:

.. code-block:: json

   {
     "notice": {
       "ts":1320435875.879278,
       "note":"SSH::Password_Guessing",
       "msg":"172.16.238.1 appears to be guessing SSH passwords (seen in 30 connections).",
       "sub":"Sampled servers:  172.16.238.136, 172.16.238.136, 172.16.238.136, 172.16.238.136, 172.16.238.136",
       "src":"172.16.238.1",
       "peer_descr":"bro",
       "actions":["Notice::ACTION_LOG"],
       "suppress_for":3600.0,
       "dropped":false
     }
   }
   
Then, like for the DNS event, it should also have been indexed in the ``notice`` index type in ElastiSearch. Browse documents in this
type like this:

.. code-block:: sh

   curl http://sandbox:9200/bro*/notice/_search?pretty

.. note::

   Do not forget to change sandbox with the IP address of the Logisland container if needed.
   
In the response, you should see a notice event like this: 
   
.. code-block:: json

   {
      "_index" : "bro.2017.02.23",
      "_type" : "notice",
      "_id" : "76ab556b-167d-4594-8ee8-b05594cab8fc",
      "_score" : 1.0,
      "_source" : {
        "@timestamp" : "2017-02-23T10:45:08Z",
        "actions" : [ "Notice::ACTION_LOG" ],
        "dropped" : false,
        "msg" : "172.16.238.1 appears to be guessing SSH passwords (seen in 30 connections).",
        "note" : "SSH::Password_Guessing",
        "peer_descr" : "bro",
        "record_id" : "76ab556b-167d-4594-8ee8-b05594cab8fc",
        "record_time" : 1487933108041,
        "record_type" : "notice",
        "src" : "172.16.238.1",
        "sub" : "Sampled servers:  172.16.238.136, 172.16.238.136, 172.16.238.136, 172.16.238.136, 172.16.238.136",
        "suppress_for" : 3600.0,
        "ts" : 1.320435875879278E9
      }
    }
    
We are done with this first approach of Bro integration with LogIsland.

As we configured Bro to also send SSH and Connection events to Kafka, you can have a look at the matching
generated events in ES by browsing the ``ssh`` and ``conn`` index types:

.. code-block:: sh

   # Browse SSH events
   curl http://sandbox:9200/bro*/ssh/_search?pretty
   # Browse Connection events
   curl http://sandbox:9200/bro*/conn/_search?pretty

If you wish, you can also add some additional event types to be sent to Kafka in the Bro config
file and browse the matching indexed events in ES using the same kind of ``curl`` commands just by changing
the type in the query (do not forget to re-deploy Bro after configuration file modifications).
