Index Apache logs
=================

In the following getting started tutorial we'll drive you through the process of Apache log mining with LogIsland platform.

We will start a Docker container hosting all the LogIsland services, launch two streaming processes and send some apache logs
to the system in order to analyze them in a dashboard.


.. note::

    You can download the `latest release <https://github.com/Hurence/logisland/releases>`_ of logisland and the `YAML configuration file <https://github.com/Hurence/logisland/blob/master/logisland-framework/logisland-resources/src/main/resources/conf/index-apache-logs.yml>`_ for this tutorial which can be also found under `$LOGISLAND_HOME/conf` directory.


1. Start LogIsland as a Docker container
----------------------------------------
LogIsland is packaged as a Docker container that you can build yourself or pull from Docker Hub.
The docker container is built from a Centos 6.4 image with the following tools enabled

- Kafka
- Spark
- Elasticsearch
- Kibana
- Logstash
- Flume
- Nginx
- LogIsland

Pull the image from Docker Repository (it may take some time)

.. code-block:: sh

    docker pull hurence/logisland

You should be aware that this Docker container is quite eager in RAM and will need at least 8G of memory to run smoothly.
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
    docker inspect logisland

    # or if your are on mac os
    docker-machine ip default

you should add an entry for **sandbox** (with the container ip) in your ``/etc/hosts`` as it will be easier to access to all web services in logisland running container.


.. note::

    If you have your own Spark and Kafka cluster, you can download the `latest release <https://github.com/Hurence/logisland/releases>`_ and unzip on an edge node.

2. Parse the logs records
-------------------------
For this tutorial we will handle some apache logs with a splitText parser and send them to Elastiscearch
Connect a shell to your logisland container to launch the following streaming jobs.

.. code-block:: sh

    docker exec -ti logisland bash
    cd $LOGISLAND_HOME
    bin/logisland.sh --conf conf/index-apache-logs.yml



Setup Spark/Kafka streaming engine
__________________________________
An Engine is needed to handle the stream processing. This ``conf/index-apache-logs.yml`` configuration file defines a stream processing job setup.
The first section configures the Spark engine (we will use a `KafkaStreamProcessingEngine <../plugins.html#kafkastreamprocessingengine>`_) as well as an Elasticsearch service that will be used later in the BulkAddElasticsearch processor

.. code-block:: yaml

    engine:
      component: com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine
      type: engine
      documentation: Main Logisland job entry point
      configuration:
        spark.app.name: LogislandTutorial
        spark.master: local[4]
        spark.driver.memory: 1G
        spark.driver.cores: 1
        spark.executor.memory: 3G
        spark.executor.instances: 4
        spark.executor.cores: 2
        spark.yarn.queue: default
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

      controllerServiceConfigurations:

        - controllerService: elasticsearch_service
          component: com.hurence.logisland.service.elasticsearch.Elasticsearch_2_4_0_ClientService
          type: service
          documentation: elasticsearch 2.4.0 service implementation
          configuration:
            hosts: sandbox:9300
            cluster.name: elasticsearch
            batch.size: 2000

      streamConfigurations:

Stream 1 : parse incoming apache log lines
__________________________________________
Inside this engine you will run a Kafka stream of processing, so we setup input/output topics and Kafka/Zookeeper hosts.
Here the stream will read all the logs sent in ``logisland_raw`` topic and push the processing output into ``logisland_events`` topic.

.. note::

    We want to specify an Avro output schema to validate our ouput records (and force their types accordingly).
    It's really for other streams to rely on a schema when processing records from a topic.

We can define some serializers to marshall all records from and to a topic.

.. code-block:: yaml

    # parsing
    - stream: parsing_stream
      component: com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing
      type: stream
      documentation: a processor that links
      configuration:
        kafka.input.topics: logisland_raw
        kafka.output.topics: logisland_events
        kafka.error.topics: logisland_errors
        kafka.input.topics.serializer: none
        kafka.output.topics.serializer: com.hurence.logisland.serializer.KryoSerializer
        kafka.error.topics.serializer: com.hurence.logisland.serializer.JsonSerializer
        avro.output.schema: >
          {  "version":1,
             "type": "record",
             "name": "com.hurence.logisland.record.apache_log",
             "fields": [
               { "name": "record_errors",   "type": [ {"type": "array", "items": "string"},"null"] },
               { "name": "record_raw_key", "type": ["string","null"] },
               { "name": "record_raw_value", "type": ["string","null"] },
               { "name": "record_id",   "type": ["string"] },
               { "name": "record_time", "type": ["long"] },
               { "name": "record_type", "type": ["string"] },
               { "name": "src_ip",      "type": ["string","null"] },
               { "name": "http_method", "type": ["string","null"] },
               { "name": "bytes_out",   "type": ["long","null"] },
               { "name": "http_query",  "type": ["string","null"] },
               { "name": "http_version","type": ["string","null"] },
               { "name": "http_status", "type": ["string","null"] },
               { "name": "identd",      "type": ["string","null"] },
               { "name": "user",        "type": ["string","null"] }    ]}
        kafka.metadata.broker.list: sandbox:9092
        kafka.zookeeper.quorum: sandbox:2181
        kafka.topic.autoCreate: true
        kafka.topic.default.partitions: 4
        kafka.topic.default.replicationFactor: 1
      processorConfigurations:


Within this stream a ``SplitText`` processor takes a log line as a String and computes a ``Record`` as a sequence of fields.

.. code-block:: yaml

    # parse apache logs
    - processor: apache_parser
      component: com.hurence.logisland.processor.SplitText
      type: parser
      documentation: a parser that produce events from an apache log REGEX
      configuration:
        value.regex: (\S+)\s+(\S+)\s+(\S+)\s+\[([\w:\/]+\s[+\-]\d{4})\]\s+"(\S+)\s+(\S+)\s*(\S*)"\s+(\S+)\s+(\S+)
        value.fields: src_ip,identd,user,record_time,http_method,http_query,http_version,http_status,bytes_out

This stream will process log entries as soon as they will be queued into `logisland_raw` Kafka topics, each log will
be parsed as an event which will be pushed back to Kafka in the ``logisland_events`` topic.


Stream 2 :Index the processed records to Elasticsearch
______________________________________________________
The second Kafka stream will handle ``Records`` pushed into ``logisland_events`` topic to index them into elasticsearch

.. code-block:: yaml

    - stream: indexing_stream
      component: com.hurence.logisland.processor.chain.KafkaRecordStream
      type: processor
      documentation: a processor that push events to ES
      configuration:
        kafka.input.topics: logisland_events
        kafka.output.topics: none
        kafka.error.topics: logisland_errors
        kafka.input.topics.serializer: com.hurence.logisland.serializer.KryoSerializer
        kafka.output.topics.serializer: com.hurence.logisland.serializer.KryoSerializer
        kafka.error.topics.serializer: com.hurence.logisland.serializer.JsonSerializer
        kafka.metadata.broker.list: sandbox:9092
        kafka.zookeeper.quorum: sandbox:2181
        kafka.topic.autoCreate: true
        kafka.topic.default.partitions: 2
        kafka.topic.default.replicationFactor: 1
      processorConfigurations:

        # add to elasticsearch
        - processor: es_publisher
          component: com.hurence.logisland.processor.elasticsearch.BulkAddElasticsearch
          type: processor
          documentation: a processor that trace the processed events
          configuration:
            elasticsearch.client.service: elasticsearch_service
            default.index: logisland
            default.type: event
            timebased.index: yesterday
            es.index.field: search_index
            es.type.field: record_type



3. Inject some Apache logs into the system
------------------------------------------
Now we're going to send some logs to ``logisland_raw`` Kafka topic.

We could setup a logstash or flume agent to load some apache logs into a kafka topic
but there's a super useful tool in the Kafka ecosystem : `kafkacat <https://github.com/edenhill/kafkacat>`_,
a *generic command line non-JVM Apache Kafka producer and consumer* which can be easily installed.


If you don't have your own httpd logs available, you can use some freely available log files from
`NASA-HTTP <http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html>`_ web site access:

- `Jul 01 to Jul 31, ASCII format, 20.7 MB gzip compressed <ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz>`_
- `Aug 04 to Aug 31, ASCII format, 21.8 MB gzip compressed <ftp://ita.ee.lbl.gov/traces/NASA_access_logAug95.gz>`_

Let's send the first 500000 lines of NASA http access over July 1995 to LogIsland with kafkacat to ``logisland_raw`` Kafka topic

.. code-block:: sh

    docker exec -ti logisland bash
    cd /tmp
    wget ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
    gunzip NASA_access_log_Jul95.gz
    head -500000 NASA_access_log_Jul95 | kafkacat -b sandbox:9092 -t logisland_raw


4. Monitor your spark jobs and Kafka topics
-------------------------------------------
Now go to `http://sandbox:4050/streaming/ <http://sandbox:4050/streaming/>`_ to see how fast Spark can process
your data

.. image:: /_static/spark-job-monitoring.png


Another tool can help you to tweak and monitor your processing `http://sandbox:9000/ <http://sandbox:9000>`_

.. image:: /_static/kafka-mgr.png


5. Use Kibana to inspect the logs
---------------------------------
Open up your browser and go to `http://sandbox:5601/ <http://sandbox:5601/app/kibana#/discover?_g=(refreshInterval:(display:Off,pause:!f,value:0),time:(from:'1995-05-08T12:14:53.216Z',mode:absolute,to:'1995-11-25T05:30:52.010Z'))&_a=(columns:!(_source),filters:!(),index:'li-*',interval:auto,query:(query_string:(analyze_wildcard:!t,query:usa)),sort:!('@timestamp',desc),vis:(aggs:!((params:(field:host,orderBy:'2',size:20),schema:segment,type:terms),(id:'2',schema:metric,type:count)),type:histogram))&indexPattern=li-*&type=histogram>`_ and you should be able to explore your apache logs.


Configure a new index pattern with ``logisland.*`` as the pattern name and ``@timestamp`` as the time value field.

.. image:: /_static/kibana-configure-index.png

Then if you go to Explore panel for the latest 15' time window you'll only see logisland process_metrics events which give you
insights about the processing bandwidth of your streams.

.. image:: /_static/kibana-logisland-metrics.png

As we explore data logs from july 1995 we'll have to select an absolute time filter from 1995-06-30 to 1995-07-08 to see the events.

.. image:: /_static/kibana-apache-logs.png



