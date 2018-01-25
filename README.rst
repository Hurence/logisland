Logisland
=========

.. image:: https://travis-ci.org/Hurence/logisland.svg?branch=master
   :target: https://travis-ci.org/Hurence/logisland


.. image:: https://badges.gitter.im/Join%20Chat.svg
   :target: https://gitter.im/logisland/logisland?utm_source=share-link&utm_medium=link&utm_campaign=share-link
   :alt: Gitter


Download the `latest release build <https://github.com/Hurence/logisland/releases>`_  and
chat with us on `gitter <https://gitter.im/logisland/logisland>`_


**LogIsland is an event mining scalable platform designed to handle a high throughput of events.**


Event mining Workflow
---------------------
Here is an example of a typical event mining pipeline.

1. Raw events (sensor data, logs, user click stream, ...) are sent to Kafka topics by a NIFI / Logstash / *Beats / Flume / Collectd (or whatever) agent
2. Raw events are structured in Logisland Records, then processed and eventually pushed back to another Kafka topic by a Logisland streaming job
3. Records are sent to external short living storage (Elasticsearch, Solr, Couchbase, ...) for online analytics.
4. Records are sent to external long living storage (HBase, HDFS, ...) for offline analytics (aggregated reports or ML models).
5. Logisland Processors handle Records to produce Alerts and Information from ML models


Online documentation
--------------------
You can find the latest Logisland documentation, including a programming guide,
on the `project web page. <http://logisland.readthedocs.io/en/latest/index.html>`_
This README file only contains basic setup instructions.

Browse the `Java API documentation <http://logisland.readthedocs.io/en/latest/_static/apidocs/>`_ for more information.


You can follow one getting started guide through the
`apache log indexing tutorial <http://logisland.readthedocs.io/en/latest/tutorials/index-apache-logs.html>`_.


Building Logisland
------------------
to build from the source just clone source and package with maven

.. code-block::

    git clone https://github.com/Hurence/logisland.git
    cd logisland
    mvn install

the final package is available at `logisland-assembly/target/logisland-0.11.0-bin-hdp2.5.tar.gz`

You can also download the `latest release build <https://github.com/Hurence/logisland/releases>`_


Local Setup
-----------
basically **logisland** depends on Kafka and Spark, you can deploy it on any linux server

.. code-block::

    # install Kafka 0.10.0.0 & start a zookeeper node + a broker
    curl -s http://apache.crihan.fr/dist/kafka/0.10.0.0/kafka_2.11-0.10.0.0.tgz | tar -xz -C /usr/local/
    cd /usr/local/kafka_2.11-0.10.0.0
    nohup bin/zookeeper-server-start.sh config/zookeeper.properties > zookeeper.log 2>&1 &
    JMX_PORT=10101 nohup bin/kafka-server-start.sh config/server.properties > kafka.log 2>&1 &

    # install Spark 2.1.0
    curl -s http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz | tar -xz -C /usr/local/
    export SPARK_HOME=/usr/local/spark-2.1.0-bin-hadoop2.7

    # install Logisland 0.11.0
    curl -s https://github.com/Hurence/logisland/releases/download/v0.10.0/logisland-0.11.0-bin-hdp2.5.tar.gz  | tar -xz -C /usr/local/
    cd /usr/local/logisland-0.11.0

    # launch a logisland job
    bin/logisland.sh --conf conf/index-apache-logs.yml

you can find some **logisland** job configuration samples under `$LOGISLAND_HOME/conf` folder


Start a stream processing job
-----------------------------

A Logisland stream processing job is made of a bunch of components.
At least one streaming engine and 1 or more stream processors. You set them up by a YAML configuration file.

Please note that events are serialized against an Avro schema while transiting through any Kafka topic.
Every `spark.streaming.batchDuration` (time window), each processor will handle its bunch of Records to eventually
 generate some new Records to the output topic.

The following `configuration.yml` file contains a sample of job that parses raw Apache logs and send them to Elasticsearch.


The first part is the `ProcessingEngine` configuration (here a Spark streaming engine)

.. code-block:: yaml

    version: 0.11.0
    documentation: LogIsland job config file
    engine:
      component: com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine
      type: engine
      documentation: Index some apache logs with logisland
      configuration:
        spark.app.name: IndexApacheLogsDemo
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
      controllerServiceConfigurations:

Then comes a list of `ControllerService` which are the shared components that interact with outside world (Elasticearch, HBase, ...)

.. code-block:: yaml

        - controllerService: elasticsearch_service
          component: com.hurence.logisland.service.elasticsearch.Elasticsearch_2_3_3_ClientService
          type: service
          documentation: elasticsearch service
          configuration:
            hosts: sandbox:9300
            cluster.name: elasticsearch
            batch.size: 5000

Then comes a list of `RecordStream`, each of them route the input batch of `Record` through a pipeline of `Processor`
to the output topic

.. code-block:: yaml

      streamConfigurations:
        - stream: parsing_stream
          component: com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing
          type: stream
          documentation: a processor that converts raw apache logs into structured log records
          configuration:
            kafka.input.topics: logisland_raw
            kafka.output.topics: logisland_events
            kafka.error.topics: logisland_errors
            kafka.input.topics.serializer: none
            kafka.output.topics.serializer: com.hurence.logisland.serializer.KryoSerializer
            kafka.error.topics.serializer: com.hurence.logisland.serializer.JsonSerializer
            kafka.metadata.broker.list: sandbox:9092
            kafka.zookeeper.quorum: sandbox:2181
            kafka.topic.autoCreate: true
            kafka.topic.default.partitions: 4
            kafka.topic.default.replicationFactor: 1

Then come the configurations of all the `Processor` pipeline. Each Record will go through these components.
Here we first parse raw apache logs and then we add those records to Elasticsearch. Pleas note that the ES processor makes
use of the previously defined ControllerService.

.. code-block:: yaml

          processorConfigurations:

            - processor: apache_parser
              component: com.hurence.logisland.processor.SplitText
              type: parser
              documentation: a parser that produce records from an apache log REGEX
              configuration:
                record.type: apache_log
                value.regex: (\S+)\s+(\S+)\s+(\S+)\s+\[([\w:\/]+\s[+\-]\d{4})\]\s+"(\S+)\s+(\S+)\s*(\S*)"\s+(\S+)\s+(\S+)
                value.fields: src_ip,identd,user,record_time,http_method,http_query,http_version,http_status,bytes_out

            - processor: es_publisher
              component: com.hurence.logisland.processor.elasticsearch.BulkAddElasticsearch
              type: processor
              documentation: a processor that indexes processed events in elasticsearch
              configuration:
                elasticsearch.client.service: elasticsearch_service
                default.index: logisland
                default.type: event
                timebased.index: yesterday
                es.index.field: search_index
                es.type.field: record_type



Once you've edited your configuration file, you can submit it to execution engine with the following cmd :

.. code-block:: bash

    bin/process-stream.sh -conf conf/job-configuration.yml

Contributing
------------

Please review the `Contribution to Logisland guide <http://logisland.readthedocs.io/en/latest/developer.html>`_ for information on how to get started contributing to the project.
