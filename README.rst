Log Island
==========

.. image:: https://travis-ci.org/Hurence/logisland.svg?branch=master
   :target: https://travis-ci.org/Hurence/logisland

.. image:: https://codeclimate.com/github/Hurence/logisland/badges/gpa.svg
   :target: https://codeclimate.com/github/Hurence/logisland

.. image:: https://coveralls.io/repos/github/Hurence/logisland/badge.svg?branch=master
   :target: https://coveralls.io/github/Hurence/logisland?branch=master

Download the `latest release build <https://github.com/Hurence/logisland/releases>`_  and chat with us on gitter

.. image:: https://badges.gitter.im/Join%20Chat.svg
   :target: https://gitter.im/logisland/logisland?utm_source=share-link&utm_medium=link&utm_campaign=share-link
   :alt: Gitter



**LogIsland is an event mining platform based on Spark and Kafka to handle a huge amount of log files.**

.. image:: https://raw.githubusercontent.com/Hurence/logisland/master/logisland-documentation/_static/logisland-workflow.png
    :alt: architecture

You can start right now playing with **logisland** through the Docker image, by getting started with `apache log indexing tutorial <http://logisland.readthedocs.io/en/latest/tutorials/index-apache-logs.html>`_ guide.

The `documentation <http://logisland.readthedocs.io/en/latest/index.html>`_  also explains how to implement your own processors and deploy them as custom `plugins <http://logisland.readthedocs.io/en/latest/plugins.html>`_.

Browse the `Java API documentation <http://logisland.readthedocs.io/en/latest/_static/apidocs/>`_ for more information.


Build
-----
to build from the source just clone and package

.. code-block::

    git clone git@github.com:Hurence/logisland.git
    cd logisland
    mvn install -Dhdp=2.4



Basic Workflow
--------------
Here is an example of how you can setup a full event mining pipeline.

1. Raw log files are sent to Kafka topics by a NIFI / Logstash / Flume / Collectd (or whatever) agent 
3. Logs in Kafka topic are translated into Events and pushed back to another Kafka topic by a LogIsland streaming job
3. Events in Kafka topic are sent to Elasticsearch (or Solr or whatever backend) for online analytics (Kibana or Banana) by a Spark streaming job
4. Log topics can also dumped to HDFS (master dataset) for offline analytics
5. Event component do some time window based analytics on events to build new events



Setup a stream processing workflow
----------------------------------

A LogIsland stream processing flow is made of a bunch of components. At least one streaming engine and 1 or more stream processors. You set them up by a YAML configuration file. Please note that events are serialized against an Avro schema while transiting through any Kafka topic. Every `spark.streaming.batchDuration` (time window), each processor will handle its bunch of Events to eventually generate some new events to the output topic.
The following `conf/configuration-template.yml` contains a sample of processor definitions.

.. code-block:: yaml


    #########################################################################################################
    # Logisland configuration script tempate
    #########################################################################################################

    version: 0.9.5
    documentation: LogIsland analytics main config file. Put here every engine or component config

    #########################################################################################################
    # engine
    engine:
      component: com.hurence.logisland.engine.spark.SparkStreamProcessingEngine
      type: engine
      documentation: Main Logisland job entry point
      configuration:
        spark.master: local[4]
        spark.driver.memory: 512m
        spark.driver.cores: 1
        spark.executor.memory: 512m
        spark.executor.cores: 2
        spark.executor.instances: 4
        spark.app.name: LogislandTutorial
        spark.streaming.batchDuration: 4000
        spark.serializer: org.apache.spark.serializer.KryoSerializer
        spark.streaming.backpressure.enabled: true
        spark.streaming.unpersist: false
        spark.streaming.blockInterval: 500
        spark.streaming.kafka.maxRatePerPartition: 3000
        spark.streaming.timeout: -1
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

        # parsing
        - stream: parsing_stream
          component: com.hurence.logisland.processor.chain.KafkaRecordStream
          type: stream
          documentation: a processor that links
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
            kafka.topic.default.partitions: 2
            kafka.topic.default.replicationFactor: 1
          processorConfigurations:

            # parse apache logs
            - processor: apache_parser
              component: com.hurence.logisland.processor.SplitText
              type: parser
              documentation: a parser that produce events from an apache log REGEX
              configuration:
                value.regex: (\S+)\s+(\S+)\s+(\S+)\s+\[([\w:/]+\s[+\-]\d{4})\]\s+"(\S+)\s+(\S+)\s+(\S+)"\s+(\S+)\s+(\S+)
                value.fields: src_ip,identd,user,record_time,http_method,http_query,http_version,http_status,bytes_out

        # indexing
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
              documentation: a processor that indexes processed events in elasticsearch
              configuration:
                elasticsearch.client.service: elasticsearch_service
                default.index: logisland
                default.type: event
                timebased.index: yesterday
                es.index.field: search_index
                es.type.field: record_type




Start a stream workflow
-----------------------

One you've edited your configuration file, you can submit it to execution engine with the following cmd :

.. code-block:: bash

    bin/process-stream.sh -conf conf/configuration-template.yml


Create a new plugin
-------------------

Logisland processors are hosted in some plugins, you can create your own with a maven archetype.


.. code-block:: bash

    git clone git@github.com:Hurence/logisland.git
    cd logisland-0.9.5/logisland-plugins
    mvn archetype:generate -DarchetypeGroupId=com.hurence.logisland -DarchetypeArtifactId=logisland-plugin-archetype -DarchetypeVersion=0.9.5 -DlogislandVersion=0.9.5
    
    
    Define value for property 'groupId': : com.hurence.logisland
    Define value for property 'artifactId': : logisland-sample-plugin
    Define value for property 'version':  0.9.5: : 0.1
    Define value for property 'artifactBaseName': : sample
    Define value for property 'package':  com.hurence.logisland.sample: :
    [INFO] Using property: logislandVersion = 0.9.5
