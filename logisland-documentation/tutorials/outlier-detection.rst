Time series sampling & Outliers detection
=========================================

In the following tutorial we'll handle time series data from a sensor. We'll see how sample the datapoints in a visually
non destructive way and

We assume that you are already familiar with logisland platform and that you have successfully done the previous tutorials.

.. note::

    You can download the `latest release <https://github.com/Hurence/logisland/releases>`_ of logisland and the `YAML configuration file <https://github.com/Hurence/logisland/blob/master/logisland-framework/logisland-resources/src/main/resources/conf/outlier-detection.yml>`_ for this tutorial which can be also found under `$LOGISLAND_HOME/conf` directory.


1. Setup the time series collection Stream
------------------------------------------
The first Stream use a `KafkaRecordStreamParallelProcessing </plugins.html#kafkarecordstreamparallelprocessing>`_
and chain of a `SplitText </plugins.html#splittext>`_

The first Processor simply parse the csv lines while the second index them into the search engine.
Please note the output schema.

.. code-block:: yaml

    # parsing time series
    - stream: parsing_stream
      component: com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing
      type: stream
      documentation: a processor that links
      configuration:
        kafka.input.topics: logisland_ts_raw
        kafka.output.topics: logisland_ts_events
        kafka.error.topics: logisland_errors
        kafka.input.topics.serializer: none
        kafka.output.topics.serializer: com.hurence.logisland.serializer.KryoSerializer
        kafka.error.topics.serializer: com.hurence.logisland.serializer.JsonSerializer
        avro.output.schema: >
          {  "version":1,
             "type": "record",
             "name": "com.hurence.logisland.record.cpu_usage",
             "fields": [
               { "name": "record_errors",   "type": [ {"type": "array", "items": "string"},"null"] },
               { "name": "record_raw_key", "type": ["string","null"] },
               { "name": "record_raw_value", "type": ["string","null"] },
               { "name": "record_id",   "type": ["string"] },
               { "name": "record_time", "type": ["long"] },
               { "name": "record_type", "type": ["string"] },
               { "name": "record_value",      "type": ["string","null"] } ]}
        kafka.metadata.broker.list: sandbox:9092
        kafka.zookeeper.quorum: sandbox:2181
        kafka.topic.autoCreate: true
        kafka.topic.default.partitions: 4
        kafka.topic.default.replicationFactor: 1
      processorConfigurations:
        - processor: apache_parser
          component: com.hurence.logisland.processor.SplitText
          type: parser
          documentation: a parser that produce events from an apache log REGEX
          configuration:
            record.type: apache_log
            value.regex: (\S+),(\S+)
            value.fields: record_time,record_value



2. Setup the Outliers detection Stream
--------------------------------------
The first Stream use a `KafkaRecordStreamParallelProcessing </plugins.html#kafkarecordstreamparallelprocessing>`_
and a `DetectOutliers </plugins.html#detectoutliers>`_  Processor

.. note::
    It's important to see that we perform outliers detection in parallel.
    So if we would perform this detection for a particular grouping of record we would have used
    a `KafkaRecordStreamSQLAggregator </plugins.html#kafkarecordstreamsqlaggregator>`_ with a ``GROUP BY`` clause instead.


.. code-block:: yaml

    # detect outliers
    - stream: detect_outliers
      component: com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing
      type: stream
      documentation: a processor that match query in parrallel
      configuration:
        kafka.input.topics: logisland_sensor_events
        kafka.output.topics: logisland_sensor_outliers_events
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
        - processor: match_query
          component: com.hurence.logisland.processor.DetectOutliers
          type: processor
          documentation: a processor that detection something exotic in a continuous time series values
          configuration:
            rotation.policy.type: by_amount
            rotation.policy.amount: 100
            rotation.policy.unit: points
            chunking.policy.type: by_amount
            chunking.policy.amount: 10
            chunking.policy.unit: points
            global.statistics.min: -100000
            min.amount.to.predict: 100
            zscore.cutoffs.normal: 3.5
            zscore.cutoffs.moderate: 5
            record.value.field: record_value
            record.time.field: record_time
            output.record.type: sensor_outlier

3. Setup the time series Sampling Stream
----------------------------------------
The first Stream use a `KafkaRecordStreamParallelProcessing </plugins.html#kafkarecordstreamparallelprocessing>`_
and a `RecordSampler </plugins.html#recordsampler>`_  Processor


.. code-block:: yaml


    # sample time series
    - stream: detect_outliers
      component: com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing
      type: stream
      documentation: a processor that match query in parrallel
      configuration:
        kafka.input.topics: logisland_sensor_events
        kafka.output.topics: logisland_sensor_sampled_events
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
        - processor: sampler
          component: com.hurence.logisland.processor.SampleRecords
          type: processor
          documentation: a processor that reduce the number of time series values
          configuration:
            record.value.field: record_value
            record.time.field: record_time
            sampling.algorithm: average
            sampling.parameter: 10


4. Setup the indexing Stream
----------------------------
The last Stream use a `KafkaRecordStreamParallelProcessing </plugins.html#kafkarecordstreamparallelprocessing>`_
and chain of a `SplitText </plugins.html#splittext>`_  and a `BulkPut </plugins.html#bulkput>`_
for indexing the whole records

.. code-block:: yaml

    # index records
    - stream: indexing_stream
      component: com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing
      type: stream
      documentation: a processor that links
      configuration:
        kafka.input.topics: logisland_sensor_events,logisland_sensor_outliers_events,logisland_sensor_sampled_events
        kafka.output.topics: none
        kafka.error.topics: logisland_errors
        kafka.input.topics.serializer: none
        kafka.input.topics.serializer: com.hurence.logisland.serializer.KryoSerializer
        kafka.output.topics.serializer: none
        kafka.error.topics.serializer: com.hurence.logisland.serializer.JsonSerializer
        kafka.metadata.broker.list: sandbox:9092
        kafka.zookeeper.quorum: sandbox:2181
        kafka.topic.autoCreate: true
        kafka.topic.default.partitions: 4
        kafka.topic.default.replicationFactor: 1
      processorConfigurations:
        - processor: es_publisher
          component: com.hurence.logisland.processor.datastore.BulkPut
          type: processor
          documentation: a processor that trace the processed events
          configuration:
            datastore.client.service: datastore_service
            default.collection: logisland
            default.type: event
            timebased.collection: yesterday
            collection.field: search_index
            type.field: record_type


4. Start logisland application
------------------------------
Connect a shell to your logisland container to launch the following stream processing job previously defined.


.. code-block:: sh

    docker exec -ti logisland bash

    #launch logisland streams
    cd $LOGISLAND_HOME
    bin/logisland.sh --conf conf/outlier-detection.yml

    # send logs to kafka
    cat cpu_utilization_asg_misconfiguration.csv | kafkacat -b sandbox:9092 -P -t logisland_sensor_raw



5. Check your alerts with Kibana
--------------------------------
