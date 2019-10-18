Threshold based alerting on Apache logs with Redis K/V store
============================================================


In a previous tutorial we saw how to use Redis K/V store as a cache storage. In this one we will practice the use of
`ComputeTag`, `CheckThresholds` and `CheckAlerts` processor in conjunction with this Redis Cache.

The following job is made of 2 streaming parts :

1. A main stream which parses Apache logs and store them to a Redis cache .
2. A timer based stream which compute some new tags values based on cached records, check some thresholds cross and send alerts if needed.

.. note::

    Be sure to know of to launch a logisland Docker environment by reading the `prerequisites <./prerequisites.html>`_ section

The full logisland job for this tutorial is already packaged in the tar.gz assembly and you can find it here :

.. code-block:: sh

    docker exec -i -t conf_logisland_1 vim conf/threshold-alerting.yml

We will start by explaining each part of the config file.

1. Controller service part
--------------------------


The `controllerServiceConfigurations` part is here to define all services that be shared by processors within the whole job, here a Redis KV cache service that will be used later in the ``BulkPut`` processor.

.. code-block:: yaml

    - controllerService: datastore_service
      component: com.hurence.logisland.redis.service.RedisKeyValueCacheService
      type: service
      documentation: redis datastore service
      configuration:
        connection.string: localhost:6379
        redis.mode: standalone
        database.index: 0
        communication.timeout: 10 seconds
        pool.max.total: 8
        pool.max.idle: 8
        pool.min.idle: 0
        pool.block.when.exhausted: true
        pool.max.wait.time: 10 seconds
        pool.min.evictable.idle.time: 60 seconds
        pool.time.between.eviction.runs: 30 seconds
        pool.num.tests.per.eviction.run: -1
        pool.test.on.create: false
        pool.test.on.borrow: false
        pool.test.on.return: false
        pool.test.while.idle: true
        record.recordSerializer: com.hurence.logisland.serializer.JsonSerializer


2. First stream : parse logs and compute tags
---------------------------------------------

Here the stream will read all the logs sent in ``logisland_raw`` topic and push the processing output into ``logisland_events`` topic as Json serialized records.

.. code-block:: yaml

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

Within this stream a ``SplitText`` processor takes a log line as a String and computes a ``Record`` as a sequence of fields.

.. code-block:: yaml

    - processor: apache_parser
      component: com.hurence.logisland.processor.SplitText
      type: parser
      documentation: a parser that produce events from an apache log REGEX
      configuration:
        value.regex: (\S+)\s+(\S+)\s+(\S+)\s+\[([\w:\/]+\s[+\-]\d{4})\]\s+"(\S+)\s+(\S+)\s*(\S*)"\s+(\S+)\s+(\S+)
        value.fields: src_ip,identd,user,record_time,http_method,http_query,http_version,http_status,bytes_out

This stream will process log entries as soon as they will be queued into `logisland_raw` Kafka topics, each log will
be parsed as an event which will be pushed back to Kafka in the ``logisland_events`` topic.

the next processing step is to  assign `bytes_out` field as `record_value`

.. code-block:: yaml

        - processor: normalize_fields
          component: com.hurence.logisland.processor.NormalizeFields
          type: parser
          documentation: change field name 'bytes_out' to `record_value`
          configuration:
            conflict.resolution.policy: overwrite_existing
            record_value: bytes_out

the we modify `record_id` to set its value as `src_ip` field.

.. code-block:: yaml

        - processor: modify_id
          component: com.hurence.logisland.processor.ModifyId
          type: parser
          documentation: change current id to src_ip
          configuration:
            id.generation.strategy: fromFields
            fields.to.hash: src_ip
            java.formatter.string: "%1$s"

now we'll remove all the unwanted fields

.. code-block:: yaml

        - processor: remove_fields
          component: com.hurence.logisland.processor.RemoveFields
          type: parser
          documentation: remove useless fields
          configuration:
            fields.to.remove: src_ip,identd,user,http_method,http_query,http_version,http_status,bytes_out

and then cast `record_value` as a double

.. code-block:: yaml

        - processor: cast
          component: com.hurence.logisland.processor.ConvertFieldsType
          type: parser
          documentation: cast values
          configuration:
            record_value: double

The next processing step wil compute a dynamic Tag value from a Javascript expression.
Here a new record with an `record_id` set to `computed1` and as a `record_value` the resulting expression of `cache("logisland.hurence.com").value * 10.2`

.. code-block:: yaml

        - processor: compute_tag
          component: com.hurence.logisland.processor.alerting.ComputeTags
          type: processor
          documentation: |
            compute tags from given formulas.
            each dynamic property will return a new record according to the formula definition
            the record name will be set to the property name
            the record time will be set to the current timestamp
          configuration:
            datastore.client.service: datastore_service
            output.record.type: computed_tag
            max.cpu.time: 500
            max.memory: 64800000
            max.prepared.statements: 5
            allow.no.brace: false
            computed1: return cache("logisland.hurence.com").value * 10.2;

The last processor will handle all the ``Records`` of this stream to index them into datastore previously defined (Redis)

.. code-block:: yaml

    # all the parsed records are added to datastore by bulk
    - processor: datastore_publisher
      component: com.hurence.logisland.processor.datastore.BulkPut
      type: processor
      documentation: "indexes processed events in datastore"
      configuration:
        datastore.client.service: datastore_service





3. Second stream : check threshold cross and alerting
-----------------------------------------------------
The second stream will read all the logs sent in ``logisland_events`` topic and push the processed outputs (threshold_cross & alerts records) into ``logisland_alerts`` topic as Json serialized records.

We won't comment the stream definition as it is really straightforward.

The first processor of this stream pipeline makes use of `CheckThresholds` component which will add a new record of type `threshold_cross` with a `record_id` set to `threshold1` if the JS expression `cache("computed1").value > 2000.0` is evaluated to true.

.. code-block:: yaml

        - processor: compute_thresholds
          component: com.hurence.logisland.processor.alerting.CheckThresholds
          type: processor
          documentation: |
            compute threshold cross from given formulas.
            each dynamic property will return a new record according to the formula definition
            the record name will be set to the property name
            the record time will be set to the current timestamp

            a threshold_cross has the following properties : count, time, duration, value
          configuration:
            datastore.client.service: datastore_service
            output.record.type: threshold_cross
            max.cpu.time: 100
            max.memory: 12800000
            max.prepared.statements: 5
            record.ttl: 300000
            threshold1: cache("computed1").value > 2000.0

.. code-block:: yaml

        - processor: compute_alerts1
          component: com.hurence.logisland.processor.alerting.CheckAlerts
          type: processor
          documentation: |
            compute threshold cross from given formulas.
            each dynamic property will return a new record according to the formula definition
            the record name will be set to the property name
            the record time will be set to the current timestamp
          configuration:
            datastore.client.service: datastore_service
            output.record.type: medium_alert
            alert.criticity: 1
            max.cpu.time: 100
            max.memory: 12800000
            max.prepared.statements: 5
            profile.activation.condition: cache("threshold1").value > 3000.0
            alert1: cache("threshold1").duration > 50.0

The last processor will handle all the ``Records`` of this stream to index them into datastore previously defined (Redis)

.. code-block:: yaml

        - processor: datastore_publisher
          component: com.hurence.logisland.processor.datastore.BulkPut
          type: processor
          documentation: "indexes processed events in datastore"
          configuration:
            datastore.client.service: datastore_service

4. Launch the script
--------------------
Connect a shell to your logisland container to launch the following streaming jobs.

.. code-block:: sh

    docker exec -i -t conf_logisland_1 bin/logisland.sh --conf conf/threshold-alerting.yml

5. Inject some Apache logs into the system
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

    cd /tmp
    wget ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
    gunzip NASA_access_log_Jul95.gz
    head -500000 NASA_access_log_Jul95 | kafkacat -b sandbox:9092 -t logisland_raw



6. Inspect the logs and alerts
------------------------------

For this part of the tutorial we will use `redis-py a Python client for Redis <https://redis-py.readthedocs.io/en/latest/>`_. You can install it by following instructions given  on `redis-py <github ²https://github.com/andymccurdy/redis-py>`_.

To install redis-py, simply:

.. code-block:: sh

    $ sudo pip install redis


Getting Started, check if you can connect with Redis

.. code-block:: python

    >>> import redis
    >>> r = redis.StrictRedis(host='localhost', port=6379, db=0)
    >>> r.set('foo', 'bar')
    >>> r.get('foo')

Then we want to grab some logs that have been collected to Redis. We first find some keys with a pattern and get the json content of one

.. code-block:: python

    >>> r.keys('1234*')
['123493eb-93df-4e57-a1c1-4a8e844fa92c', '123457d5-8ccc-4f0f-b4ba-d70967aa48eb', '12345e06-6d72-4ce8-8254-a7cc4bab5e31']

    >>> r.get('123493eb-93df-4e57-a1c1-4a8e844fa92c')
'{\n  "id" : "123493eb-93df-4e57-a1c1-4a8e844fa92c",\n  "type" : "apache_log",\n  "creationDate" : 804574829000,\n  "fields" : {\n    "src_ip" : "204.191.209.4",\n    "record_id" : "123493eb-93df-4e57-a1c1-4a8e844fa92c",\n    "http_method" : "GET",\n    "http_query" : "/images/WORLD-logosmall.gif",\n    "bytes_out" : "669",\n    "identd" : "-",\n    "http_version" : "HTTP/1.0",\n    "record_raw_value" : "204.191.209.4 - - [01/Jul/1995:01:00:29 -0400] \\"GET /images/WORLD-logosmall.gif HTTP/1.0\\" 200 669",\n    "http_status" : "200",\n    "record_time" : 804574829000,\n    "user" : "-",\n    "record_type" : "apache_log"\n  }\n}'

    >>> import json
    >>> record = json.loads(r.get('123493eb-93df-4e57-a1c1-4a8e844fa92c'))
    >>> record['fields']['bytes_out']

