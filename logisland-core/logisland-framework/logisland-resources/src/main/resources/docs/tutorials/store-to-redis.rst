Store Apache logs to Redis K/V store
====================================

In the following getting started tutorial we'll drive you through the process of Apache log mining with LogIsland platform.

.. note::

    Be sure to know of to launch a logisland Docker environment by reading the `prerequisites <./prerequisites.html>`_ section

Note, it is possible to store data in different datastores. In this tutorial, we will see the case of Redis, if you need more in-depth explanations you can read the previous tutorial on indexing apache logs to elasticsearch or solr : `index-apache-logs.html`_ .

1. Logisland job setup
----------------------
The logisland job for this tutorial is already packaged in the tar.gz assembly and you can find it here :

.. code-block:: sh

    docker exec -i -t logisland vim conf/store-to-redis.yml

We will start by explaining each part of the config file.

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


Here the stream will read all the logs sent in ``logisland_raw`` topic and push the processing output into ``logisland_events`` topic.

.. note::

    We want to specify an Avro output schema to validate our ouput records (and force their types accordingly).
    It's really for other streams to rely on a schema when processing records from a topic.

We can define some serializers to marshall all records from and to a topic.

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

The second processor  will handle ``Records`` produced by the ``SplitText`` to index them into datastore previously defined (Redis)

.. code-block:: yaml

    # all the parsed records are added to datastore by bulk
    - processor: datastore_publisher
      component: com.hurence.logisland.processor.datastore.BulkPut
      type: processor
      documentation: "indexes processed events in datastore"
      configuration:
        datastore.client.service: datastore_service



2. Launch the script
--------------------
For this tutorial we will handle some apache logs with a splitText parser and send them to Redis
Connect a shell to your logisland container to launch the following streaming jobs.

For ElasticSearch :

.. code-block:: sh

    docker exec -i -t logisland bin/logisland.sh --conf conf/store-to-redis.yml


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

    cd /tmp
    wget ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
    gunzip NASA_access_log_Jul95.gz
    head -500000 NASA_access_log_Jul95 | kafkacat -b sandbox:9092 -t logisland_raw



4. Inspect the logs
-------------------

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
'{\n  "id" : "123493eb-93df-4e57-a1c1-4a8e844fa92c",\n  "type" : "apache_log",\n  "creationDate" : 804574829000,\n  "fields" : {\n    "src_ip" : "204.1.2.009.4",\n    "record_id" : "123493eb-93df-4e57-a1c1-4a8e844fa92c",\n    "http_method" : "GET",\n    "http_query" : "/images/WORLD-logosmall.gif",\n    "bytes_out" : "669",\n    "identd" : "-",\n    "http_version" : "HTTP/1.0",\n    "record_raw_value" : "204.191.209.4 - - [01/Jul/1995:01:00:29 -0400] \\"GET /images/WORLD-logosmall.gif HTTP/1.0\\" 200 669",\n    "http_status" : "200",\n    "record_time" : 804574829000,\n    "user" : "-",\n    "record_type" : "apache_log"\n  }\n}'

    >>> import json
    >>> record = json.loads(r.get('123493eb-93df-4e57-a1c1-4a8e844fa92c'))
    >>> record['fields']['bytes_out']

