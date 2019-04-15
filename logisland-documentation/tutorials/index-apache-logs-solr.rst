==============================
Apache logs indexing with solr
==============================

In the following getting started tutorial we'll drive you through the process of Apache log mining with LogIsland platform.
The final data will be stored in solr

This tutorial is very similar to :

* `Apache logs indexing into mongodb <./index-apache-logs-mongo.html>`_
* `Apache logs indexing into elasticsearch <./index-apache-logs-es.html>`_

.. note::

    Please note that you should not launch silmutaneously several docker-compose because we are exposing local port in them. So running several
    at the same time would be conflicting. So be sure to have killed all your currently running containers.

1.Install required components
-----------------------------

* You either use docker-compose with available docker-compose-index-apache-logs-es.yml file in the tar.gz assembly in the conf folder.

    In this case you can skip this section

* Or you can launch the job in your cluster, but in this case you will have to make changes to job conf file so it works in your environment.

    In this case please make sure to already have installed solr modules (depending on which base you will use).

If not you can just do it through the components.sh command line:

.. code-block:: sh

    bin/components.sh -i com.hurence.logisland:logisland-service-mongodb-client:1.1.2

.. note::

    In the following sections we will use docker-compose to run the job. (please install it before pursuing if you are not using your own cluster)

2. Logisland job setup
----------------------

The logisland job that we will use is **./conf/index-apache-logs-solr.yml**
The logisland docker-compose file that we will use is **./conf/docker-compose-index-apache-logs-solr.yml**

We will start by explaining each part of the config file.

An Engine is needed to handle the stream processing. This ``conf/index-apache-logs-solr.yml`` configuration file defines a stream processing job setup.
The first section configures the Spark engine (we will use a `KafkaStreamProcessingEngine <../plugins.html#kafkastreamprocessingengine>`_) to run in local mode with 2 cpu cores and 2G of RAM.

.. code-block:: yaml

    engine:
      component: com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine
      type: engine
      documentation: Index some apache logs with logisland
      configuration:
        spark.app.name: IndexApacheLogsDemo
        spark.master: local[2]
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
        spark.streaming.batchDuration: 1000
        spark.streaming.backpressure.enabled: false
        spark.streaming.unpersist: false
        spark.streaming.blockInterval: 500
        spark.streaming.kafka.maxRatePerPartition: 3000
        spark.streaming.timeout: -1
        spark.streaming.kafka.maxRetries: 3
        spark.streaming.ui.retainedBatches: 200
        spark.streaming.receiver.writeAheadLog.enable: false
        spark.ui.port: 4050

The `controllerServiceConfigurations` part is here to define all services that be shared by processors within the whole job,
here an Solr service that will be used later in the ``TODO`` processor.

.. code-block:: yaml

    # Datastore service using Solr 6.6.2 - 5.5.5 also available
    - controllerService: datastore_service
      component: com.hurence.logisland.service.solr.Solr_6_6_2_ClientService
      type: service
      documentation: "SolR 6.6.2 service"
      configuration:
        solr.cloud: false
        solr.connection.string: ${SOLR_CONNECTION}
        solr.collection: solr-apache-logs
        solr.concurrent.requests: 4
        flush.interval: 2000
        batch.size: 1000

.. note::

    As you can see it uses environment variable so make sure to set them. (if you use the docker-compose file of this tutorial it is already done for you)

Inside this engine you will run a Kafka stream of processing, so we setup input/output topics and Kafka/Zookeeper hosts.
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
        kafka.metadata.broker.list: ${KAFKA_BROKERS}
        kafka.zookeeper.quorum: ${ZK_QUORUM}
        kafka.topic.autoCreate: true
        kafka.topic.default.partitions: 4
        kafka.topic.default.replicationFactor: 1

.. note::

    As you can see it uses environment variable so make sure to set them. (if you use the docker-compose file of this tutorial it is already done for you)

Within this stream a ``SplitText`` processor takes a log line as a String and computes a ``Record`` as a sequence of fields.

.. code-block:: yaml

    # parse apache logs into logisland records
    - processor: apache_parser
      component: com.hurence.logisland.processor.SplitText
      type: parser
      documentation: a parser that produce events from an apache log REGEX
      configuration:
        record.type: apache_log
        value.regex: (\S+)\s+(\S+)\s+(\S+)\s+\[([\w:\/]+\s[+\-]\d{4})\]\s+"(\S+)\s+(\S+)\s*(\S*)"\s+(\S+)\s+(\S+)
        value.fields: src_ip,identd,user,record_time,http_method,http_query,http_version,http_status,bytes_out

This stream will process log entries as soon as they will be queued into `logisland_raw` Kafka topics, each log will
be parsed as an event which will be pushed back to Kafka in the ``logisland_events`` topic.

The second processor  will handle ``Records`` produced by the ``SplitText`` to index them into solr

.. code-block:: yaml

    # all the parsed records are added to solr by bulk
    - processor: solr_publisher
      component: com.hurence.logisland.processor.datastore.BulkPut
      type: processor
      documentation: "indexes processed events in SolR"
      configuration:
        datastore.client.service: datastore_service

3. Launch the job
-----------------

---------------------
1. Run docker-compose
---------------------

For this tutorial we will handle some apache logs with a splitText parser and send them to Elastiscearch.
Launch your docker container with this command (we suppose you are in the root of the tar gz assembly) :

.. code-block:: sh

    sudo docker-compose -f ./conf/docker-compose-index-apache-logs-solr.yml up -d

Make sure all container are running and that there is no error.

.. code-block:: sh

    sudo docker-compose ps

Those containers should be visible and running

```
CONTAINER ID        IMAGE                                                 COMMAND                  CREATED             STATUS              PORTS                                                                    NAMES
0d9e02b22c38        docker.elastic.co/kibana/kibana:5.4.0                 "/bin/sh -c /usr/loc…"   13 seconds ago      Up 8 seconds        0.0.0.0:5601->5601/tcp                                                   conf_kibana_1
ab15f4b5198c        docker.elastic.co/elasticsearch/elasticsearch:5.4.0   "/bin/bash bin/es-do…"   13 seconds ago      Up 7 seconds        0.0.0.0:9200->9200/tcp, 0.0.0.0:9300->9300/tcp                           conf_elasticsearch_1
a697e45d2d1a        hurence/logisland:1.1.2                               "tail -f bin/logisla…"   13 seconds ago      Up 9 seconds        0.0.0.0:4050->4050/tcp, 0.0.0.0:8082->8082/tcp, 0.0.0.0:9999->9999/tcp   conf_logisland_1
db80cdf23b45        hurence/zookeeper                                     "/bin/sh -c '/usr/sb…"   13 seconds ago      Up 10 seconds       2888/tcp, 3888/tcp, 0.0.0.0:2181->2181/tcp, 7072/tcp                     conf_zookeeper_1
7aa7a87dd16b        hurence/kafka:0.10.2.2-scala-2.11                     "start-kafka.sh"         13 seconds ago      Up 5 seconds        0.0.0.0:9092->9092/tcp                                                   conf_kafka_1

```

.. code-block:: sh

    sudo docker logs conf_kibana_1
    sudo docker logs conf_elasticsearch_1
    sudo docker logs conf_logisland_1
    sudo docker logs conf_zookeeper_1
    sudo docker logs conf_kafka_1

Should not return errors or any suspicious messages

------------------------
2. Initializing solr db
------------------------

We will now set up our solr database. First create the 'solr-apache-logs' collection

.. code-block:: sh

    sudo docker exec -it --user=solr conf_solr_1 bin/solr create_core -c solr-apache-logs

The core/collection should have thos fields (corresponding to apache logs parsed fields) : ``src_ip``, ``identd``, ``user``, ``bytes_out``,
    ``http_method``, ``http_version``, ``http_query``, ``http_status``

Otherwise for simplicity you can add a dynamic field called '*' and of type string for this collection with the web ui :
`http://localhost:8983/solr <http://localhost:8983/solr>`_

Select the solr-apache-logs collection, go to schema and add your fields.

--------------------
3. Run logisland job
--------------------

you can now run the job inside the logisland container

.. code-block:: sh

    sudo docker exec -ti conf_logisland_1 ./bin/logisland.sh --conf ./conf/index-apache-logs-solr.yml

The last logs should be something like :

2019-03-19 16:08:47 INFO  StreamProcessingRunner:95 - awaitTermination for engine 1
2019-03-19 16:08:47 WARN  SparkContext:66 - Using an existing SparkContext; some configuration may not take effect.


4. Inject some Apache logs into the system
------------------------------------------

Now we're going to send some logs to ``logisland_raw`` Kafka topic.

If you don't have your own httpd logs available, you can use some freely available log files from
`NASA-HTTP <http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html>`_ web site access:

- `Jul 01 to Jul 31, ASCII format, 20.7 MB gzip compressed <ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz>`_
- `Aug 04 to Aug 31, ASCII format, 21.8 MB gzip compressed <ftp://ita.ee.lbl.gov/traces/NASA_access_logAug95.gz>`_

Let's send the first 500 lines of NASA http access over July 1995 to LogIsland with kafka scripts
 (available in our logisland container) to ``logisland_raw`` Kafka topic.
In another terminal run those commands

.. code-block:: sh

    sudo docker exec -ti conf_logisland_1 bash
    cd /tmp
    wget ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
    gunzip NASA_access_log_Jul95.gz
    head -n 500 NASA_access_log_Jul95 | ${KAFKA_HOME}/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic logisland_raw

The logisland job should output logs, verify that there is no error, otherwise there is chances that your solr collection is not well configured.

5. Monitor your spark jobs and Kafka topics
-------------------------------------------

Now go to `http://localhost:4050/streaming/ <http://localhost:4050/streaming/>`_ to see how fast Spark can process
your data

.. image:: /_static/spark-job-monitoring.png

6. Inspect the logs
-------------------

Solr
""""

With Solr, you can directly use the solr web ui.

Open up your browser and go to `http://localhost:8983/solr <http://localhost:8983/solr>`_ and you should be able to view your apache logs.

In non cloud mode, use the core selector, to select the core ```solr-apache-logs``` :

.. image:: /_static/solr-dashboard.png

Then, go to query and by clicking to Execute Query, you will see some data from your Apache logs :

.. image:: /_static/solr-query.png


3. Stop the job
-----------------

You can Ctr+c the console where you launched logisland job.
Then to kill all containers used run :

.. code-block:: sh

    sudo docker-compose -f ./conf/docker-compose-index-apache-logs-solr.yml down

Make sure all container have disappeared.

.. code-block:: sh

    sudo docker ps
