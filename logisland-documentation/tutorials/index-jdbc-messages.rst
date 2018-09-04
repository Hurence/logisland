Index JDBC messages
===================

In the following getting started tutorial, we'll explain you how to read messages from a JDBC table
them into an elasticsearch store.

The JDBC data will leverage the JDBC connector available as part of logisland connect.


.. note::

    Be sure to know of to launch a logisland Docker environment by reading the `prerequisites <./prerequisites.html>`_ section

    For kafka connect related information please follow as well the `connectors <../connectors.html>`_ section.



1. Installing H2 datatabase
----------------------

In this tutorial we'll use `H2 Database <http://h2database.com/html/main.html>`_.


H2 is a Java relational database

- Very fast database engine
- Open source
- Written in Java
- Supports standard SQL, JDBC API
- Embedded and Server mode, Clustering support
- Strong security features
- The PostgreSQL ODBC driver can be used
- Multi version concurrency

you can acces the web UI through the following URL :

    http://sandbox:8181/






2. Logisland job setup
----------------------

The interesting part in this tutorial is how to setup the JMS stream.

Let's first focus on the stream configuration and then on its pipeline in order to extract the data in the right way.

==============
The JMS stream
==============

Here we are going to use a special processor (``KafkaConnectStructuredSourceProviderService``) to use the kafka connect source as input for the structured stream defined below.

Logisland ships by default a kafka connect JMS source implemented by the class *com.datamountaineer.streamreactor.connect.jms.source.JMSSourceConnector*.

.. note::
You can find more information about how to configure a JMS source in the official page of the `JMS Connector <https://lenses.stream/1.1/connectors/source/jms.html>`_


Coming back to our example, we would like to read from a queue called *test-queue* hosted in our local ActiveMQ broker.
For this we will connect as usual to its Openwire channel and we'll use client acknowledgement to be sure to have an exactly once delivery.

The kafka connect controller service configuration will look like this:

.. code-block:: yaml

    - controllerService: kc_source_service
      component: com.hurence.logisland.stream.spark.provider.KafkaConnectStructuredSourceProviderService
      configuration:
        kc.data.value.converter: com.hurence.logisland.connect.converter.LogIslandRecordConverter
        kc.data.value.converter.properties: |
          record.serializer=com.hurence.logisland.serializer.KryoSerializer
        kc.data.key.converter.properties: |
          schemas.enable=false
        kc.data.key.converter: org.apache.kafka.connect.storage.StringConverter
        kc.worker.tasks.max: 1
        kc.connector.class: com.datamountaineer.streamreactor.connect.jms.source.JMSSourceConnector
        kc.connector.offset.backing.store: memory
        kc.connector.properties: |
          connection.url=jdbc:mysql://localhost:3306/logisland?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC
          connection.user=root
          connection.password=formation
          schema.pattern=logisland
          connection.attempts=3
          connection.backoff.ms=5000
          mode=incrementing
          incrementing.column.name=bytes_out
          query=SELECT * FROM logisland.apache
          topic.prefix=jdbc-

Insert the following data into sql table



launch a mysql shell

    mysql -phurence




.. code-block:: sql

    CREATE SCHEMA IF NOT EXISTS logisland;
    USE logisland;

    DROP TABLE IF EXISTS apache;

    CREATE TABLE apache (record_id varchar(50), bytes_out integer, http_method varchar(20), http_query varchar(200), http_status varchar(10), http_version varchar(10), record_time timestamp, src_ip varchar(50), user varchar(20));

    INSERT into apache values ('ae7f8d32-c2c5-4f07-8942-c55c7f338cd0', 46888, 'GET', '/shuttle/missions/sts-71/images/KSC-95EC-0918.jpg', '200', 'HTTP/1.0', '2010-01-01 10:00:00' , 'net-1-141.eden.com', '-');


    INSERT into apache values ('400c589f-a507-45b5-80fb-1673f8c0f008' ,110,'GET','/cgi-bin/imagemap/countdown?99,176','302' ,'HTTP/1.0 ', '1995-07-01 04:01:06' ,'205.189.154.54', '-');
    INSERT into apache values ('52fd071e-0a23-4355-b7e0-4d4dc3d40244' ,12040,'GET','/shuttle/missions/sts-71/mission-sts-71.html','200','HTTP/1.0', '1995-07-01 04:04:38','pme607.onramp.awinc.com', '-');
    INSERT into apache values ('6579fe80-6f3b-40a0-88ba-cef096b8567c' ,40310,'GET','/shuttle/countdown/count.gif','200' ,'HTTP/1.0 ', '1995-07-01 04:05:18' ,'199.166.39.14', '-');
    INSERT into apache values ('a08f9757-9a55-4a2b-a74f-05f13019eb43', 141308,'GET','/images/dual-pad.gif','200' ,'HTTP/1.0 ', '1995-07-01 04:04:10' ,'isdn6-34.dnai.com', '-');
    INSERT into apache values ('6488f43e-e441-4c00-ba03-60fbe611a55b', 9867,'GET','/software/winvn/winvn.html','200' ,'HTTP/1.0 ', '1995-07-01 04:02:39' ,'dynip42.efn.org', '-');
    INSERT into apache values ('2b032466-38a1-46f2-bd68-903072fcd544', 1204,'GET','/images/KSC-logosmall.gif','200' ,'HTTP/1.0 ', '1995-07-01 04:04:34' ,'netport-27.iu.net', '-');





     INSERT into logisland.apache values ('ba75b4a2-86a7-4e06-9c17-40b5543fec8a', 73728,'GET','/msfc/astro_home3.gif','200','HTTP/1.0 ', '1995-07-01 04:03:38' ,'teleman.pr.mcs.net', '-');
     INSERT into logisland.apache values ('125717f5-9d1c-4045-aeb4-ec14fae33986', 9630,'GET','/history/apollo/images/apollo-small.gif','200','HTTP/1.0 ', '1995-07-01 04:06:05' ,'dd11-054.compuserve.com', '-');
     INSERT into logisland.apache values ('14ca31fc-8cd1-440a-a4e8-15a83100a001', 11417,'GET','/shuttle/resources/orbiters/columbia-logo.gif','200','HTTP/1.0 ', '1995-07-01 04:01:56' ,' link097.txdirect.net', '-');
     INSERT into logisland.apache values ('45c0961c-be61-4795-9312-8e2f4355c26e', 11853,'GET','/images/launchmedium.gif','200','HTTP/1.0 ', '1995-07-01 04:04:34' ,'     savvy1.savvy.com', '-');
     INSERT into logisland.apache values ('53c315e1-7e4e-4de5-a7ea-51974e844b11', 47122,'GET','shuttle/missions/sts-71/images/KSC-95EC-0868.gif','200','HTTP/1.0 ', '1995-07-01 04:02:14', 'remote27.compusmart.ab.ca', '-');


ALTER USER GUEST ADMIN TRUE


CREATE USER GUEST PASSWORD 'abc'

docker cp ./h2-1.4.197.jar logisland:/opt/logisland-0.15.0/lib
docker cp ./mysql-connector-java-8.0.12.jar logisland:/opt/logisland-0.15.0/lib


docker cp logisland-framework/logisland-resources/src/main/resources/conf/index-jdbc-messages.yml logisland:/opt/logisland-0.15.0/conf
docker exec logisland bin/logisland.sh --conf conf/index-jdbc-messages.yml




============
The pipeline
============

Within this stream, a we need to extract the data coming from the JMS.

First of all a ``FlatMap`` processor takes out the value and key (required when using *StructuredStream* as source of records)

.. code-block:: yaml

       processorConfigurations:
        - processor: flatten
          component: com.hurence.logisland.processor.FlatMap
          type: processor
          documentation: "Takes out data from record_value"
          configuration:
            keep.root.record: false


Then, since our JMS messages will carry text data, we need to extract this data from the raw message bytes:


.. code-block:: yaml


    - processor: add_fields
      component: com.hurence.logisland.processor.AddFields
      type: processor
      documentation: "Extract the message as a text"
      configuration:
      conflict.resolution.policy: overwrite_existing
      message_text: ${new String(bytes_payload)}

Now we will as well set the record time as the time when the message has been created (and not received).
This thanks to a NormalizeFields processor:

.. code-block:: yaml

  - processor: rename_fields
    component: com.hurence.logisland.processor.NormalizeFields
    type: processor
    documentation: "Change the record time according to message_timestamp field"
    configuration:
    conflict.resolution.policy: overwrite_existing
    record_time: message_timestamp

Last but not least, a ``BulkAddElasticsearch`` takes care of indexing a ``Record`` sending it to elasticsearch.

.. code-block:: yaml

       -  processor: es_publisher
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


In details, this processor makes use of a ``Elasticsearch_5_4_0_ClientService`` controller service to interact with our Elasticsearch 5.X backend
running locally (and started as part of the docker compose configuration we mentioned above).

Here below its configuration:

.. code-block:: yaml

    - controllerService: elasticsearch_service
      component: com.hurence.logisland.service.elasticsearch.Elasticsearch_5_4_0_ClientService
      type: service
      documentation: elasticsearch service
      configuration:
        hosts: sandbox:9300
        cluster.name: es-logisland
        batch.size: 5000


3. Launch the script
--------------------
Connect a shell to your logisland container to launch the following streaming jobs.

.. code-block:: sh

    bin/logisland.sh --conf conf/index-jms-messages.yml


4. Do some insights and visualizations
--------------------------------------

With ElasticSearch, you can use Kibana.

Open up your browser and go to http://sandbox:5601/app/kibana#/ and you should be able to explore the blockchain transactions.


Configure a new index pattern with ``logisland.*`` as the pattern name and ``@timestamp`` as the time value field.

.. image:: /_static/kibana-configure-index.png

Now just send some message thanks to the ActiveMQ console.

Click on the *Send* link on the top of the console main page and specify the destination to *test-queue* and type the message you like. You should have something like this:

.. image:: /_static/activemq-send-message.png

Now that the message have been consumed (you can also verify this thanks to the ActiveMQ console) you can come back to kibana and go to Explore panel for the latest 15' time window you'll only see logisland process_metrics events which give you
insights about the processing bandwidth of your streams.


.. image:: /_static/kibana-jms-records.png



5. Monitor your spark jobs and Kafka topics
-------------------------------------------
Now go to `http://sandbox:4050/streaming/ <http://sandbox:4050/streaming/>`_ to see how fast Spark can process
your data

.. image:: /_static/spark-job-monitoring.png

Another tool can help you to tweak and monitor your processing `http://sandbox:9000/ <http://sandbox:9000>`_

.. image:: /_static/kafka-mgr.png


