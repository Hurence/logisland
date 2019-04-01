Index JDBC messages
===================

In the following getting started tutorial, we'll explain you how to read messages from a JDBC table.

The JDBC data will leverage the JDBC connector available as part of logisland connect.


.. note::

    Be sure to know of to launch a logisland Docker environment by reading the `prerequisites <./prerequisites.html>`_ section

    For kafka connect related information please follow as well the `connectors <../connectors.html>`_ section.



1.Install required components
-----------------------------

For this tutorial please make sure to already have installed the kafka connect jdbc connector.

If not you can just do it through the componentes.sh command line:

.. code-block:: sh

    bin/components.sh -r com.hurence.logisland.repackaged:kafka-connect-jdbc:5.0.0


2. Installing H2 datatabase
---------------------------

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


first wee need an sql engine. Let's use an `H2 Java database<http://h2database.com/html/main.html>`_.
You can get the jar from their website and copy it to logisland lib folder inside Docker container.
Then run the server on 9999 port


.. code-block:: sh

    docker cp ./h2-1.4.197.jar logisland:/opt/logisland-1.1.0/lib
    docker exec logisland java -jar lib/h2-1.4.197.jar   -webAllowOthers -tcpAllowOthers -tcpPort 9999


You can manage your database through the web ui at `http://sandbox:8082 <http://sandbox:8082>`_

With the URL JDBC parameter set to `jdbc:h2:tcp://sandbox:9999/~/test` you should be able to connect and create the following table


.. code-block:: sql

    CREATE SCHEMA IF NOT EXISTS logisland;
    USE logisland;

    DROP TABLE IF EXISTS apache;

    CREATE TABLE apache (record_id int auto_increment primary key, bytes_out integer, http_method varchar(20), http_query varchar(200), http_status varchar(10), http_version varchar(10), record_time timestamp, src_ip varchar(50), user varchar(20));





3. Logisland job setup
----------------------

The interesting part in this tutorial is how to setup the JDBC stream.

Let's first focus on the stream configuration and then on its pipeline in order to extract the data in the right way.


Here we are going to use a special processor (``KafkaConnectStructuredSourceProviderService``) to use the kafka connect source as input for the structured stream defined below.

Logisland ships by default a kafka connect JDBC source implemented by the class *io.confluent.connect.jdbc.JdbcSourceConnector*.

.. note::
You can find more information about how to configure a JDBC source in the official page of the `JDBC Connector <https://docs.confluent.io/current/connect/connect-jdbc/docs/index.html>`_


Coming back to our example, we would like to read from a table called *logisland.apache* hosted in our local H2 database.
The kafka connect controller service configuration will look like this:

.. code-block:: yaml

    - controllerService: kc_jdbc_source
      component: com.hurence.logisland.stream.spark.provider.KafkaConnectStructuredSourceProviderService
      configuration:
        kc.data.value.converter: com.hurence.logisland.connect.converter.LogIslandRecordConverter
        kc.data.value.converter.properties: |
          record.serializer=com.hurence.logisland.serializer.KryoSerializer
        kc.data.key.converter.properties:
        kc.data.key.converter: org.apache.kafka.connect.storage.StringConverter
        kc.worker.tasks.max: 1
        kc.partitions.max: 4
        kc.connector.class: io.confluent.connect.jdbc.JdbcSourceConnector
        kc.connector.offset.backing.store: memory
        kc.connector.properties: |
          connection.url=jdbc:h2:tcp://sandbox:9999/~/test
          connection.user=sa
          connection.password=
          mode=incrementing
          incrementing.column.name=RECORD_ID
          query=SELECT * FROM LOGISLAND.APACHE
          topic.prefix=test-jdbc-

Within this stream, a we need to extract the data coming from the JDBC.

First of all a ``FlatMap`` processor takes out the value and key (required when using *StructuredStream* as source of records)

.. code-block:: yaml

       processorConfigurations:
        - processor: flatten
          component: com.hurence.logisland.processor.FlatMap
          type: processor
          documentation: "Takes out data from record_value"
          configuration:
            keep.root.record: false




4. Launch the script
--------------------
Now run the logisland job that will poll updates of new records inserted into `logisland.apache` table

.. code-block:: sh

    docker exec logisland bin/logisland.sh --conf conf/index-jdbc-messages.yml


try to insert a few rows and have a look at the console output

.. code-block:: sql

    INSERT into apache values (default, 46888, 'GET', '/shuttle/missions/sts-71/images/KSC-95EC-0918.jpg', '200', 'HTTP/1.0', '2010-01-01 10:00:00' , 'net-1-141.eden.com', '-');
    INSERT into apache values (default, 110,'GET','/cgi-bin/imagemap/countdown?99,176','302' ,'HTTP/1.0 ', '1995-07-01 04:01:06' ,'205.189.154.54', '-');
    INSERT into apache values (default,12040,'GET','/shuttle/missions/sts-71/mission-sts-71.html','200','HTTP/1.0', '1995-07-01 04:04:38','pme607.onramp.awinc.com', '-');
    INSERT into apache values (default, 40310,'GET','/shuttle/countdown/count.gif','200' ,'HTTP/1.0 ', '1995-07-01 04:05:18' ,'199.166.39.14', '-');
    INSERT into apache values (default, 141308,'GET','/images/dual-pad.gif','200' ,'HTTP/1.0 ', '1995-07-01 04:04:10' ,'isdn6-34.dnai.com', '-');
    INSERT into apache values (default,  9867,'GET','/software/winvn/winvn.html','200' ,'HTTP/1.0 ', '1995-07-01 04:02:39' ,'dynip42.efn.org', '-');
    INSERT into apache values (default, 1204,'GET','/images/KSC-logosmall.gif','200' ,'HTTP/1.0 ', '1995-07-01 04:04:34' ,'netport-27.iu.net', '-');


it should be something like the following

.. code-block:: json

    ...
    18/09/04 12:47:33 INFO DebugStream: {
      "id" : "f7690b71-f339-4a84-8bd9-a0beb9ba5f92",
      "type" : "kafka_connect",
      "creationDate" : 1536065253831,
      "fields" : {
        "record_id" : "f7690b71-f339-4a84-8bd9-a0beb9ba5f92",
        "RECORD_TIME" : 0,
        "HTTP_STATUS" : "200",
        "SRC_IP" : "netport-27.iu.net",
        "RECORD_ID" : 7,
        "HTTP_QUERY" : "/images/KSC-logosmall.gif",
        "HTTP_VERSION" : "HTTP/1.0 ",
        "USER" : "-",
        "record_time" : 1536065253831,
        "record_type" : "kafka_connect",
        "HTTP_METHOD" : "GET",
        "BYTES_OUT" : 1204
      }
    }




