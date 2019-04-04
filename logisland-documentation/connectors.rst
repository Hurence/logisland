
Connectors
==========

In this chapter we will present you how to integrate kafka connect connectors into logisland.

.. contents:: Table of Contents


Introduction
------------

Logisland features the integration between `kafka connect <https://www.confluent.io/product/connectors/>`_ world and the spark structured streaming engine.

In order to seamlessy integrate both world, we just wrapped out the kafka connectors interfaces (unplugging them from kafka) and let the run in a logisland spark managed container. Hence the name *"Logisland Connect"* :-)


This allows you to leverage the existing kafka connectors library to import data into a logisland pipeline without having the need to make use of any another middleware or ETL system.

Prerequisites
-------------

You can use this functionality only with a spark engine >= 2.1.x

Getting started
---------------

In order to use a kafka connect source or sink you have to package and install the required libraries to the logisland lib folder.

Hopefully it can be easily done by using the *components.sh* tool.


.. code-block:: sh

  bin/components.sh -i <plugin_artifact>


The plugin artifact should be provided according this format: *groupId:artifactId:version* where groupId, artifactId and version refer to the maven artifact you're going to install.

Some examples, with the suggested artifacts to use, in the following table:

+--------------------------+-------------------------+--------------------------------------------------------+------------------------------------------------------------------------+
| Connector                | URL                                                                              |  Artifact                                                              |
+==========================+=========================+========================================================+========================================================================+
| Simulator                | https://github.com/jcustenborder/kafka-connect-simulator                         | com.github.jcustenborder.kafka.connect:kafka-connect-simulator:0.1.118 |
+--------------------------+----------------------------------------------------------------------------------+------------------------------------------------------------------------+
| OPC-DA / OPC-UA (IIoT)   | https://github.com/Hurence/logisland                                             | com.hurence.logisland:logisland-connector-opc:<logisland_version>      |
+--------------------------+----------------------------------------------------------------------------------+------------------------------------------------------------------------+
| FTP                      | https://github.com/Eneco/kafka-connect-ftp                                       | com.eneco:kafka-connect-ftp:0.1.4                                      |
+--------------------------+----------------------------------------------------------------------------------+------------------------------------------------------------------------+
| Blockchain               | https://github.com/Landoop/stream-reactor/tree/master/kafka-connect-blockchain   | com.datamountaineer:kafka-connect-blockchain:1.1.1                     |
+--------------------------+----------------------------------------------------------------------------------+------------------------------------------------------------------------+
| JMS                      | https://github.com/Landoop/stream-reactor/tree/master/kafka-connect-jms          | com.datamountaineer:kafka-connect-jms:1.1.1                            |
+--------------------------+----------------------------------------------------------------------------------+------------------------------------------------------------------------+
| JDBC                     | https://docs.confluent.io/current/connect/connect-jdbc/docs/index.html           | io.confluent:kafka-connect-jdbc:5.0.0                                  |
+--------------------------+----------------------------------------------------------------------------------+------------------------------------------------------------------------+



Configuring
-----------

Once you have bundled the connectors you need, you are now ready to use them.

Let's do it step by step.

First of all we need to declare a *KafkaConnectStructuredSourceProviderService* or a *KafkaConnectStructuredSinkProviderService* that will manage our connector in Logisland.
Along with this we need to put some configuration (In general you can always refer to kafka connect documentation to better understand the underlying architecture and how to configure a connector):


+-------------------------------------------------+----------------------------------------------------------+
| Property                                        |    Description                                           |
+=================================================+==========================================================+
|  kc.connector.class                             | The class of the connector (Fully qualified name)        |
+-------------------------------------------------+----------------------------------------------------------+
|  kc.data.key.converter                          | The class of the converter to be used for the key.       |
|                                                 | Please refer to `Choosing the right converter`_ section  |
+-------------------------------------------------+----------------------------------------------------------+
|  kc.data.key.converter.properties               | The properties to be provided to the key converter       |
|                                                 |                                                          |
+-------------------------------------------------+----------------------------------------------------------+
|  kc.data.value.converter                        | The class of the converter to be used for the value.     |
|                                                 | Please refer to `Choosing the right converter`_ section  |
+-------------------------------------------------+----------------------------------------------------------+
|  kc.data.value.converter.properties             | The properties to be provided to the value converter     |
|                                                 |                                                          |
+-------------------------------------------------+----------------------------------------------------------+
|  kc.connector.properties                        | The properties to be provided to the connector and       |
|                                                 | specific to the connector itself.                        |
+-------------------------------------------------+----------------------------------------------------------+
|  kc.worker.tasks.max                            | How many concurrent threads to spawn for a connector     |
+-------------------------------------------------+----------------------------------------------------------+
|  kc.connector.offset.backing.store              | The offset backing store to use. Choose among:           |
|                                                 |                                                          |
|                                                 | * **memory** : standalone in memory                      |
|                                                 | * **file** : standalone file based.                      |
|                                                 | * **kafka** : distributed kafka topic based              |
|                                                 |                                                          |
|                                                 |                                                          |
+-------------------------------------------------+----------------------------------------------------------+
|  kc.connector.offset.backing.store.properties   | Specific properties to configure the chosen backing      |
|                                                 |  store.                                                  |
+-------------------------------------------------+----------------------------------------------------------+

.. note:: Please refer to `Kafka connect guide <https://docs.confluent.io/current/connect/userguide.html#running-workers>`_ for further information about offset backing store and how to configure them.


Choosing the right converter
----------------------------

Choosing the right converter is perhaps one of the most important part. In fact we're going to adapt what is coming from kafka connect to what is flowing into our logisland pipeline.
This means that we have to know how the source is managing its data.

In order to simplify your choice, we recommend you to follow this simple approach (the same applies for both keys and values):


+----------------------------+-----------------------------------+-----------------------------------+
|        Source data         |          Kafka Converter          |         Logisland Encoder         |
+============================+===================================+===================================+
|  String                    |  StringConverter                  | StringEncoder                     |
+----------------------------+-----------------------------------+-----------------------------------+
|  Raw Bytes                 |  ByteArrayConverter               | BytesArraySerialiser              |
+----------------------------+-----------------------------------+-----------------------------------+
|  Structured                |  LogIslandRecordConverter         | The serializer used by the record |
|                            |                                   | converter (*)                     |
+----------------------------+-----------------------------------+-----------------------------------+


.. note::
 (*)In case you deal with structured data, the LogIslandRecordConverter will embed the structured object in a logisland record. In order to do this you have to specify the serializer to be used to convert your data (the serializer property **record.serializer**). Generally the *KryoSerialiser* is a good choice to start with.



Putting all together
--------------------

In the previous two sections we explained how to configure a connector and how to choose the right serializer for it.

The recap we can examine the following configuration example:


.. code-block:: yaml

     # Our source service
    - controllerService: kc_source_service
      component: com.hurence.logisland.stream.spark.provider.KafkaConnectStructuredSourceProviderService
      documentation: A kafka source connector provider reading from its own source and providing structured streaming to the underlying layer
      configuration:
        # We will use the logisland record converter for both key and value
        kc.data.value.converter: com.hurence.logisland.connect.converter.LogIslandRecordConverter
        # Use kryo to serialize the inner data
        kc.data.value.converter.properties: |
          record.serializer=com.hurence.logisland.serializer.KryoSerializer
        kc.data.key.converter: com.hurence.logisland.connect.converter.LogIslandRecordConverter
        # Use kryo to serialize the inner data
        kc.data.key.converter.properties: |
          record.serializer=com.hurence.logisland.serializer.KryoSerializer
        # Only one task to handle source input (unique)
        kc.worker.tasks.max: 1
        # The kafka source connector to wrap (here we're using a simulator source)
        kc.connector.class: com.github.jcustenborder.kafka.connect.simulator.SimulatorSourceConnector
        # The properties for the connector (as per connector documentation)
        kc.connector.properties: |
          key.schema.fields=email
          topic=simulator
          value.schema.fields=email,firstName,middleName,lastName,telephoneNumber,dateOfBirth
        # We are using a standalone source for testing. We can store processed offsets in memory
        kc.connector.offset.backing.store: memory




In the example both key and value provided by the connector are structured objects.

For this reason we use for that the converter *LogIslandRecordConverter*.
As well, we provide the serializer to be used for both key and value converter specifying
*record.serializer=com.hurence.logisland.serializer.KryoSerializer* among the related converter properties.


Going further
-------------


Please do not hesitate to take a look to our kafka connect tutorials for more details and practical use cases.


