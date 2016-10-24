Extension plugins
=================
You'll find here the list of all usable Processors, Engines and other components taht can be usable out of the box in your analytics streams

.. _com.hurence.logisland.processor.chain.KafkaRecordStream: 

KafkaRecordStream
-----------------
No description provided.

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**kafka.error.topics**", "Sets the error topics Kafka topic name", "", "logisland_errors", "", ""
   "**kafka.input.topics**", "Sets the input Kafka topic name", "", "logisland_raw", "", ""
   "**kafka.output.topics**", "Sets the output Kafka topic name", "", "logisland_events", "", ""
   "kafka.metrics.topic", "a topic to send metrics of processing. no output if not set", "", "logisland_metrics", "", ""
   "avro.input.schema", "the avro schema definition", "", "null", "", ""
   "avro.output.schema", "the avro schema definition for the output serialization", "", "null", "", ""
   "kafka.input.topics.serializer", "No Description Provided.", "kryo serialization : serialize events as json blocs, avro serialization : serialize events as json blocs, avro serialization : serialize events as avro blocs, no serialization : send events as bytes, ", "com.hurence.logisland.serializer.KryoSerializer", "", ""
   "kafka.output.topics.serializer", "No Description Provided.", "kryo serialization : serialize events as json blocs, avro serialization : serialize events as json blocs, avro serialization : serialize events as avro blocs, no serialization : send events as bytes, ", "com.hurence.logisland.serializer.KryoSerializer", "", ""
   "kafka.error.topics.serializer", "No Description Provided.", "kryo serialization : serialize events as json blocs, avro serialization : serialize events as json blocs, avro serialization : serialize events as avro blocs, no serialization : send events as bytes, ", "com.hurence.logisland.serializer.JsonSerializer", "", ""
   "kafka.topic.autoCreate", "define wether a topic should be created automatically if not already exists", "", "true", "", ""
   "kafka.topic.default.partitions", "if autoCreate is set to true, this will set the number of partition at topic creation time", "", "8", "", ""
   "kafka.topic.default.replicationFactor", "if autoCreate is set to true, this will set the number of replica for each partition at topic creation time", "", "2", "", ""
   "**kafka.metadata.broker.list**", "a comma separated list of host:port brokers", "", "sandbox:9092", "", ""
   "**kafka.zookeeper.quorum**", "No Description Provided.", "", "sandbox:2181", "", ""
   "kafka.manual.offset.reset", "Sets manually an initial offset in ZooKeeper: smallest (automatically reset the offset to the smallest offset), largest (automatically reset the offset to the largest offset), anything else (throw exception to the consumer)", "largest offset : the offset to the largest offset, smallest offset : the offset to the smallest offset, ", "largest", "", ""

----------

.. _com.hurence.logisland.documentation.example.NakedProcessor: 

NakedProcessor
--------------
No description provided.

Tags
____
None.

Properties
__________
This component has no required or optional properties.

----------

.. _com.hurence.logisland.processor.SplitTextMultiline: 

SplitTextMultiline
------------------
No description provided.

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**regex**", "the regex to match", "", "null", "", ""
   "**fields**", "a comma separated list of fields corresponding to matching groups", "", "null", "", ""
   "**event.type**", "the type of event", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.SplitText: 

SplitText
---------
This is a processor that is used to split a String into fields according to a given mapping

Tags
____
parser, regex, log

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**value.regex**", "the regex to match for the message value", "", "null", "", ""
   "**value.fields**", "a comma separated list of fields corresponding to matching groups for the message value", "", "null", "", ""
   "key.regex", "the regex to match for the message key", "", ".*", "", ""
   "key.fields", "a comma separated list of fields corresponding to matching groups for the message key", "", "record_raw_key", "", ""
   "event.type", "default type of event", "", "event", "", ""
   "keep.raw.content", "do we add the initial raw content ?", "", "true", "", ""

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "Relationship Name", "some XPath", "Routes Records to relationships based on XPath", **true**

See Also:
_________
`com.hurence.logisland.processor.SplitTextMultiline`_ 

----------

.. _com.hurence.logisland.engine.spark.SparkStreamProcessingEngine: 

SparkStreamProcessingEngine
---------------------------
No description provided.

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**spark.app.name**", "Tha application name", "", "logisland", "", ""
   "**spark.master**", "The url to Spark Master", "", "local[2]", "", ""
   "spark.yarn.deploy-mode", "The yarn deploy mode", "", "null", "", ""
   "spark.yarn.queue", "The name of the YARN queue", "", "default", "", ""
   "spark.driver.memory", "The memory size for Spark driver", "", "512m", "", ""
   "spark.executor.memory", "The memory size for Spark executors", "", "1g", "", ""
   "spark.driver.cores", "The number of cores for Spark driver", "", "4", "", ""
   "spark.executor.cores", "The number of cores for Spark driver", "", "1", "", ""
   "spark.executor.instances", "The number of instances for Spark app", "", "null", "", ""
   "spark.serializer", "Class to use for serializing objects that will be sent over the network or need to be cached in serialized form", "", "org.apache.spark.serializer.KryoSerializer", "", ""
   "spark.streaming.blockInterval", "Interval at which data received by Spark Streaming receivers is chunked into blocks of data before storing them in Spark. Minimum recommended - 50 ms", "", "350", "", ""
   "spark.streaming.kafka.maxRatePerPartition", "Maximum rate (number of records per second) at which data will be read from each Kafka partition", "", "5000", "", ""
   "**spark.streaming.batchDuration**", "No Description Provided.", "", "2000", "", ""
   "spark.streaming.backpressure.enabled", "This enables the Spark Streaming to control the receiving rate based on the current batch scheduling delays and processing times so that the system receives only as fast as the system can process.", "", "false", "", ""
   "spark.streaming.unpersist", "Force RDDs generated and persisted by Spark Streaming to be automatically unpersisted from Spark's memory. The raw input data received by Spark Streaming is also automatically cleared. Setting this to false will allow the raw data and persisted RDDs to be accessible outside the streaming application as they will not be cleared automatically. But it comes at the cost of higher memory usage in Spark.", "", "false", "", ""
   "spark.ui.port", "No Description Provided.", "", "4050", "", ""
   "spark.streaming.timeout", "No Description Provided.", "", "-1", "", ""
   "spark.streaming.kafka.maxRetries", "Maximum rate (number of records per second) at which data will be read from each Kafka partition", "", "3", "", ""
   "spark.streaming.ui.retainedBatches", "How many batches the Spark Streaming UI and status APIs remember before garbage collecting.", "", "200", "", ""
   "spark.streaming.receiver.writeAheadLog.enable", "Enable write ahead logs for receivers. All the input data received through receivers will be saved to write ahead logs that will allow it to be recovered after driver failures.", "", "false", "", ""

----------

.. _com.hurence.logisland.engine.MockProcessingEngine: 

MockProcessingEngine
--------------------
No description provided.

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "fake.settings", "No Description Provided.", "", "oups", "", ""

----------

.. _com.hurence.logisland.processor.RandomRecordGenerator: 

RandomRecordGenerator
---------------------
No description provided.

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**avro.output.schema**", "the avro schema definition for the output serialization", "", "null", "", ""
   "**min.events.count**", "the minimum number of generated events each run", "", "10", "", ""
   "**max.events.count**", "the maximum number of generated events each run", "", "200", "", ""

----------

.. _com.hurence.logisland.processor.MockProcessor: 

MockProcessor
-------------
No description provided.

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**fake.message**", "a fake message", "", "yoyo", "", ""

----------

.. _com.hurence.logisland.engine.spark.HdfsBurnerEngine: 

HdfsBurnerEngine
----------------
No description provided.

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**spark.app.name**", "Tha application name", "", "logisland", "", ""
   "**spark.master**", "The url to Spark Master", "", "local[2]", "", ""
   "spark.yarn.deploy-mode", "The yarn deploy mode", "", "null", "", ""
   "spark.yarn.queue", "The name of the YARN queue", "", "default", "", ""
   "spark.driver.memory", "The memory size for Spark driver", "", "512m", "", ""
   "spark.executor.memory", "The memory size for Spark executors", "", "1g", "", ""
   "spark.driver.cores", "The number of cores for Spark driver", "", "4", "", ""
   "spark.executor.cores", "The number of cores for Spark driver", "", "1", "", ""
   "spark.executor.instances", "The number of instances for Spark app", "", "null", "", ""
   "spark.serializer", "Class to use for serializing objects that will be sent over the network or need to be cached in serialized form", "", "org.apache.spark.serializer.KryoSerializer", "", ""
   "spark.streaming.blockInterval", "Interval at which data received by Spark Streaming receivers is chunked into blocks of data before storing them in Spark. Minimum recommended - 50 ms", "", "350", "", ""
   "spark.streaming.kafka.maxRatePerPartition", "Maximum rate (number of records per second) at which data will be read from each Kafka partition", "", "5000", "", ""
   "**spark.streaming.batchDuration**", "No Description Provided.", "", "2000", "", ""
   "spark.streaming.backpressure.enabled", "This enables the Spark Streaming to control the receiving rate based on the current batch scheduling delays and processing times so that the system receives only as fast as the system can process.", "", "false", "", ""
   "spark.streaming.unpersist", "Force RDDs generated and persisted by Spark Streaming to be automatically unpersisted from Spark's memory. The raw input data received by Spark Streaming is also automatically cleared. Setting this to false will allow the raw data and persisted RDDs to be accessible outside the streaming application as they will not be cleared automatically. But it comes at the cost of higher memory usage in Spark.", "", "false", "", ""
   "spark.ui.port", "No Description Provided.", "", "4050", "", ""
   "spark.streaming.timeout", "No Description Provided.", "", "-1", "", ""
   "spark.streaming.kafka.maxRetries", "Maximum rate (number of records per second) at which data will be read from each Kafka partition", "", "3", "", ""
   "spark.streaming.ui.retainedBatches", "How many batches the Spark Streaming UI and status APIs remember before garbage collecting.", "", "200", "", ""
   "spark.streaming.receiver.writeAheadLog.enable", "Enable write ahead logs for receivers. All the input data received through receivers will be saved to write ahead logs that will allow it to be recovered after driver failures.", "", "false", "", ""
   "**output.folder.path**", "the location where to put files : file:///tmp/out", "", "null", "", ""
   "**output.format**", "can be parquet, orc csv", "parquet : , orc : , txt : , json : , ", "null", "", ""
   "**record.type**", "the type of event to filter", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.chain.MockProcessorChain: 

MockProcessorChain
------------------
No description provided.

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**mock.chain**", "a fake", "", "yoyo", "", ""

----------

.. _com.hurence.logisland.documentation.example.FullyDocumentedProcessor: 

FullyDocumentedProcessor
------------------------
This is a processor that is used to test documentation.

Tags
____
one, two, three

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
, and whether a property supports the  `Expression Language <expression-language.html>`_ .

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**Input Directory**", "The input directory from which to pull files", "", "null", "", "**true**"
   "**Recurse Subdirectories**", "Indicates whether or not to pull files from subdirectories", "true : Should pull from sub directories, false : Should not pull from sub directories, ", "true", "", ""
   "Optional Property", "This is a property you can use or not", "", "null", "", ""
   "**Type**", "This is the type of something that you can choose.  It has several possible values", "yes : , no : , maybe : , possibly : , not likely : , longer option name : , ", "null", "", ""

Dynamic Properties
__________________
Dynamic Properties allow the user to specify both the name and value of a property.

.. csv-table:: dynamic-properties
   :header: "Name","Value","Description","EL"
   :widths: 20,20,40,10

   "Relationship Name", "some XPath", "Routes Records to relationships based on XPath", **true**

See Also:
_________
`com.hurence.logisland.documentation.example.NakedProcessor`_ 

----------

.. _com.hurence.logisland.processor.RecordDebugger: 

RecordDebugger
--------------
No description provided.

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**event.serializer**", "the way to serialize event", "Json serialization : serialize events as json blocs, String serialization : serialize events as toString() blocs, ", "json", "", ""

----------

.. _com.hurence.logisland.engine.spark.StandardSparkStreamProcessingEngine: 

StandardSparkStreamProcessingEngine
-----------------------------------
This is a spark streaming engine

Tags
____
engine, spark, kafka

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**spark.app.name**", "Tha application name", "", "logisland", "", ""
   "**spark.master**", "The url to Spark Master", "", "local[2]", "", ""
   "spark.yarn.deploy-mode", "The yarn deploy mode", "", "null", "", ""
   "spark.yarn.queue", "The name of the YARN queue", "", "default", "", ""
   "spark.driver.memory", "The memory size for Spark driver", "", "512m", "", ""
   "spark.executor.memory", "The memory size for Spark executors", "", "1g", "", ""
   "spark.driver.cores", "The number of cores for Spark driver", "", "4", "", ""
   "spark.executor.cores", "The number of cores for Spark driver", "", "1", "", ""
   "spark.executor.instances", "The number of instances for Spark app", "", "null", "", ""
   "spark.serializer", "Class to use for serializing objects that will be sent over the network or need to be cached in serialized form", "", "org.apache.spark.serializer.KryoSerializer", "", ""
   "spark.streaming.blockInterval", "Interval at which data received by Spark Streaming receivers is chunked into blocks of data before storing them in Spark. Minimum recommended - 50 ms", "", "350", "", ""
   "spark.streaming.kafka.maxRatePerPartition", "Maximum rate (number of records per second) at which data will be read from each Kafka partition", "", "5000", "", ""
   "**spark.streaming.batchDuration**", "No Description Provided.", "", "2000", "", ""
   "spark.streaming.backpressure.enabled", "This enables the Spark Streaming to control the receiving rate based on the current batch scheduling delays and processing times so that the system receives only as fast as the system can process.", "", "false", "", ""
   "spark.streaming.unpersist", "Force RDDs generated and persisted by Spark Streaming to be automatically unpersisted from Spark's memory. The raw input data received by Spark Streaming is also automatically cleared. Setting this to false will allow the raw data and persisted RDDs to be accessible outside the streaming application as they will not be cleared automatically. But it comes at the cost of higher memory usage in Spark.", "", "false", "", ""
   "spark.ui.port", "No Description Provided.", "", "4050", "", ""
   "spark.streaming.timeout", "No Description Provided.", "", "-1", "", ""
   "spark.streaming.kafka.maxRetries", "Maximum rate (number of records per second) at which data will be read from each Kafka partition", "", "3", "", ""
   "spark.streaming.ui.retainedBatches", "How many batches the Spark Streaming UI and status APIs remember before garbage collecting.", "", "200", "", ""
   "spark.streaming.receiver.writeAheadLog.enable", "Enable write ahead logs for receivers. All the input data received through receivers will be saved to write ahead logs that will allow it to be recovered after driver failures.", "", "false", "", ""

----------

