Extension plugins
=================
You'll find here the list of all usable Processors, Engines and other components taht can be usable out of the box in your analytics streams

.. _com.hurence.logisland.documentation.example.FullyDocumentedProcessor: 

FullyDocumentedProcessor
------------------------
This is a processor that is used to test documentation.

Class
_____
com.hurence.logisland.documentation.example.FullyDocumentedProcessor

Tags
____
one, two, three

Properties
__________
This component has no required or optional properties.

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

.. _com.hurence.logisland.engine.spark.HdfsBurnerEngine: 

HdfsBurnerEngine
----------------
No description provided.

Class
_____
com.hurence.logisland.engine.spark.HdfsBurnerEngine

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

.. _com.hurence.logisland.processor.chain.KafkaRecordStream: 

KafkaRecordStream
-----------------
This is a processor chain that connects to Kafka topics

Class
_____
com.hurence.logisland.processor.chain.KafkaRecordStream

Tags
____
chain, kafka

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

.. _com.hurence.logisland.engine.MockProcessingEngine: 

MockProcessingEngine
--------------------
No description provided.

Class
_____
com.hurence.logisland.engine.MockProcessingEngine

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

.. _com.hurence.logisland.processor.MockProcessor: 

MockProcessor
-------------
This is a processor that add a fake message to each incoming records

Class
_____
com.hurence.logisland.processor.MockProcessor

Tags
____
record, mock, test

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**fake.message**", "a fake message", "", "yoyo", "", ""

----------

.. _com.hurence.logisland.processor.chain.MockProcessorChain: 

MockProcessorChain
------------------
This is a processor chain that does nothing

Class
_____
com.hurence.logisland.processor.chain.MockProcessorChain

Tags
____
record, mock, test

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**mock.chain**", "a fake", "", "yoyo", "", ""

----------

.. _com.hurence.logisland.documentation.example.NakedProcessor: 

NakedProcessor
--------------
No description provided.

Class
_____
com.hurence.logisland.documentation.example.NakedProcessor

Tags
____
None.

Properties
__________
This component has no required or optional properties.

----------

.. _com.hurence.logisland.processor.OutliersDetection: 

OutliersDetection
-----------------
Outlier Analysis: A Hybrid Approach
In order to function at scale, a two-phase approach is taken

For every data point
- Detect outlier candidates using a robust estimator of variability (e.g. median absolute deviation) that uses distributional sketching (e.g. Q-trees)
- Gather a biased sample (biased by recency)
- Extremely deterministic in space and cheap in computation

For every outlier candidate
- Use traditional, more computationally complex approaches to outlier analysis (e.g. Robust PCA) on the biased sample
- Expensive computationally, but run infrequently

This becomes a data filter which can be attached to a timeseries data stream within a distributed computational framework (i.e. Storm, Spark, Flink, NiFi) to detect outliers.

Class
_____
com.hurence.logisland.processor.OutliersDetection

Tags
____
analytic, outlier, record, iot, timeseries

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**value.field**", "the numeric field to get the value", "", "record_value", "", ""
   "**time.field**", "the numeric field to get the value", "", "record_time", "", ""
   "**rotation.policy.type**", "...", "by_amount : , by_time : , never : , ", "by_amount", "", ""
   "**rotation.policy.amount**", "...", "", "100", "", ""
   "**rotation.policy.unit**", "...", "milliseconds : , seconds : , hours : , days : , months : , years : , points : , ", "points", "", ""
   "**chunking.policy.type**", "...", "by_amount : , by_time : , never : , ", "by_amount", "", ""
   "**chunking.policy.amount**", "...", "", "100", "", ""
   "**chunking.policy.unit**", "...", "milliseconds : , seconds : , hours : , days : , months : , years : , points : , ", "points", "", ""
   "sketchy.outlier.algorithm", "...", "SKETCHY_MOVING_MAD : , ", "SKETCHY_MOVING_MAD", "", ""
   "batch.outlier.algorithm", "...", "RAD : , ", "RAD", "", ""
   "global.statistics.min", "minimum value", "", "null", "", ""
   "global.statistics.max", "maximum value", "", "null", "", ""
   "global.statistics.mean", "mean value", "", "null", "", ""
   "global.statistics.stddev", "standard deviation value", "", "null", "", ""
   "**zscore.cutoffs.normal**", "zscoreCutoffs levele for normal outlier", "", "0.000000000000001", "", ""
   "**zscore.cutoffs.moderate**", "zscoreCutoffs levele for moderate outlier", "", "1.5", "", ""
   "**zscore.cutoffs.severe**", "zscoreCutoffs levele for severe outlier", "", "10.0", "", ""
   "zscore.cutoffs.notEnoughData", "zscoreCutoffs levele for notEnoughData outlier", "", "100", "", ""
   "smooth", "do smoothing ?", "", "false", "", ""
   "decay", "the decay", "", "0.1", "", ""
   "**min.amount.to.predict**", "minAmountToPredict", "", "100", "", ""
   "min_zscore_percentile", "minZscorePercentile", "", "50.0", "", ""
   "reservoir_size", "the size of points reservoir", "", "100", "", ""
   "rpca.force.diff", "No Description Provided.", "", "null", "", ""
   "rpca.lpenalty", "No Description Provided.", "", "null", "", ""
   "rpca.min.records", "No Description Provided.", "", "null", "", ""
   "rpca.spenalty", "No Description Provided.", "", "null", "", ""
   "rpca.threshold", "No Description Provided.", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.elasticsearch.PutElasticsearch: 

PutElasticsearch
----------------
This is a processor that puts records to ES

Class
_____
com.hurence.logisland.processor.elasticsearch.PutElasticsearch

Tags
____
record, elasticsearch, sink, record

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
, whether a property supports the  `Expression Language <expression-language.html>`_ , and whether a property is considered "sensitive", meaning that its value will be encrypted. Before entering a value in a sensitive property, ensure that the **logisland.properties** file has an entry for the property **logisland.sensitive.props.key**.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**cluster.name**", "Name of the ES cluster (for example, elasticsearch_brew). Defaults to 'elasticsearch'", "", "elasticsearch", "", ""
   "**hosts**", "ElasticSearch Hosts, which should be comma separated and colon for hostname/port host1:port,host2:port,....  For example testcluster:9300.", "", "null", "", ""
   "ssl.context.service", "The SSL Context Service used to provide client certificate information for TLS/SSL connections. This service only applies if the Shield plugin is available.", "", "null", "", ""
   "shield.location", "Specifies the path to the JAR for the Elasticsearch Shield plugin. If the Elasticsearch cluster has been secured with the Shield plugin, then the Shield plugin JAR must also be available to this processor. Note: Do NOT place the Shield JAR into NiFi's lib/ directory, doing so will prevent the Shield plugin from being loaded.", "", "null", "", ""
   "username", "Username to access the Elasticsearch cluster", "", "null", "", ""
   "password", "Password to access the Elasticsearch cluster", "", "null", "**true**", ""
   "**ping.timeout**", "The ping timeout used to determine when a node is unreachable. For example, 5s (5 seconds). If non-local recommended is 30s", "", "5s", "", ""
   "**sampler.interval**", "How often to sample / ping the nodes listed and connected. For example, 5s (5 seconds). If non-local recommended is 30s.", "", "5s", "", ""
   "**default.index**", "The name of the index to insert into", "", "null", "", "**true**"
   "**default.type**", "The type of this document (used by Elasticsearch for indexing and searching)", "", "null", "", "**true**"
   "**charset**", "Specifies the character set of the document data.", "", "UTF-8", "", ""
   "batch.size", "The preferred number of Records to setField to the database in a single transaction", "", "1000", "", ""
   "bulk.size", "bulk size in MB", "", "5", "", ""
   "concurrent.requests", "setConcurrentRequests", "", "2", "", ""
   "flush.interval", "flush interval in sec", "", "5", "", ""
   "**timebased.index**", "do we add a date suffix", "No date : no date added to default index, Today's date : today's date added to default index, yesterday's date : yesterday's date added to default index, ", "no", "", ""
   "es.index.field", "the name of the event field containing es index type => will override index value if set", "", "null", "", ""
   "es.type.field", "the name of the event field containing es doc type => will override type value if set", "", "null", "", ""

----------

.. _com.hurence.logisland.processor.QueryMatcherProcessor: 

QueryMatcherProcessor
---------------------
No description provided.

Class
_____
com.hurence.logisland.processor.QueryMatcherProcessor

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

   "**Luwak rules**", "a comma separated string of rules", "", "*", "", ""

----------

.. _com.hurence.logisland.processor.RandomRecordGenerator: 

RandomRecordGenerator
---------------------
This is a processor that make random records given an Avro schema

Class
_____
com.hurence.logisland.processor.RandomRecordGenerator

Tags
____
record, avro, generator

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

.. _com.hurence.logisland.processor.RecordDebugger: 

RecordDebugger
--------------
This is a processor that logs incoming records

Class
_____
com.hurence.logisland.processor.RecordDebugger

Tags
____
record, debug

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values
.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10

   "**event.serializer**", "the way to serialize event", "Json serialization : serialize events as json blocs, String serialization : serialize events as toString() blocs, ", "json", "", ""

----------

.. _com.hurence.logisland.engine.spark.SparkStreamProcessingEngine: 

SparkStreamProcessingEngine
---------------------------
No description provided.

Class
_____
com.hurence.logisland.engine.spark.SparkStreamProcessingEngine

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
   "spark.ui.showConsoleProgress", "enable disable progress bar logging", "", "false", "", ""

----------

.. _com.hurence.logisland.processor.SplitText: 

SplitText
---------
This is a processor that is used to split a String into fields according to a given Record mapping

Class
_____
com.hurence.logisland.processor.SplitText

Tags
____
parser, regex, log, record

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
   "record.type", "default type of record", "", "record", "", ""
   "keep.raw.content", "do we add the initial raw content ?", "", "true", "", ""

See Also:
_________
`com.hurence.logisland.processor.SplitTextMultiline`_ 

----------

.. _com.hurence.logisland.processor.SplitTextMultiline: 

SplitTextMultiline
------------------
No description provided.

Class
_____
com.hurence.logisland.processor.SplitTextMultiline

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

.. _com.hurence.logisland.engine.spark.StandardSparkStreamProcessingEngine: 

StandardSparkStreamProcessingEngine
-----------------------------------
This is a spark streaming engine

Class
_____
com.hurence.logisland.engine.spark.StandardSparkStreamProcessingEngine

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

