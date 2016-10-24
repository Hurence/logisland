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

.. _com.hurence.logisland.processor.OutlierProcessor: 

OutlierProcessor
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

   "**Rotation Policy Type**", "...", "", "BY_AMOUNT", "", ""
   "**Rotation Policy Amount**", "...", "", "100", "", ""
   "**Rotation Policy Amount**", "...", "", "100", "", ""
   "**Chunking Policy Type**", "...", "", "BY_AMOUNT", "", ""
   "**Chunking Policy Amount**", "...", "", "100", "", ""
   "**Chunking Policy Amount**", "...", "", "100", "", ""
   "**Sketchy outlier algorithm**", "...", "SKETCHY_MOVING_MAD : , ", "SKETCHY_MOVING_MAD", "", ""
   "**Batch outlier algorithm**", "...", "RAD : , ", "RAD", "", ""

----------

.. _com.hurence.logisland.processor.elasticsearch.PutElasticsearch: 

PutElasticsearch
----------------
No description provided.

Tags
____
None.

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

