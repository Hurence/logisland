.. _user-engine-spark:

Engine-spark
==========


----------

.. _com.hurence.logisland.stream.spark.structured.provider.ConsoleStructuredStreamProviderService: 

ConsoleStructuredStreamProviderService
--------------------------------------
No description provided.

Class
_____
com.hurence.logisland.stream.spark.structured.provider.ConsoleStructuredStreamProviderService

Tags
____
None.

Properties
__________
This component has no required or optional properties.

----------

.. _com.hurence.logisland.stream.spark.DummyRecordStream: 

DummyRecordStream
-----------------
No description provided.

Class
_____
com.hurence.logisland.stream.spark.DummyRecordStream

Tags
____
None.

Properties
__________
This component has no required or optional properties.

----------

.. _com.hurence.logisland.stream.spark.provider.KafkaConnectBaseProviderService: 

KafkaConnectBaseProviderService
-------------------------------
No description provided.

Class
_____
com.hurence.logisland.stream.spark.provider.KafkaConnectBaseProviderService

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10
   :escape: \

   "**kc.connector.class**", "The class canonical name of the kafka connector to use.", "", "null", "false", "false"
   "kc.connector.properties", "The properties (key=value) for the connector.", "", "", "false", "false"
   "**kc.data.key.converter**", "Key converter class", "", "null", "false", "false"
   "kc.data.key.converter.properties", "Key converter properties", "", "", "false", "false"
   "**kc.data.value.converter**", "Value converter class", "", "null", "false", "false"
   "kc.data.value.converter.properties", "Value converter properties", "", "", "false", "false"
   "**kc.worker.tasks.max**", "Max number of threads for this connector", "", "1", "false", "false"
   "kc.partitions.max", "Max number of partitions for this connector.", "", "null", "false", "false"
   "kc.connector.offset.backing.store", "The underlying backing store to be used.", "memory (Standalone in memory offset backing store. Not suitable for clustered deployments unless source is unique or stateless), file (Standalone filesystem based offset backing store. You have to specify the property offset.storage.file.filename for the file path.Not suitable for clustered deployments unless source is unique or standalone), kafka (Distributed kafka topic based offset backing store. See the javadoc of class org.apache.kafka.connect.storage.KafkaOffsetBackingStore for the configuration options.This backing store is well suited for distributed deployments.)", "memory", "false", "false"
   "kc.connector.offset.backing.store.properties", "Properties to configure the offset backing store", "", "", "false", "false"

----------

.. _com.hurence.logisland.stream.spark.provider.KafkaConnectStructuredSinkProviderService: 

KafkaConnectStructuredSinkProviderService
-----------------------------------------
No description provided.

Class
_____
com.hurence.logisland.stream.spark.provider.KafkaConnectStructuredSinkProviderService

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10
   :escape: \

   "**kc.connector.class**", "The class canonical name of the kafka connector to use.", "", "null", "false", "false"
   "kc.connector.properties", "The properties (key=value) for the connector.", "", "", "false", "false"
   "**kc.data.key.converter**", "Key converter class", "", "null", "false", "false"
   "kc.data.key.converter.properties", "Key converter properties", "", "", "false", "false"
   "**kc.data.value.converter**", "Value converter class", "", "null", "false", "false"
   "kc.data.value.converter.properties", "Value converter properties", "", "", "false", "false"
   "**kc.worker.tasks.max**", "Max number of threads for this connector", "", "1", "false", "false"
   "kc.partitions.max", "Max number of partitions for this connector.", "", "null", "false", "false"
   "kc.connector.offset.backing.store", "The underlying backing store to be used.", "memory (Standalone in memory offset backing store. Not suitable for clustered deployments unless source is unique or stateless), file (Standalone filesystem based offset backing store. You have to specify the property offset.storage.file.filename for the file path.Not suitable for clustered deployments unless source is unique or standalone), kafka (Distributed kafka topic based offset backing store. See the javadoc of class org.apache.kafka.connect.storage.KafkaOffsetBackingStore for the configuration options.This backing store is well suited for distributed deployments.)", "memory", "false", "false"
   "kc.connector.offset.backing.store.properties", "Properties to configure the offset backing store", "", "", "false", "false"

----------

.. _com.hurence.logisland.stream.spark.provider.KafkaConnectStructuredSourceProviderService: 

KafkaConnectStructuredSourceProviderService
-------------------------------------------
No description provided.

Class
_____
com.hurence.logisland.stream.spark.provider.KafkaConnectStructuredSourceProviderService

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10
   :escape: \

   "**kc.connector.class**", "The class canonical name of the kafka connector to use.", "", "null", "false", "false"
   "kc.connector.properties", "The properties (key=value) for the connector.", "", "", "false", "false"
   "**kc.data.key.converter**", "Key converter class", "", "null", "false", "false"
   "kc.data.key.converter.properties", "Key converter properties", "", "", "false", "false"
   "**kc.data.value.converter**", "Value converter class", "", "null", "false", "false"
   "kc.data.value.converter.properties", "Value converter properties", "", "", "false", "false"
   "**kc.worker.tasks.max**", "Max number of threads for this connector", "", "1", "false", "false"
   "kc.partitions.max", "Max number of partitions for this connector.", "", "null", "false", "false"
   "kc.connector.offset.backing.store", "The underlying backing store to be used.", "memory (Standalone in memory offset backing store. Not suitable for clustered deployments unless source is unique or stateless), file (Standalone filesystem based offset backing store. You have to specify the property offset.storage.file.filename for the file path.Not suitable for clustered deployments unless source is unique or standalone), kafka (Distributed kafka topic based offset backing store. See the javadoc of class org.apache.kafka.connect.storage.KafkaOffsetBackingStore for the configuration options.This backing store is well suited for distributed deployments.)", "memory", "false", "false"
   "kc.connector.offset.backing.store.properties", "Properties to configure the offset backing store", "", "", "false", "false"

----------

.. _com.hurence.logisland.stream.spark.KafkaRecordStreamDebugger: 

KafkaRecordStreamDebugger
-------------------------
No description provided.

Class
_____
com.hurence.logisland.stream.spark.KafkaRecordStreamDebugger

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10
   :escape: \

   "**kafka.error.topics**", "Sets the error topics Kafka topic name", "", "_errors", "false", "false"
   "**kafka.input.topics**", "Sets the input Kafka topic name", "", "_raw", "false", "false"
   "**kafka.output.topics**", "Sets the output Kafka topic name", "", "_records", "false", "false"
   "avro.input.schema", "the avro schema definition", "", "null", "false", "false"
   "avro.output.schema", "the avro schema definition for the output serialization", "", "null", "false", "false"
   "kafka.input.topics.serializer", "No Description Provided.", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes)", "com.hurence.logisland.serializer.KryoSerializer", "false", "false"
   "kafka.output.topics.serializer", "No Description Provided.", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes)", "com.hurence.logisland.serializer.KryoSerializer", "false", "false"
   "kafka.error.topics.serializer", "No Description Provided.", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes)", "com.hurence.logisland.serializer.JsonSerializer", "false", "false"
   "kafka.topic.autoCreate", "define wether a topic should be created automatically if not already exists", "", "true", "false", "false"
   "kafka.topic.default.partitions", "if autoCreate is set to true, this will set the number of partition at topic creation time", "", "20", "false", "false"
   "kafka.topic.default.replicationFactor", "if autoCreate is set to true, this will set the number of replica for each partition at topic creation time", "", "3", "false", "false"
   "**kafka.metadata.broker.list**", "a comma separated list of host:port brokers", "", "sandbox:9092", "false", "false"
   "**kafka.zookeeper.quorum**", "No Description Provided.", "", "sandbox:2181", "false", "false"
   "kafka.manual.offset.reset", "What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted):

   earliest: automatically reset the offset to the earliest offset

   latest: automatically reset the offset to the latest offset

   none: throw exception to the consumer if no previous offset is found for the consumer's group

   anything else: throw exception to the consumer.", "latest (the offset to the latest offset), earliest (the offset to the earliest offset), none (the latest saved  offset)", "earliest", "false", "false"
   "kafka.batch.size", "measures batch size in total bytes instead of the number of messages. It controls how many bytes of data to collect before sending messages to the Kafka broker. Set this as high as possible, without exceeding available memory. The default value is 16384.

   

   If you increase the size of your buffer, it might never get full.The Producer sends the information eventually, based on other triggers, such as linger time in milliseconds. Although you can impair memory usage by setting the buffer batch size too high, this does not impact latency.

   

   If your producer is sending all the time, you are probably getting the best throughput possible. If the producer is often idle, you might not be writing enough data to warrant the current allocation of resources.", "", "16384", "false", "false"
   "kafka.linger.ms", "linger.ms sets the maximum time to buffer data in asynchronous mode. For example, a setting of 100 batches 100ms of messages to send at once. This improves throughput, but the buffering adds message delivery latency.

   

   By default, the producer does not wait. It sends the buffer any time data is available.

   

   Instead of sending immediately, you can set linger.ms to 5 and send more messages in one batch. This would reduce the number of requests sent, but would add up to 5 milliseconds of latency to records sent, even if the load on the system does not warrant the delay.

   

   The farther away the broker is from the producer, the more overhead required to send messages. Increase linger.ms for higher latency and higher throughput in your producer.", "", "5", "false", "false"
   "kafka.acks", "The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the  durability of records that are sent. The following settings are common:  <ul> <li><code>acks=0</code> If set to zero then the producer will not wait for any acknowledgment from the server at all. The record will be immediately added to the socket buffer and considered sent. No guarantee can be made that the server has received the record in this case, and the <code>retries</code> configuration will not take effect (as the client won't generally know of any failures). The offset given back for each record will always be set to -1. <li><code>acks=1</code> This will mean the leader will write the record to its local log but will respond without awaiting full acknowledgement from all followers. In this case should the leader fail immediately after acknowledging the record but before the followers have replicated it then the record will be lost. <li><code>acks=all</code> This means the leader will wait for the full set of in-sync replicas to acknowledge the record. This guarantees that the record will not be lost as long as at least one in-sync replica remains alive. This is the strongest available guarantee.", "", "all", "false", "false"
   "window.duration", "all the elements in seen in a sliding window of time over. windowDuration = width of the window; must be a multiple of batching interval", "", "null", "false", "false"
   "slide.duration", "sliding interval of the window (i.e., the interval after which  the new DStream will generate RDDs); must be a multiple of batching interval", "", "null", "false", "false"

----------

.. _com.hurence.logisland.stream.spark.KafkaRecordStreamHDFSBurner: 

KafkaRecordStreamHDFSBurner
---------------------------
No description provided.

Class
_____
com.hurence.logisland.stream.spark.KafkaRecordStreamHDFSBurner

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10
   :escape: \

   "**kafka.error.topics**", "Sets the error topics Kafka topic name", "", "_errors", "false", "false"
   "**kafka.input.topics**", "Sets the input Kafka topic name", "", "_raw", "false", "false"
   "**kafka.output.topics**", "Sets the output Kafka topic name", "", "_records", "false", "false"
   "avro.input.schema", "the avro schema definition", "", "null", "false", "false"
   "avro.output.schema", "the avro schema definition for the output serialization", "", "null", "false", "false"
   "kafka.input.topics.serializer", "No Description Provided.", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes)", "com.hurence.logisland.serializer.KryoSerializer", "false", "false"
   "kafka.output.topics.serializer", "No Description Provided.", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes)", "com.hurence.logisland.serializer.KryoSerializer", "false", "false"
   "kafka.error.topics.serializer", "No Description Provided.", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes)", "com.hurence.logisland.serializer.JsonSerializer", "false", "false"
   "kafka.topic.autoCreate", "define wether a topic should be created automatically if not already exists", "", "true", "false", "false"
   "kafka.topic.default.partitions", "if autoCreate is set to true, this will set the number of partition at topic creation time", "", "20", "false", "false"
   "kafka.topic.default.replicationFactor", "if autoCreate is set to true, this will set the number of replica for each partition at topic creation time", "", "3", "false", "false"
   "**kafka.metadata.broker.list**", "a comma separated list of host:port brokers", "", "sandbox:9092", "false", "false"
   "**kafka.zookeeper.quorum**", "No Description Provided.", "", "sandbox:2181", "false", "false"
   "kafka.manual.offset.reset", "What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted):

   earliest: automatically reset the offset to the earliest offset

   latest: automatically reset the offset to the latest offset

   none: throw exception to the consumer if no previous offset is found for the consumer's group

   anything else: throw exception to the consumer.", "latest (the offset to the latest offset), earliest (the offset to the earliest offset), none (the latest saved  offset)", "earliest", "false", "false"
   "kafka.batch.size", "measures batch size in total bytes instead of the number of messages. It controls how many bytes of data to collect before sending messages to the Kafka broker. Set this as high as possible, without exceeding available memory. The default value is 16384.

   

   If you increase the size of your buffer, it might never get full.The Producer sends the information eventually, based on other triggers, such as linger time in milliseconds. Although you can impair memory usage by setting the buffer batch size too high, this does not impact latency.

   

   If your producer is sending all the time, you are probably getting the best throughput possible. If the producer is often idle, you might not be writing enough data to warrant the current allocation of resources.", "", "16384", "false", "false"
   "kafka.linger.ms", "linger.ms sets the maximum time to buffer data in asynchronous mode. For example, a setting of 100 batches 100ms of messages to send at once. This improves throughput, but the buffering adds message delivery latency.

   

   By default, the producer does not wait. It sends the buffer any time data is available.

   

   Instead of sending immediately, you can set linger.ms to 5 and send more messages in one batch. This would reduce the number of requests sent, but would add up to 5 milliseconds of latency to records sent, even if the load on the system does not warrant the delay.

   

   The farther away the broker is from the producer, the more overhead required to send messages. Increase linger.ms for higher latency and higher throughput in your producer.", "", "5", "false", "false"
   "kafka.acks", "The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the  durability of records that are sent. The following settings are common:  <ul> <li><code>acks=0</code> If set to zero then the producer will not wait for any acknowledgment from the server at all. The record will be immediately added to the socket buffer and considered sent. No guarantee can be made that the server has received the record in this case, and the <code>retries</code> configuration will not take effect (as the client won't generally know of any failures). The offset given back for each record will always be set to -1. <li><code>acks=1</code> This will mean the leader will write the record to its local log but will respond without awaiting full acknowledgement from all followers. In this case should the leader fail immediately after acknowledging the record but before the followers have replicated it then the record will be lost. <li><code>acks=all</code> This means the leader will wait for the full set of in-sync replicas to acknowledge the record. This guarantees that the record will not be lost as long as at least one in-sync replica remains alive. This is the strongest available guarantee.", "", "all", "false", "false"
   "window.duration", "all the elements in seen in a sliding window of time over. windowDuration = width of the window; must be a multiple of batching interval", "", "null", "false", "false"
   "slide.duration", "sliding interval of the window (i.e., the interval after which  the new DStream will generate RDDs); must be a multiple of batching interval", "", "null", "false", "false"
   "**output.folder.path**", "the location where to put files : file:///tmp/out", "", "null", "false", "false"
   "**output.format**", "can be parquet, orc csv", "parquet, txt, json, json", "null", "false", "false"
   "**record.type**", "the type of event to filter", "", "null", "false", "false"
   "num.partitions", "the numbers of physical files on HDFS", "", "4", "false", "false"
   "exclude.errors", "do we include records with errors ?", "", "true", "false", "false"
   "date.format", "The format of the date for the partition", "", "yyyy-MM-dd", "false", "false"
   "input.format", "Used to load data from a raw record_value. Only json supported", "", "", "false", "false"

----------

.. _com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing: 

KafkaRecordStreamParallelProcessing
-----------------------------------
No description provided.

Class
_____
com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10
   :escape: \

   "**kafka.error.topics**", "Sets the error topics Kafka topic name", "", "_errors", "false", "false"
   "**kafka.input.topics**", "Sets the input Kafka topic name", "", "_raw", "false", "false"
   "**kafka.output.topics**", "Sets the output Kafka topic name", "", "_records", "false", "false"
   "avro.input.schema", "the avro schema definition", "", "null", "false", "false"
   "avro.output.schema", "the avro schema definition for the output serialization", "", "null", "false", "false"
   "kafka.input.topics.serializer", "No Description Provided.", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes)", "com.hurence.logisland.serializer.KryoSerializer", "false", "false"
   "kafka.output.topics.serializer", "No Description Provided.", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes)", "com.hurence.logisland.serializer.KryoSerializer", "false", "false"
   "kafka.error.topics.serializer", "No Description Provided.", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes)", "com.hurence.logisland.serializer.JsonSerializer", "false", "false"
   "kafka.topic.autoCreate", "define wether a topic should be created automatically if not already exists", "", "true", "false", "false"
   "kafka.topic.default.partitions", "if autoCreate is set to true, this will set the number of partition at topic creation time", "", "20", "false", "false"
   "kafka.topic.default.replicationFactor", "if autoCreate is set to true, this will set the number of replica for each partition at topic creation time", "", "3", "false", "false"
   "**kafka.metadata.broker.list**", "a comma separated list of host:port brokers", "", "sandbox:9092", "false", "false"
   "**kafka.zookeeper.quorum**", "No Description Provided.", "", "sandbox:2181", "false", "false"
   "kafka.manual.offset.reset", "What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted):

   earliest: automatically reset the offset to the earliest offset

   latest: automatically reset the offset to the latest offset

   none: throw exception to the consumer if no previous offset is found for the consumer's group

   anything else: throw exception to the consumer.", "latest (the offset to the latest offset), earliest (the offset to the earliest offset), none (the latest saved  offset)", "earliest", "false", "false"
   "kafka.batch.size", "measures batch size in total bytes instead of the number of messages. It controls how many bytes of data to collect before sending messages to the Kafka broker. Set this as high as possible, without exceeding available memory. The default value is 16384.

   

   If you increase the size of your buffer, it might never get full.The Producer sends the information eventually, based on other triggers, such as linger time in milliseconds. Although you can impair memory usage by setting the buffer batch size too high, this does not impact latency.

   

   If your producer is sending all the time, you are probably getting the best throughput possible. If the producer is often idle, you might not be writing enough data to warrant the current allocation of resources.", "", "16384", "false", "false"
   "kafka.linger.ms", "linger.ms sets the maximum time to buffer data in asynchronous mode. For example, a setting of 100 batches 100ms of messages to send at once. This improves throughput, but the buffering adds message delivery latency.

   

   By default, the producer does not wait. It sends the buffer any time data is available.

   

   Instead of sending immediately, you can set linger.ms to 5 and send more messages in one batch. This would reduce the number of requests sent, but would add up to 5 milliseconds of latency to records sent, even if the load on the system does not warrant the delay.

   

   The farther away the broker is from the producer, the more overhead required to send messages. Increase linger.ms for higher latency and higher throughput in your producer.", "", "5", "false", "false"
   "kafka.acks", "The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the  durability of records that are sent. The following settings are common:  <ul> <li><code>acks=0</code> If set to zero then the producer will not wait for any acknowledgment from the server at all. The record will be immediately added to the socket buffer and considered sent. No guarantee can be made that the server has received the record in this case, and the <code>retries</code> configuration will not take effect (as the client won't generally know of any failures). The offset given back for each record will always be set to -1. <li><code>acks=1</code> This will mean the leader will write the record to its local log but will respond without awaiting full acknowledgement from all followers. In this case should the leader fail immediately after acknowledging the record but before the followers have replicated it then the record will be lost. <li><code>acks=all</code> This means the leader will wait for the full set of in-sync replicas to acknowledge the record. This guarantees that the record will not be lost as long as at least one in-sync replica remains alive. This is the strongest available guarantee.", "", "all", "false", "false"
   "window.duration", "all the elements in seen in a sliding window of time over. windowDuration = width of the window; must be a multiple of batching interval", "", "null", "false", "false"
   "slide.duration", "sliding interval of the window (i.e., the interval after which  the new DStream will generate RDDs); must be a multiple of batching interval", "", "null", "false", "false"

----------

.. _com.hurence.logisland.stream.spark.KafkaRecordStreamSQLAggregator: 

KafkaRecordStreamSQLAggregator
------------------------------
This is a stream capable of SQL query interpretations.

Class
_____
com.hurence.logisland.stream.spark.KafkaRecordStreamSQLAggregator

Tags
____
stream, SQL, query, record

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10
   :escape: \

   "**kafka.error.topics**", "Sets the error topics Kafka topic name", "", "_errors", "false", "false"
   "**kafka.input.topics**", "Sets the input Kafka topic name", "", "_raw", "false", "false"
   "**kafka.output.topics**", "Sets the output Kafka topic name", "", "_records", "false", "false"
   "avro.input.schema", "the avro schema definition", "", "null", "false", "false"
   "avro.output.schema", "the avro schema definition for the output serialization", "", "null", "false", "false"
   "kafka.input.topics.serializer", "No Description Provided.", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes)", "com.hurence.logisland.serializer.KryoSerializer", "false", "false"
   "kafka.output.topics.serializer", "No Description Provided.", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes)", "com.hurence.logisland.serializer.KryoSerializer", "false", "false"
   "kafka.error.topics.serializer", "No Description Provided.", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes)", "com.hurence.logisland.serializer.JsonSerializer", "false", "false"
   "kafka.topic.autoCreate", "define wether a topic should be created automatically if not already exists", "", "true", "false", "false"
   "kafka.topic.default.partitions", "if autoCreate is set to true, this will set the number of partition at topic creation time", "", "20", "false", "false"
   "kafka.topic.default.replicationFactor", "if autoCreate is set to true, this will set the number of replica for each partition at topic creation time", "", "3", "false", "false"
   "**kafka.metadata.broker.list**", "a comma separated list of host:port brokers", "", "sandbox:9092", "false", "false"
   "**kafka.zookeeper.quorum**", "No Description Provided.", "", "sandbox:2181", "false", "false"
   "kafka.manual.offset.reset", "What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted):

   earliest: automatically reset the offset to the earliest offset

   latest: automatically reset the offset to the latest offset

   none: throw exception to the consumer if no previous offset is found for the consumer's group

   anything else: throw exception to the consumer.", "latest (the offset to the latest offset), earliest (the offset to the earliest offset), none (the latest saved  offset)", "earliest", "false", "false"
   "kafka.batch.size", "measures batch size in total bytes instead of the number of messages. It controls how many bytes of data to collect before sending messages to the Kafka broker. Set this as high as possible, without exceeding available memory. The default value is 16384.

   

   If you increase the size of your buffer, it might never get full.The Producer sends the information eventually, based on other triggers, such as linger time in milliseconds. Although you can impair memory usage by setting the buffer batch size too high, this does not impact latency.

   

   If your producer is sending all the time, you are probably getting the best throughput possible. If the producer is often idle, you might not be writing enough data to warrant the current allocation of resources.", "", "16384", "false", "false"
   "kafka.linger.ms", "linger.ms sets the maximum time to buffer data in asynchronous mode. For example, a setting of 100 batches 100ms of messages to send at once. This improves throughput, but the buffering adds message delivery latency.

   

   By default, the producer does not wait. It sends the buffer any time data is available.

   

   Instead of sending immediately, you can set linger.ms to 5 and send more messages in one batch. This would reduce the number of requests sent, but would add up to 5 milliseconds of latency to records sent, even if the load on the system does not warrant the delay.

   

   The farther away the broker is from the producer, the more overhead required to send messages. Increase linger.ms for higher latency and higher throughput in your producer.", "", "5", "false", "false"
   "kafka.acks", "The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the  durability of records that are sent. The following settings are common:  <ul> <li><code>acks=0</code> If set to zero then the producer will not wait for any acknowledgment from the server at all. The record will be immediately added to the socket buffer and considered sent. No guarantee can be made that the server has received the record in this case, and the <code>retries</code> configuration will not take effect (as the client won't generally know of any failures). The offset given back for each record will always be set to -1. <li><code>acks=1</code> This will mean the leader will write the record to its local log but will respond without awaiting full acknowledgement from all followers. In this case should the leader fail immediately after acknowledging the record but before the followers have replicated it then the record will be lost. <li><code>acks=all</code> This means the leader will wait for the full set of in-sync replicas to acknowledge the record. This guarantees that the record will not be lost as long as at least one in-sync replica remains alive. This is the strongest available guarantee.", "", "all", "false", "false"
   "window.duration", "all the elements in seen in a sliding window of time over. windowDuration = width of the window; must be a multiple of batching interval", "", "null", "false", "false"
   "slide.duration", "sliding interval of the window (i.e., the interval after which  the new DStream will generate RDDs); must be a multiple of batching interval", "", "null", "false", "false"
   "max.results.count", "the max number of rows to output. (-1 for no limit)", "", "-1", "false", "false"
   "**sql.query**", "The SQL query to execute, please note that the table name must exists in input topics names", "", "null", "false", "false"
   "output.record.type", "the output type of the record", "", "aggregation", "false", "false"

----------

.. _com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine: 

KafkaStreamProcessingEngine
---------------------------
No description provided.

Class
_____
com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10
   :escape: \

   "**spark.app.name**", "Tha application name", "", "logisland", "false", "false"
   "**spark.master**", "The url to Spark Master", "", "local[2]", "false", "false"
   "spark.monitoring.driver.port", "The port for exposing monitoring metrics", "", "null", "false", "false"
   "spark.yarn.deploy-mode", "The yarn deploy mode", "", "null", "false", "false"
   "spark.yarn.queue", "The name of the YARN queue", "", "default", "false", "false"
   "spark.driver.memory", "The memory size for Spark driver", "", "512m", "false", "false"
   "spark.executor.memory", "The memory size for Spark executors", "", "1g", "false", "false"
   "spark.driver.cores", "The number of cores for Spark driver", "", "4", "false", "false"
   "spark.executor.cores", "The number of cores for Spark driver", "", "1", "false", "false"
   "spark.executor.instances", "The number of instances for Spark app", "", "null", "false", "false"
   "spark.serializer", "Class to use for serializing objects that will be sent over the network or need to be cached in serialized form", "", "org.apache.spark.serializer.KryoSerializer", "false", "false"
   "spark.streaming.blockInterval", "Interval at which data received by Spark Streaming receivers is chunked into blocks of data before storing them in Spark. Minimum recommended - 50 ms", "", "350", "false", "false"
   "spark.streaming.kafka.maxRatePerPartition", "Maximum rate (number of records per second) at which data will be read from each Kafka partition", "", "5000", "false", "false"
   "**spark.streaming.batchDuration**", "No Description Provided.", "", "2000", "false", "false"
   "spark.streaming.backpressure.enabled", "This enables the Spark Streaming to control the receiving rate based on the current batch scheduling delays and processing times so that the system receives only as fast as the system can process.", "", "false", "false", "false"
   "spark.streaming.unpersist", "Force RDDs generated and persisted by Spark Streaming to be automatically unpersisted from Spark's memory. The raw input data received by Spark Streaming is also automatically cleared. Setting this to false will allow the raw data and persisted RDDs to be accessible outside the streaming application as they will not be cleared automatically. But it comes at the cost of higher memory usage in Spark.", "", "false", "false", "false"
   "spark.ui.port", "No Description Provided.", "", "4050", "false", "false"
   "spark.streaming.timeout", "No Description Provided.", "", "-1", "false", "false"
   "spark.streaming.kafka.maxRetries", "Maximum rate (number of records per second) at which data will be read from each Kafka partition", "", "3", "false", "false"
   "spark.streaming.ui.retainedBatches", "How many batches the Spark Streaming UI and status APIs remember before garbage collecting.", "", "200", "false", "false"
   "spark.streaming.receiver.writeAheadLog.enable", "Enable write ahead logs for receivers. All the input data received through receivers will be saved to write ahead logs that will allow it to be recovered after driver failures.", "", "false", "false", "false"
   "spark.yarn.maxAppAttempts", "Because Spark driver and Application Master share a single JVM, any error in Spark driver stops our long-running job. Fortunately it is possible to configure maximum number of attempts that will be made to re-run the application. It is reasonable to set higher value than default 2 (derived from YARN cluster property yarn.resourcemanager.am.max-attempts). 4 works quite well, higher value may cause unnecessary restarts even if the reason of the failure is permanent.", "", "4", "false", "false"
   "spark.yarn.am.attemptFailuresValidityInterval", "If the application runs for days or weeks without restart or redeployment on highly utilized cluster, 4 attempts could be exhausted in few hours. To avoid this situation, the attempt counter should be reset on every hour of so.", "", "1h", "false", "false"
   "spark.yarn.max.executor.failures", "a maximum number of executor failures before the application fails. By default it is max(2 * num executors, 3), well suited for batch jobs but not for long-running jobs. The property comes with corresponding validity interval which also should be set.8 * num_executors", "", "20", "false", "false"
   "spark.yarn.executor.failuresValidityInterval", "If the application runs for days or weeks without restart or redeployment on highly utilized cluster, x attempts could be exhausted in few hours. To avoid this situation, the attempt counter should be reset on every hour of so.", "", "1h", "false", "false"
   "spark.task.maxFailures", "For long-running jobs you could also consider to boost maximum number of task failures before giving up the job. By default tasks will be retried 4 times and then job fails.", "", "8", "false", "false"
   "spark.memory.fraction", "expresses the size of M as a fraction of the (JVM heap space - 300MB) (default 0.75). The rest of the space (25%) is reserved for user data structures, internal metadata in Spark, and safeguarding against OOM errors in the case of sparse and unusually large records.", "", "0.6", "false", "false"
   "spark.memory.storageFraction", "expresses the size of R as a fraction of M (default 0.5). R is the storage space within M where cached blocks immune to being evicted by execution.", "", "0.5", "false", "false"
   "spark.scheduler.mode", "The scheduling mode between jobs submitted to the same SparkContext. Can be set to FAIR to use fair sharing instead of queueing jobs one after another. Useful for multi-user services.", "FAIR (fair sharing), FIFO (queueing jobs one after another)", "FAIR", "false", "false"
   "spark.properties.file.path", "for using --properties-file option while submitting spark job", "", "null", "false", "false"

----------

.. _com.hurence.logisland.stream.spark.structured.provider.KafkaStructuredStreamProviderService: 

KafkaStructuredStreamProviderService
------------------------------------
No description provided.

Class
_____
com.hurence.logisland.stream.spark.structured.provider.KafkaStructuredStreamProviderService

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10
   :escape: \

   "**kafka.error.topics**", "Sets the error topics Kafka topic name", "", "_errors", "false", "false"
   "**kafka.input.topics**", "Sets the input Kafka topic name", "", "_raw", "false", "false"
   "**kafka.output.topics**", "Sets the output Kafka topic name", "", "_records", "false", "false"
   "avro.input.schema", "the avro schema definition", "", "null", "false", "false"
   "avro.output.schema", "the avro schema definition for the output serialization", "", "null", "false", "false"
   "kafka.input.topics.serializer", "No Description Provided.", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes)", "com.hurence.logisland.serializer.KryoSerializer", "false", "false"
   "kafka.output.topics.serializer", "No Description Provided.", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes)", "com.hurence.logisland.serializer.KryoSerializer", "false", "false"
   "kafka.error.topics.serializer", "No Description Provided.", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes)", "com.hurence.logisland.serializer.JsonSerializer", "false", "false"
   "kafka.topic.autoCreate", "define wether a topic should be created automatically if not already exists", "", "true", "false", "false"
   "kafka.topic.default.partitions", "if autoCreate is set to true, this will set the number of partition at topic creation time", "", "20", "false", "false"
   "kafka.topic.default.replicationFactor", "if autoCreate is set to true, this will set the number of replica for each partition at topic creation time", "", "3", "false", "false"
   "**kafka.metadata.broker.list**", "a comma separated list of host:port brokers", "", "sandbox:9092", "false", "false"
   "**kafka.zookeeper.quorum**", "No Description Provided.", "", "sandbox:2181", "false", "false"
   "kafka.manual.offset.reset", "What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted):

   earliest: automatically reset the offset to the earliest offset

   latest: automatically reset the offset to the latest offset

   none: throw exception to the consumer if no previous offset is found for the consumer's group

   anything else: throw exception to the consumer.", "latest (the offset to the latest offset), earliest (the offset to the earliest offset), none (the latest saved  offset)", "earliest", "false", "false"
   "kafka.batch.size", "measures batch size in total bytes instead of the number of messages. It controls how many bytes of data to collect before sending messages to the Kafka broker. Set this as high as possible, without exceeding available memory. The default value is 16384.

   

   If you increase the size of your buffer, it might never get full.The Producer sends the information eventually, based on other triggers, such as linger time in milliseconds. Although you can impair memory usage by setting the buffer batch size too high, this does not impact latency.

   

   If your producer is sending all the time, you are probably getting the best throughput possible. If the producer is often idle, you might not be writing enough data to warrant the current allocation of resources.", "", "16384", "false", "false"
   "kafka.linger.ms", "linger.ms sets the maximum time to buffer data in asynchronous mode. For example, a setting of 100 batches 100ms of messages to send at once. This improves throughput, but the buffering adds message delivery latency.

   

   By default, the producer does not wait. It sends the buffer any time data is available.

   

   Instead of sending immediately, you can set linger.ms to 5 and send more messages in one batch. This would reduce the number of requests sent, but would add up to 5 milliseconds of latency to records sent, even if the load on the system does not warrant the delay.

   

   The farther away the broker is from the producer, the more overhead required to send messages. Increase linger.ms for higher latency and higher throughput in your producer.", "", "5", "false", "false"
   "kafka.acks", "The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the  durability of records that are sent. The following settings are common:  <ul> <li><code>acks=0</code> If set to zero then the producer will not wait for any acknowledgment from the server at all. The record will be immediately added to the socket buffer and considered sent. No guarantee can be made that the server has received the record in this case, and the <code>retries</code> configuration will not take effect (as the client won't generally know of any failures). The offset given back for each record will always be set to -1. <li><code>acks=1</code> This will mean the leader will write the record to its local log but will respond without awaiting full acknowledgement from all followers. In this case should the leader fail immediately after acknowledging the record but before the followers have replicated it then the record will be lost. <li><code>acks=all</code> This means the leader will wait for the full set of in-sync replicas to acknowledge the record. This guarantees that the record will not be lost as long as at least one in-sync replica remains alive. This is the strongest available guarantee.", "", "all", "false", "false"
   "window.duration", "all the elements in seen in a sliding window of time over. windowDuration = width of the window; must be a multiple of batching interval", "", "null", "false", "false"
   "slide.duration", "sliding interval of the window (i.e., the interval after which  the new DStream will generate RDDs); must be a multiple of batching interval", "", "null", "false", "false"

----------

.. _com.hurence.logisland.stream.spark.structured.provider.MQTTStructuredStreamProviderService: 

MQTTStructuredStreamProviderService
-----------------------------------
No description provided.

Class
_____
com.hurence.logisland.stream.spark.structured.provider.MQTTStructuredStreamProviderService

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10
   :escape: \

   "mqtt.broker.url", "brokerUrl A url MqttClient connects to. Set this or path as the url of the Mqtt Server. e.g. tcp://localhost:1883", "", "tcp://localhost:1883", "false", "false"
   "mqtt.clean.session", "cleanSession Setting it true starts a clean session, removes all checkpointed messages by a previous run of this source. This is set to false by default.", "", "true", "false", "false"
   "**mqtt.client.id**", "clientID this client is associated. Provide the same value to recover a stopped client.", "", "null", "false", "false"
   "mqtt.connection.timeout", "connectionTimeout Sets the connection timeout, a value of 0 is interpreted as wait until client connects. See MqttConnectOptions.setConnectionTimeout for more information", "", "5000", "false", "false"
   "mqtt.keep.alive", "keepAlive Same as MqttConnectOptions.setKeepAliveInterval.", "", "5000", "false", "false"
   "mqtt.password", "password Sets the password to use for the connection", "", "null", "false", "false"
   "mqtt.persistence", "persistence By default it is used for storing incoming messages on disk. If memory is provided as value for this option, then recovery on restart is not supported.", "", "memory", "false", "false"
   "mqtt.version", "mqttVersion Same as MqttConnectOptions.setMqttVersion", "", "5000", "false", "false"
   "mqtt.username", " username Sets the user name to use for the connection to Mqtt Server. Do not set it, if server does not need this. Setting it empty will lead to errors.", "", "null", "false", "false"
   "mqtt.qos", " QoS The maximum quality of service to subscribe each topic at.Messages published at a lower quality of service will be received at the published QoS.Messages published at a higher quality of service will be received using the QoS specified on the subscribe", "", "0", "false", "false"
   "**mqtt.topic**", "Topic MqttClient subscribes to.", "", "null", "false", "false"

----------

.. _com.hurence.logisland.engine.spark.RemoteApiStreamProcessingEngine: 

RemoteApiStreamProcessingEngine
-------------------------------
No description provided.

Class
_____
com.hurence.logisland.engine.spark.RemoteApiStreamProcessingEngine

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10
   :escape: \

   "**spark.app.name**", "Tha application name", "", "logisland", "false", "false"
   "**spark.master**", "The url to Spark Master", "", "local[2]", "false", "false"
   "spark.monitoring.driver.port", "The port for exposing monitoring metrics", "", "null", "false", "false"
   "spark.yarn.deploy-mode", "The yarn deploy mode", "", "null", "false", "false"
   "spark.yarn.queue", "The name of the YARN queue", "", "default", "false", "false"
   "spark.driver.memory", "The memory size for Spark driver", "", "512m", "false", "false"
   "spark.executor.memory", "The memory size for Spark executors", "", "1g", "false", "false"
   "spark.driver.cores", "The number of cores for Spark driver", "", "4", "false", "false"
   "spark.executor.cores", "The number of cores for Spark driver", "", "1", "false", "false"
   "spark.executor.instances", "The number of instances for Spark app", "", "null", "false", "false"
   "spark.serializer", "Class to use for serializing objects that will be sent over the network or need to be cached in serialized form", "", "org.apache.spark.serializer.KryoSerializer", "false", "false"
   "spark.streaming.blockInterval", "Interval at which data received by Spark Streaming receivers is chunked into blocks of data before storing them in Spark. Minimum recommended - 50 ms", "", "350", "false", "false"
   "spark.streaming.kafka.maxRatePerPartition", "Maximum rate (number of records per second) at which data will be read from each Kafka partition", "", "5000", "false", "false"
   "**spark.streaming.batchDuration**", "No Description Provided.", "", "2000", "false", "false"
   "spark.streaming.backpressure.enabled", "This enables the Spark Streaming to control the receiving rate based on the current batch scheduling delays and processing times so that the system receives only as fast as the system can process.", "", "false", "false", "false"
   "spark.streaming.unpersist", "Force RDDs generated and persisted by Spark Streaming to be automatically unpersisted from Spark's memory. The raw input data received by Spark Streaming is also automatically cleared. Setting this to false will allow the raw data and persisted RDDs to be accessible outside the streaming application as they will not be cleared automatically. But it comes at the cost of higher memory usage in Spark.", "", "false", "false", "false"
   "spark.ui.port", "No Description Provided.", "", "4050", "false", "false"
   "spark.streaming.timeout", "No Description Provided.", "", "-1", "false", "false"
   "spark.streaming.kafka.maxRetries", "Maximum rate (number of records per second) at which data will be read from each Kafka partition", "", "3", "false", "false"
   "spark.streaming.ui.retainedBatches", "How many batches the Spark Streaming UI and status APIs remember before garbage collecting.", "", "200", "false", "false"
   "spark.streaming.receiver.writeAheadLog.enable", "Enable write ahead logs for receivers. All the input data received through receivers will be saved to write ahead logs that will allow it to be recovered after driver failures.", "", "false", "false", "false"
   "spark.yarn.maxAppAttempts", "Because Spark driver and Application Master share a single JVM, any error in Spark driver stops our long-running job. Fortunately it is possible to configure maximum number of attempts that will be made to re-run the application. It is reasonable to set higher value than default 2 (derived from YARN cluster property yarn.resourcemanager.am.max-attempts). 4 works quite well, higher value may cause unnecessary restarts even if the reason of the failure is permanent.", "", "4", "false", "false"
   "spark.yarn.am.attemptFailuresValidityInterval", "If the application runs for days or weeks without restart or redeployment on highly utilized cluster, 4 attempts could be exhausted in few hours. To avoid this situation, the attempt counter should be reset on every hour of so.", "", "1h", "false", "false"
   "spark.yarn.max.executor.failures", "a maximum number of executor failures before the application fails. By default it is max(2 * num executors, 3), well suited for batch jobs but not for long-running jobs. The property comes with corresponding validity interval which also should be set.8 * num_executors", "", "20", "false", "false"
   "spark.yarn.executor.failuresValidityInterval", "If the application runs for days or weeks without restart or redeployment on highly utilized cluster, x attempts could be exhausted in few hours. To avoid this situation, the attempt counter should be reset on every hour of so.", "", "1h", "false", "false"
   "spark.task.maxFailures", "For long-running jobs you could also consider to boost maximum number of task failures before giving up the job. By default tasks will be retried 4 times and then job fails.", "", "8", "false", "false"
   "spark.memory.fraction", "expresses the size of M as a fraction of the (JVM heap space - 300MB) (default 0.75). The rest of the space (25%) is reserved for user data structures, internal metadata in Spark, and safeguarding against OOM errors in the case of sparse and unusually large records.", "", "0.6", "false", "false"
   "spark.memory.storageFraction", "expresses the size of R as a fraction of M (default 0.5). R is the storage space within M where cached blocks immune to being evicted by execution.", "", "0.5", "false", "false"
   "spark.scheduler.mode", "The scheduling mode between jobs submitted to the same SparkContext. Can be set to FAIR to use fair sharing instead of queueing jobs one after another. Useful for multi-user services.", "FAIR (fair sharing), FIFO (queueing jobs one after another)", "FAIR", "false", "false"
   "spark.properties.file.path", "for using --properties-file option while submitting spark job", "", "null", "false", "false"
   "**remote.api.baseUrl**", "The base URL of the remote server providing logisland configuration", "", "null", "false", "false"
   "**remote.api.polling.rate**", "Remote api polling rate in milliseconds", "", "null", "false", "false"
   "**remote.api.push.rate**", "Remote api configuration push rate in milliseconds", "", "null", "false", "false"
   "remote.api.timeouts.connect", "Remote api connection timeout in milliseconds", "", "10000", "false", "false"
   "remote.api.auth.user", "The basic authentication user for the remote api endpoint.", "", "null", "false", "false"
   "remote.api.auth.password", "The basic authentication password for the remote api endpoint.", "", "null", "false", "false"
   "remote.api.timeouts.socket", "Remote api default read/write socket timeout in milliseconds", "", "10000", "false", "false"

----------

.. _com.hurence.logisland.stream.spark.structured.StructuredStream: 

StructuredStream
----------------
No description provided.

Class
_____
com.hurence.logisland.stream.spark.structured.StructuredStream

Tags
____
None.

Properties
__________
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values.

.. csv-table:: allowable-values
   :header: "Name","Description","Allowable Values","Default Value","Sensitive","EL"
   :widths: 20,60,30,20,10,10
   :escape: \

   "**read.topics**", "the input path for any topic to be read from", "", "null", "false", "false"
   "**read.topics.client.service**", "the controller service that gives connection information", "", "null", "false", "false"
   "**read.topics.serializer**", "the serializer to use", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes), com.hurence.logisland.serializer.KuraProtobufSerializer (serialize events as Kura protocol buffer)", "none", "false", "false"
   "**read.topics.key.serializer**", "The key serializer to use", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.KuraProtobufSerializer (serialize events as Kura protocol buffer), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes)", "none", "false", "false"
   "**write.topics**", "the input path for any topic to be written to", "", "null", "false", "false"
   "**write.topics.client.service**", "the controller service that gives connection information", "", "null", "false", "false"
   "**write.topics.serializer**", "the serializer to use", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes), com.hurence.logisland.serializer.KuraProtobufSerializer (serialize events as Kura protocol buffer)", "none", "false", "false"
   "**write.topics.key.serializer**", "The key serializer to use", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes), com.hurence.logisland.serializer.KuraProtobufSerializer (serialize events as Kura protocol buffer)", "none", "false", "false"
