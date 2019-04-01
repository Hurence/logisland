Engine-vanilla
==========


----------

.. _com.hurence.logisland.engine.vanilla.stream.amqp.AmqpClientPipelineStream: 

AmqpClientPipelineStream
------------------------
No description provided.

Class
_____
com.hurence.logisland.engine.vanilla.stream.amqp.AmqpClientPipelineStream

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

   "**connection.host**", "Connection host name", "", "null", "false", "false"
   "connection.port", "Connection port", "", "5672", "false", "false"
   "link.credits", "Flow control. How many credits for this links. Higher means higher prefetch (prebuffered number of messages", "", "1024", "false", "false"
   "connection.auth.user", "Connection authenticated user name", "", "null", "false", "false"
   "connection.auth.password", "Connection authenticated password", "", "null", "false", "false"
   "connection.auth.tls.cert", "Connection TLS public certificate (PEM file path)", "", "null", "false", "false"
   "connection.auth.tls.key", "Connection TLS private key (PEM file path)", "", "null", "false", "false"
   "connection.auth.ca.cert", "Connection TLS CA cert (PEM file path)", "", "null", "false", "false"
   "**read.topic**", "The input path for any topic to be read from", "", "", "false", "false"
   "**read.topic.serializer**", "The serializer to use", "com.hurence.logisland.serializer.BsonSerializer (serialize events as bson), com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes), com.hurence.logisland.serializer.KuraProtobufSerializer (serialize events as Kura protocol buffer)", "none", "false", "false"
   "avro.input.schema", "The avro schema definition", "", "null", "false", "false"
   "**write.topic**", "The input path for any topic to be written to", "", "", "false", "false"
   "**write.topic.serializer**", "The serializer to use", "com.hurence.logisland.serializer.BsonSerializer (serialize events as bson), com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes), com.hurence.logisland.serializer.KuraProtobufSerializer (serialize events as Kura protocol buffer)", "none", "false", "false"
   "avro.output.schema", "The avro schema definition for the output serialization", "", "null", "false", "false"
   "container.id", "AMQP container ID", "", "null", "false", "false"
   "write.topic.content.type", "The content type to set in the output message", "", "null", "false", "false"
   "connection.reconnect.backoff", "Reconnection delay linear backoff", "", "2.0", "false", "false"
   "connection.reconnect.initial.delay", "Initial reconnection delay in milliseconds", "", "1000", "false", "false"
   "connection.reconnect.max.delay", "Maximum reconnection delay in milliseconds", "", "30000", "false", "false"

----------

.. _com.hurence.logisland.engine.vanilla.stream.kafka.KafkaStreamsPipelineStream: 

KafkaStreamsPipelineStream
--------------------------
No description provided.

Class
_____
com.hurence.logisland.engine.vanilla.stream.kafka.KafkaStreamsPipelineStream

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

   "**bootstrap.servers**", "List of kafka nodes to connect to", "", "null", "false", "false"
   "**read.topics**", "The input path for any topic to be read from", "", "", "false", "false"
   "avro.input.schema", "The avro schema definition", "", "null", "false", "false"
   "avro.output.schema", "The avro schema definition for the output serialization", "", "null", "false", "false"
   "kafka.manual.offset.reset", "What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted):

   earliest: automatically reset the offset to the earliest offset

   latest: automatically reset the offset to the latest offset

   none: throw exception to the consumer if no previous offset is found for the consumer's group

   anything else: throw exception to the consumer.", "latest (the offset to the latest offset), earliest (the offset to the earliest offset), none (the latest saved  offset)", "earliest", "false", "false"
   "**read.topics.serializer**", "The serializer to use", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes), com.hurence.logisland.serializer.KuraProtobufSerializer (serialize events as Kura protocol buffer)", "none", "false", "false"
   "**write.topics**", "The input path for any topic to be written to", "", "", "false", "false"
   "**write.topics.serializer**", "The serializer to use", "com.hurence.logisland.serializer.KryoSerializer (serialize events as binary blocs), com.hurence.logisland.serializer.JsonSerializer (serialize events as json blocs), com.hurence.logisland.serializer.ExtendedJsonSerializer (serialize events as json blocs supporting nested objects/arrays), com.hurence.logisland.serializer.AvroSerializer (serialize events as avro blocs), com.hurence.logisland.serializer.BytesArraySerializer (serialize events as byte arrays), com.hurence.logisland.serializer.StringSerializer (serialize events as string), none (send events as bytes), com.hurence.logisland.serializer.KuraProtobufSerializer (serialize events as Kura protocol buffer)", "none", "false", "false"

----------

.. _com.hurence.logisland.engine.vanilla.PlainJavaEngine: 

PlainJavaEngine
---------------
No description provided.

Class
_____
com.hurence.logisland.engine.vanilla.PlainJavaEngine

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

   "jvm.heap.min", "Minimum memory the JVM should allocate for its heap", "", "null", "false", "false"
   "jvm.heap.max", "Maximum memory the JVM should allocate for its heap", "", "null", "false", "false"
