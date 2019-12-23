/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
  * Copyright (C) 2016 Hurence (support@hurence.com)
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.hurence.logisland.stream

import com.hurence.logisland.component.{AllowableValue, PropertyDescriptor}
import com.hurence.logisland.serializer._
import com.hurence.logisland.stream.spark.structured.provider.StructuredStreamProviderService
import com.hurence.logisland.validator.StandardValidators

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
object StreamProperties {

  val NONE_TOPIC = "none"

  val DEFAULT_RAW_TOPIC = new AllowableValue("_raw", "default raw topic", "the incoming non structured topic")
  val DEFAULT_RECORDS_TOPIC = new AllowableValue("_records", "default events topic", "the outgoing structured topic")
  val DEFAULT_ERRORS_TOPIC = new AllowableValue("_errors", "default raw topic", "the outgoing structured error topic")
  val DEFAULT_METRICS_TOPIC = new AllowableValue("_metrics", "default metrics topic", "the topic to place processing metrics")

  val INPUT_TOPICS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.input.topics")
    .description("Sets the input Kafka topic name")
    .required(true)
    .defaultValue(DEFAULT_RAW_TOPIC.getValue)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val OUTPUT_TOPICS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.output.topics")
    .description("Sets the output Kafka topic name")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .defaultValue(DEFAULT_RECORDS_TOPIC.getValue)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val ERROR_TOPICS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.error.topics")
    .description("Sets the error topics Kafka topic name")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .defaultValue(DEFAULT_ERRORS_TOPIC.getValue)
    .build

  val INPUT_TOPICS_PARTITIONS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.input.topics.partitions")
    .description("if autoCreate is set to true, this will set the number of partition at topic creation time")
    .required(false)
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .defaultValue("20")
    .build

  val OUTPUT_TOPICS_PARTITIONS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.output.topics.partitions")
    .description("if autoCreate is set to true, this will set the number of partition at topic creation time")
    .required(false)
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .defaultValue("20")
    .build

  val AVRO_INPUT_SCHEMA: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("avro.input.schema")
    .description("the avro schema definition")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val AVRO_OUTPUT_SCHEMA: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("avro.output.schema")
    .description("the avro schema definition for the output serialization")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val AVRO_SERIALIZER = new AllowableValue(classOf[AvroSerializer].getName,
    "avro serialization", "serialize events as avro blocs")
  val JSON_SERIALIZER = new AllowableValue(classOf[JsonSerializer].getName,
    "json serialization", "serialize events as json blocs")
  val EXTENDED_JSON_SERIALIZER = new AllowableValue(classOf[ExtendedJsonSerializer].getName,
    "extended json serialization", "serialize events as json blocs supporting nested objects/arrays")
  val KRYO_SERIALIZER = new AllowableValue(classOf[KryoSerializer].getName,
    "kryo serialization", "serialize events as binary blocs")
  val STRING_SERIALIZER = new AllowableValue(classOf[StringSerializer].getName,
    "string serialization", "serialize events as string")
  val BYTESARRAY_SERIALIZER = new AllowableValue(classOf[BytesArraySerializer].getName,
    "byte array serialization", "serialize events as byte arrays")
  val KURA_PROTOCOL_BUFFER_SERIALIZER = new AllowableValue(classOf[KuraProtobufSerializer].getName,
    "Kura Protobuf serialization", "serialize events as Kura protocol buffer")
  val NO_SERIALIZER = new AllowableValue("none", "no serialization", "send events as bytes")

  val INPUT_SERIALIZER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.input.topics.serializer")
    .description("")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER)
    .defaultValue(KRYO_SERIALIZER.getValue)
    .build

  val OUTPUT_SERIALIZER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.output.topics.serializer")
    .description("")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER)
    .defaultValue(KRYO_SERIALIZER.getValue)
    .build

  val ERROR_SERIALIZER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.error.topics.serializer")
    .description("")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .defaultValue(JSON_SERIALIZER.getValue)
    .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER)
    .build


  val KAFKA_TOPIC_AUTOCREATE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.topic.autoCreate")
    .description("define wether a topic should be created automatically if not already exists")
    .required(false)
    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
    .defaultValue("true")
    .build

  val KAFKA_TOPIC_DEFAULT_PARTITIONS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.topic.default.partitions")
    .description("if autoCreate is set to true, this will set the number of partition at topic creation time")
    .required(false)
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .defaultValue("20")
    .build

  val KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.topic.default.replicationFactor")
    .description("if autoCreate is set to true, this will set the number of replica for each partition at topic creation time")
    .required(false)
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .defaultValue("3")
    .build

  val KAFKA_METADATA_BROKER_LIST: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.metadata.broker.list")
    .description("a comma separated list of host:port brokers")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .defaultValue("sandbox:9092")
    .build

  val KAFKA_ZOOKEEPER_QUORUM: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.zookeeper.quorum")
    .description("")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .defaultValue("sandbox:2181")
    .build

  val LATEST_OFFSET = new AllowableValue("latest", "latest", "the offset to the latest offset")
  val EARLIEST_OFFSET = new AllowableValue("earliest", "earliest offset", "the offset to the earliest offset")
  val NONE_OFFSET = new AllowableValue("none", "none offset", "the latest saved  offset")

  val KAFKA_MANUAL_OFFSET_RESET: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.manual.offset.reset")
    .description("What to do when there is no initial offset in Kafka or if the current offset does not exist " +
      "any more on the server (e.g. because that data has been deleted):\n" +
      "earliest: automatically reset the offset to the earliest offset\n" +
      "latest: automatically reset the offset to the latest offset\n" +
      "none: throw exception to the consumer if no previous offset is found for the consumer's group\n" +
      "anything else: throw exception to the consumer.")
    .required(false)
    .allowableValues(LATEST_OFFSET, EARLIEST_OFFSET, NONE_OFFSET)
    .defaultValue(EARLIEST_OFFSET.getValue)
    .build


  val KAFKA_BATCH_SIZE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.batch.size")
    .description("measures batch size in total bytes instead of the number of messages. " +
      "It controls how many bytes of data to collect before sending messages to the Kafka broker. " +
      "Set this as high as possible, without exceeding available memory. The default value is 16384.\n\n" +
      "If you increase the size of your buffer, it might never get full." +
      "The Producer sends the information eventually, based on other triggers, such as linger time in milliseconds. " +
      "Although you can impair memory usage by setting the buffer batch size too high, " +
      "this does not impact latency.\n\n" +
      "If your producer is sending all the time, " +
      "you are probably getting the best throughput possible. If the producer is often idle, " +
      "you might not be writing enough data to warrant the current allocation of resources.")
    .required(false)
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .defaultValue("16384")
    .build


  val KAFKA_LINGER_MS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.linger.ms")
    .description("linger.ms sets the maximum time to buffer data in asynchronous mode. " +
      "For example, a setting of 100 batches 100ms of messages to send at once. " +
      "This improves throughput, but the buffering adds message delivery latency.\n\n" +
      "By default, the producer does not wait. It sends the buffer any time data is available.\n\n" +
      "Instead of sending immediately, you can set linger.ms to 5 and send more messages in one batch." +
      " This would reduce the number of requests sent, but would add up to 5 milliseconds of latency to records " +
      "sent, even if the load on the system does not warrant the delay.\n\n" +
      "The farther away the broker is from the producer, the more overhead required to send messages. " +
      "Increase linger.ms for higher latency and higher throughput in your producer.")
    .required(false)
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .defaultValue("5")
    .build

  val KAFKA_ACKS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.acks")
    .description("The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the "
      + " durability of records that are sent. The following settings are common: "
      + " <ul>"
      + " <li><code>acks=0</code> If set to zero then the producer will not wait for any acknowledgment from the"
      + " server at all. The record will be immediately added to the socket buffer and considered sent. No guarantee can be"
      + " made that the server has received the record in this case, and the <code>retries</code> configuration will not"
      + " take effect (as the client won't generally know of any failures). The offset given back for each record will"
      + " always be set to -1."
      + " <li><code>acks=1</code> This will mean the leader will write the record to its local log but will respond"
      + " without awaiting full acknowledgement from all followers. In this case should the leader fail immediately after"
      + " acknowledging the record but before the followers have replicated it then the record will be lost."
      + " <li><code>acks=all</code> This means the leader will wait for the full set of in-sync replicas to"
      + " acknowledge the record. This guarantees that the record will not be lost as long as at least one in-sync replica"
      + " remains alive. This is the strongest available guarantee.")
    .required(false)
    .defaultValue("all")
    .build


  val WINDOW_DURATION: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("window.duration")
    .description("all the elements in seen in a sliding window of time over. windowDuration = width of the window; must be a multiple of batching interval")
    .addValidator(StandardValidators.LONG_VALIDATOR)
    .required(false)
    .build

  val SLIDE_DURATION: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("slide.duration")
    .description("sliding interval of the window (i.e., the interval after which  the new DStream will generate RDDs); must be a multiple of batching interval")
    .addValidator(StandardValidators.LONG_VALIDATOR)
    .required(false)
    .build

  val GROUPBY: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("groupby")
    .description("comma separated list of fields to group the partition by")
    .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
    .required(false)
    .build

  val STATE_TIMEOUT_MS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("state.timeout.ms")
    .description("the time in ms before we invalidate the microbatch state")
    .addValidator(StandardValidators.LONG_VALIDATOR)
    .required(false)
    .defaultValue("2000")
    .build

  val CHUNK_SIZE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("chunk.size")
    .description("the number of records to group into chunks")
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .required(false)
    .defaultValue("100")
    .build
  //////////////////////////////////////
  // MQTT options
  //////////////////////////////////////

  val MQTT_BROKER_URL: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.broker.url")
    .description("brokerUrl A url MqttClient connects to. Set this or path as the url of the Mqtt Server. e.g. tcp://localhost:1883")
    .addValidator(StandardValidators.URL_VALIDATOR)
    .defaultValue("tcp://localhost:1883")
    .required(false)
    .build

  val MQTT_PERSISTENCE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.persistence")
    .description("persistence By default it is used for storing incoming messages on disk. " +
      "If memory is provided as value for this option, then recovery on restart is not supported.")
    .defaultValue("memory")
    .required(false)
    .build

  val MQTT_TOPIC: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.topic")
    .description("Topic MqttClient subscribes to.")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(true)
    .build

  val MQTT_CLIENTID: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.client.id")
    .description("clientID this client is associated. Provide the same value to recover a stopped client.")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(true)
    .build

  val MQTT_QOS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.qos")
    .description(" QoS The maximum quality of service to subscribe each topic at.Messages published at a lower " +
      "quality of service will be received at the published QoS.Messages published at a higher quality of " +
      "service will be received using the QoS specified on the subscribe")
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .defaultValue("0")
    .required(false)
    .build

  val MQTT_USERNAME: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.username")
    .description(" username Sets the user name to use for the connection to Mqtt Server. " +
      "Do not set it, if server does not need this. Setting it empty will lead to errors.")
    .required(false)
    .build

  val MQTT_PASSWORD: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.password")
    .description("password Sets the password to use for the connection")
    .required(false)
    .build

  val MQTT_CLEAN_SESSION: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.clean.session")
    .description("cleanSession Setting it true starts a clean session, removes all checkpointed messages by " +
      "a previous run of this source. This is set to false by default.")
    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
    .defaultValue("true")
    .required(false)
    .build

  val MQTT_CONNECTION_TIMEOUT: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.connection.timeout")
    .description("connectionTimeout Sets the connection timeout, a value of 0 is interpreted as " +
      "wait until client connects. See MqttConnectOptions.setConnectionTimeout for more information")
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .defaultValue("5000")
    .required(false)
    .build

  val MQTT_KEEP_ALIVE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.keep.alive")
    .description("keepAlive Same as MqttConnectOptions.setKeepAliveInterval.")
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .defaultValue("5000")
    .required(false)
    .build


  val MQTT_VERSION: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.version")
    .description("mqttVersion Same as MqttConnectOptions.setMqttVersion")
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .defaultValue("5000")
    .required(false)
    .build

  val READ_TOPICS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("read.topics")
    .description("the input path for any topic to be read from")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(true)
    .build

  val READ_TOPICS_SERIALIZER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("read.topics.serializer")
    .description("the serializer to use")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER, KURA_PROTOCOL_BUFFER_SERIALIZER)
    .defaultValue(NO_SERIALIZER.getValue)
    .build

  val READ_TOPICS_KEY_SERIALIZER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("read.topics.key.serializer")
    .description("The key serializer to use")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, KURA_PROTOCOL_BUFFER_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER)
    .defaultValue(NO_SERIALIZER.getValue)
    .build

  val READ_STREAM_SERVICE_PROVIDER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("read.stream.service.provider")
    .description("the controller service that gives connection information")
    .required(true)
    .identifiesControllerService(classOf[StructuredStreamProviderService])
    .build


  val WRITE_TOPICS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("write.topics")
    .description("the input path for any topic to be written to")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(true)
    .build

  val WRITE_TOPICS_SERIALIZER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("write.topics.serializer")
    .description("the serializer to use")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER, KURA_PROTOCOL_BUFFER_SERIALIZER)
    .defaultValue(NO_SERIALIZER.getValue)
    .build

  val WRITE_TOPICS_KEY_SERIALIZER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("write.topics.key.serializer")
    .description("The key serializer to use")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER, KURA_PROTOCOL_BUFFER_SERIALIZER)
    .defaultValue(NO_SERIALIZER.getValue)
    .build

  val WRITE_STREAM_SERVICE_PROVIDER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("write.stream.service.provider")
    .description("the controller service that gives connection information")
    .required(true)
    .identifiesControllerService(classOf[StructuredStreamProviderService])
    .build


  //////////////////////////////////////
  // HDFS options
  //////////////////////////////////////
  val FILE_FORMAT_PARQUET = "parquet"
  val FILE_FORMAT_ORC = "orc"
  val FILE_FORMAT_JSON = "json"
  val FILE_FORMAT_TXT = "txt"

  val OUTPUT_FOLDER_PATH = new PropertyDescriptor.Builder()
    .name("output.folder.path")
    .description("the location where to put files : file:///tmp/out")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build


  val INPUT_FORMAT = new PropertyDescriptor.Builder()
    .name("input.format")
    .description("Used to load data from a raw record_value. Only json supported")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .defaultValue("")
    .build

  val OUTPUT_FORMAT = new PropertyDescriptor.Builder()
    .name("output.format")
    .description("can be parquet, orc csv")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .allowableValues(FILE_FORMAT_PARQUET, FILE_FORMAT_TXT, FILE_FORMAT_JSON, FILE_FORMAT_JSON)
    .build

  val RECORD_TYPE = new PropertyDescriptor.Builder()
    .name("record.type")
    .description("the type of event to filter")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val NUM_PARTITIONS = new PropertyDescriptor.Builder()
    .name("num.partitions")
    .description("the numbers of physical files on HDFS")
    .required(false)
    .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
    .defaultValue("4")
    .build

  val EXCLUDE_ERRORS = new PropertyDescriptor.Builder()
    .name("exclude.errors")
    .description("do we include records with errors ?")
    .required(false)
    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
    .defaultValue("true")
    .build

  val DATE_FORMAT = new PropertyDescriptor.Builder()
    .name("date.format")
    .description("The format of the date for the partition")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .defaultValue("yyyy-MM-dd")
    .build


  //////////////////////////////////////
  // SQL options
  //////////////////////////////////////
  val SQL_QUERY = new PropertyDescriptor.Builder()
    .name("sql.query")
    .description("The SQL query to execute, " +
      "please note that the table name must exists in input topics names")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val MAX_RESULTS_COUNT = new PropertyDescriptor.Builder()
    .name("max.results.count")
    .description("the max number of rows to output. (-1 for no limit)")
    .required(false)
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .defaultValue("-1")
    .build

  val OUTPUT_RECORD_TYPE = new PropertyDescriptor.Builder()
    .name("output.record.type")
    .description("the output type of the record")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .defaultValue("aggregation")
    .build


  //////////////////////////////////////
  // Security options
  //////////////////////////////////////

  val PLAINTEXT = new AllowableValue("PLAINTEXT", "PLAINTEXT", "Un-authenticated, non-encrypted channel")
  val SSL = new AllowableValue("SSL", "SSL", "SSL channel")
  val SASL_PLAINTEXT = new AllowableValue("SASL_PLAINTEXT", "SASL_PLAINTEXT", "SASL authenticated, non-encrypted channel")
  val SASL_SSL = new AllowableValue("SASL_SSL", "SASL_SSL", "SASL authenticated, SSL channel")

  val KAFKA_SECURITY_PROTOCOL: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.security.protocol")
    .description("kafka.security.protocol sets the value of of the security protocol \n" +
      "Apache Kafka® brokers supports client authentication via SASL. " +
      "SASL authentication can be enabled concurrently with SSL encryption " +
      "(SSL client authentication will be disabled).\n\nThe supported SASL mechanisms are:")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .defaultValue(PLAINTEXT.getValue)
    .allowableValues(PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
    .build

  val KAFKA_SASL_KERBEROS_SERVICE_NAME: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.sasl.kerberos.service.name")
    .description("follow the guide here to configure your job to work with kerberos authentification \n" +
      "https://docs.confluent.io/2.0.0/kafka/sasl.html")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .defaultValue("kafka")
    .build

}
