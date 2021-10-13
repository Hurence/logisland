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
package com.hurence.logisland.stream.spark.structured.provider

import com.hurence.logisland.component.{AllowableValue, PropertyDescriptor}
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.validator.StandardValidators

object KafkaProperties {
  val NONE_TOPIC = "none"

  val DEFAULT_RAW_TOPIC = new AllowableValue("logisland_raw", "default raw topic", "the incoming non structured topic")
  val DEFAULT_RECORDS_TOPIC = new AllowableValue("logisland_records", "default events topic", "the outgoing structured topic")
  val DEFAULT_ERRORS_TOPIC = new AllowableValue("logisland_errors", "default raw topic", "the outgoing structured error topic")
  val DEFAULT_METRICS_TOPIC = new AllowableValue("logisland_metrics", "default metrics topic", "the topic to place processing metrics")

  val INPUT_TOPICS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.input.topics")
    .description("A comma-separated list of topics. The topic list to subscribe.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val INPUT_TOPIC_PATTERN: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.input.topics.pattern")
    .description("Java regex string. The pattern used to subscribe to topic(s).")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val OUTPUT_TOPICS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.output.topics")
    .description("Sets the output Kafka topic name")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val ERROR_TOPICS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.error.topics")
    .description("Sets the error topics Kafka topic name")
    .required(false)
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
    .defaultValue("3")
    .build

  val KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.topic.default.replicationFactor")
    .description("if autoCreate is set to true, this will set the number of replica for each partition at topic creation time")
    .required(false)
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .defaultValue("1")
    .build

  val KAFKA_METADATA_BROKER_LIST: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.metadata.broker.list")
    .description("a comma separated list of host:port brokers")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .defaultValue("localhost:9092")
    .build

  val KAFKA_ZOOKEEPER_QUORUM: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.zookeeper.quorum")
    .description("")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .defaultValue("localhost:2181")
    .build

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

  val KAFKA_STARTING_OFFSETS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.startingOffsets")
    .description("\"earliest\", \"latest\", or json string \"\"\" {\"topicA\":{\"0\":23,\"1\":-1},\"topicB\":{\"0\":-2}} \"\"\"" +
      "The start point when a query is started, either \"earliest\" which is from the earliest offsets, " +
      "\"latest\" which is just from the latest offsets, or a json string specifying a starting offset for each TopicPartition. " +
      "In the json, -2 as an offset can be used to refer to earliest, -1 to latest. " +
      "Note: this only applies when a new query is started, and that resuming will always pick up from where the query " +
      "left off. Newly discovered partitions during a query will start at earliest.")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(false)
    .defaultValue("earliest")
    .build

  val KAFKA_FAIL_ON_DATA_LOSS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.failOnDataLoss")
    .description("Whether to fail the query when it's possible that data is lost" +
      " (e.g., topics are deleted, or offsets are out of range). This may be a false alarm. " +
      "You can disable it when it doesn't work as you expected. " +
      "Batch queries will always fail if it fails to read any data from the provided offsets due to lost data.")
    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
    .required(false)
    .defaultValue("true")
    .build

  val KAFKA_MAX_OFFSETS_PER_TRIGGER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("kafka.maxOffsetsPerTrigger")
    .description("Rate limit on maximum number of offsets processed per trigger interval. " +
      "The specified total number of offsets will be proportionally split across topicPartitions of different volume.")
    .addValidator(StandardValidators.LONG_VALIDATOR)
    .required(false)
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

  //////////////////////////////////////
  // Schema registry (Avro)
  //////////////////////////////////////
  val AVRO_SCHEMA_URL: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("avro.schema.url")
    .description("The avro schema url for the schema registry")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val AVRO_SCHEMA_NAME: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("avro.schema.name")
    .description("The avro schema name to get in the schema registry")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val AVRO_SCHEMA_VERSION: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("avro.schema.version")
    .description("The avro schema version to use for the schema registry")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build
}
