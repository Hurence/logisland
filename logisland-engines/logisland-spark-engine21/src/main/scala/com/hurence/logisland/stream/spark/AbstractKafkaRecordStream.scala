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
  * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.stream.spark

import java.io.ByteArrayInputStream
import java.util
import java.util.Collections

import com.hurence.logisland.component.{AllowableValue, PropertyDescriptor, RestComponentFactory}
import com.hurence.logisland.engine.EngineContext
import com.hurence.logisland.record.Record
import com.hurence.logisland.serializer.{AvroSerializer, JsonSerializer, KryoSerializer, BytesArraySerializer, RecordSerializer}
import com.hurence.logisland.stream.{AbstractRecordStream, StreamContext}
import com.hurence.logisland.util.spark._
import com.hurence.logisland.validator.StandardValidators
import kafka.admin.AdminUtils
import kafka.message.MessageAndMetadata
import kafka.utils.ZkUtils
import org.apache.avro.Schema.Parser
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.{Assign, Subscribe}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, KafkaUtils, OffsetRange}
import org.slf4j.LoggerFactory


object AbstractKafkaRecordStream {

    val DEFAULT_RAW_TOPIC = new AllowableValue("_raw", "default raw topic", "the incoming non structured topic")
    val DEFAULT_RECORDS_TOPIC = new AllowableValue("_records", "default events topic", "the outgoing structured topic")
    val DEFAULT_ERRORS_TOPIC = new AllowableValue("_errors", "default raw topic", "the outgoing structured error topic")
    val DEFAULT_METRICS_TOPIC = new AllowableValue("_metrics", "default metrics topic", "the topic to place processing metrics")

    val INPUT_TOPICS = new PropertyDescriptor.Builder()
        .name("kafka.input.topics")
        .description("Sets the input Kafka topic name")
        .required(true)
        .defaultValue(DEFAULT_RAW_TOPIC.getValue)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

    val OUTPUT_TOPICS = new PropertyDescriptor.Builder()
        .name("kafka.output.topics")
        .description("Sets the output Kafka topic name")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue(DEFAULT_RECORDS_TOPIC.getValue)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

    val ERROR_TOPICS = new PropertyDescriptor.Builder()
        .name("kafka.error.topics")
        .description("Sets the error topics Kafka topic name")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue(DEFAULT_ERRORS_TOPIC.getValue)
        .build

    val INPUT_TOPICS_PARTITIONS = new PropertyDescriptor.Builder()
        .name("kafka.input.topics.partitions")
        .description("if autoCreate is set to true, this will set the number of partition at topic creation time")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("20")
        .build

    val OUTPUT_TOPICS_PARTITIONS = new PropertyDescriptor.Builder()
        .name("kafka.output.topics.partitions")
        .description("if autoCreate is set to true, this will set the number of partition at topic creation time")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("20")
        .build

    val AVRO_INPUT_SCHEMA = new PropertyDescriptor.Builder()
        .name("avro.input.schema")
        .description("the avro schema definition")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

    val AVRO_OUTPUT_SCHEMA = new PropertyDescriptor.Builder()
        .name("avro.output.schema")
        .description("the avro schema definition for the output serialization")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

    val AVRO_SERIALIZER = new AllowableValue(classOf[AvroSerializer].getName,
        "avro serialization", "serialize events as avro blocs")
    val JSON_SERIALIZER = new AllowableValue(classOf[JsonSerializer].getName,
        "avro serialization", "serialize events as json blocs")
    val KRYO_SERIALIZER = new AllowableValue(classOf[KryoSerializer].getName,
        "kryo serialization", "serialize events as json blocs")
    val BYTESARRAY_SERIALIZER = new AllowableValue(classOf[BytesArraySerializer].getName,
        "byte array serialization", "serialize events as byte arrays")
    val NO_SERIALIZER = new AllowableValue("none", "no serialization", "send events as bytes")


    val INPUT_SERIALIZER = new PropertyDescriptor.Builder()
        .name("kafka.input.topics.serializer")
        .description("")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, NO_SERIALIZER)
        .defaultValue(KRYO_SERIALIZER.getValue)
        .build

    val OUTPUT_SERIALIZER = new PropertyDescriptor.Builder()
        .name("kafka.output.topics.serializer")
        .description("")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, NO_SERIALIZER)
        .defaultValue(KRYO_SERIALIZER.getValue)
        .build

    val ERROR_SERIALIZER = new PropertyDescriptor.Builder()
        .name("kafka.error.topics.serializer")
        .description("")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue(JSON_SERIALIZER.getValue)
        .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, NO_SERIALIZER)
        .build

    val METRICS_TOPIC = new PropertyDescriptor.Builder()
        .name("kafka.metrics.topic")
        .description("a topic to send metrics of processing. no output if not set")
        .required(false)
        .defaultValue(DEFAULT_METRICS_TOPIC.getValue)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

    val KAFKA_TOPIC_AUTOCREATE = new PropertyDescriptor.Builder()
        .name("kafka.topic.autoCreate")
        .description("define wether a topic should be created automatically if not already exists")
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("true")
        .build

    val KAFKA_TOPIC_DEFAULT_PARTITIONS = new PropertyDescriptor.Builder()
        .name("kafka.topic.default.partitions")
        .description("if autoCreate is set to true, this will set the number of partition at topic creation time")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("20")
        .build

    val KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR = new PropertyDescriptor.Builder()
        .name("kafka.topic.default.replicationFactor")
        .description("if autoCreate is set to true, this will set the number of replica for each partition at topic creation time")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("3")
        .build

    val KAFKA_METADATA_BROKER_LIST = new PropertyDescriptor.Builder()
        .name("kafka.metadata.broker.list")
        .description("a comma separated list of host:port brokers")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("sandbox:9092")
        .build

    val KAFKA_ZOOKEEPER_QUORUM = new PropertyDescriptor.Builder()
        .name("kafka.zookeeper.quorum")
        .description("")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("sandbox:2181")
        .build

    val LARGEST_OFFSET = new AllowableValue("largest", "largest offset", "the offset to the largest offset")
    val SMALLEST_OFFSET = new AllowableValue("smallest", "smallest offset", "the offset to the smallest offset")

    val KAFKA_MANUAL_OFFSET_RESET = new PropertyDescriptor.Builder()
        .name("kafka.manual.offset.reset")
        .description("Sets manually an initial offset in ZooKeeper: "
            + "smallest (automatically reset the offset to the smallest offset), "
            + "largest (automatically reset the offset to the largest offset), "
            + "anything else (throw exception to the consumer)")
        .required(false)
        .allowableValues(LARGEST_OFFSET, SMALLEST_OFFSET)
        .build

    val LOGISLAND_AGENT_QUORUM = new PropertyDescriptor.Builder()
        .name("logisland.agent.quorum")
        .description("the stream needs to know how to reach Agent REST api in order to live update its processors")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("sandbox:8081")
        .build

    val LOGISLAND_AGENT_PULL_THROTTLING = new PropertyDescriptor.Builder()
        .name("logisland.agent.pull.throttling")
        .description("wait every x batch to pull agent for new conf")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("10")
        .build
}

abstract class AbstractKafkaRecordStream extends AbstractRecordStream with KafkaRecordStream {

    val NONE_TOPIC: String = "none"
    private val logger = LoggerFactory.getLogger(classOf[AbstractKafkaRecordStream])
    protected var kafkaSink: Broadcast[KafkaSink] = null
    protected var zkSink: Broadcast[ZookeeperSink] = null
    protected var appName: String = ""
    @transient protected var ssc: StreamingContext = null
    protected var streamContext: StreamContext = null
    protected var engineContext: EngineContext = null
    protected var restApiSink: Broadcast[RestJobsApiClientSink] = null
    protected var controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink] = null
    protected var currentJobVersion: Int = 0
    protected var lastCheckCount: Int = 0

    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
        descriptors.add(AbstractKafkaRecordStream.ERROR_TOPICS)
        descriptors.add(AbstractKafkaRecordStream.INPUT_TOPICS)
        descriptors.add(AbstractKafkaRecordStream.OUTPUT_TOPICS)
        descriptors.add(AbstractKafkaRecordStream.METRICS_TOPIC)
        descriptors.add(AbstractKafkaRecordStream.AVRO_INPUT_SCHEMA)
        descriptors.add(AbstractKafkaRecordStream.AVRO_OUTPUT_SCHEMA)
        descriptors.add(AbstractKafkaRecordStream.INPUT_SERIALIZER)
        descriptors.add(AbstractKafkaRecordStream.OUTPUT_SERIALIZER)
        descriptors.add(AbstractKafkaRecordStream.ERROR_SERIALIZER)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_TOPIC_AUTOCREATE)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_PARTITIONS)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_METADATA_BROKER_LIST)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_ZOOKEEPER_QUORUM)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_MANUAL_OFFSET_RESET)
        descriptors.add(AbstractKafkaRecordStream.LOGISLAND_AGENT_QUORUM)
        descriptors.add(AbstractKafkaRecordStream.LOGISLAND_AGENT_PULL_THROTTLING)
        Collections.unmodifiableList(descriptors)
    }


    override def setup(appName: String, ssc: StreamingContext, streamContext: StreamContext, engineContext: EngineContext) = {
        this.appName = appName
        this.ssc = ssc
        this.streamContext = streamContext
        this.engineContext = engineContext
        SparkUtils.customizeLogLevels
    }

    override def getStreamContext() : StreamingContext = {
        return(this.ssc);
    }

    override def start() = {
        if (ssc == null)
            throw new IllegalStateException("stream not initialized")

        try {

            // Define the Kafka parameters, broker list must be specified
            val inputTopics = streamContext.getPropertyValue(AbstractKafkaRecordStream.INPUT_TOPICS).asString.split(",").toSet
            val outputTopics = streamContext.getPropertyValue(AbstractKafkaRecordStream.OUTPUT_TOPICS).asString.split(",").toSet
            val errorTopics = streamContext.getPropertyValue(AbstractKafkaRecordStream.ERROR_TOPICS).asString.split(",").toSet
            val topicAutocreate = streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_TOPIC_AUTOCREATE).asBoolean().booleanValue()
            val topicDefaultPartitions = streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_PARTITIONS).asInteger().intValue()
            val topicDefaultReplicationFactor = streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR).asInteger().intValue()
            val brokerList = streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_METADATA_BROKER_LIST).asString
            val zkQuorum = streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_ZOOKEEPER_QUORUM).asString
            val zkUtils = ZkUtils.apply(zkQuorum, 10000, 10000, JaasUtils.isZkSecurityEnabled)
            val agentQuorum = streamContext.getPropertyValue(AbstractKafkaRecordStream.LOGISLAND_AGENT_QUORUM).asString
            val throttling = streamContext.getPropertyValue(AbstractKafkaRecordStream.LOGISLAND_AGENT_PULL_THROTTLING).asInteger()

            val kafkaSinkParams = Map(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
                ProducerConfig.CLIENT_ID_CONFIG -> appName,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getCanonicalName,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
                ProducerConfig.ACKS_CONFIG -> "all",
                ProducerConfig.RETRIES_CONFIG -> "3",
                ProducerConfig.LINGER_MS_CONFIG -> "5",
                ProducerConfig.BATCH_SIZE_CONFIG -> "20000",
                ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> "1000",
                ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG -> "1000")

            kafkaSink = ssc.sparkContext.broadcast(KafkaSink(kafkaSinkParams))
            zkSink = ssc.sparkContext.broadcast(ZookeeperSink(zkQuorum))
            restApiSink = ssc.sparkContext.broadcast(RestJobsApiClientSink(agentQuorum))
            controllerServiceLookupSink = ssc.sparkContext.broadcast(
                ControllerServiceLookupSink(engineContext.getControllerServiceConfigurations)
            )

            // TODO deprecate topic creation here (must be done through the agent)
            if (topicAutocreate) {
                createTopicsIfNeeded(zkUtils, inputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
                createTopicsIfNeeded(zkUtils, outputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
                createTopicsIfNeeded(zkUtils, errorTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
                createTopicsIfNeeded(
                    zkUtils,
                    Set(streamContext.getPropertyValue(AbstractKafkaRecordStream.METRICS_TOPIC).asString),
                    topicDefaultPartitions,
                    topicDefaultReplicationFactor)

            }


            val kafkaParams = Map[String, Object](
                "bootstrap.servers" -> brokerList,
                "key.deserializer" -> classOf[ByteArrayDeserializer],
                "value.deserializer" -> classOf[ByteArrayDeserializer],
                "group.id" -> appName,
                "refresh.leader.backoff.ms" -> "5000",
                "auto.offset.reset" -> "latest",
                "enable.auto.commit" -> (false: java.lang.Boolean)
            )





            val fromOffsets = zkSink.value.loadOffsetRangesFromZookeeper(brokerList, appName, inputTopics)
            @transient val kafkaStream = if (
                streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_MANUAL_OFFSET_RESET).isSet
                    || fromOffsets.isEmpty) {

                logger.info(s"starting Kafka direct stream on topics $inputTopics from largest offsets")
                KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
                    ssc,
                    PreferConsistent,
                    Subscribe[Array[Byte], Array[Byte]](inputTopics, kafkaParams)
                )
            }
            else {
                val messageHandler: MessageAndMetadata[Array[Byte], Array[Byte]] => (Array[Byte], Array[Byte]) =
                    (mmd: MessageAndMetadata[Array[Byte], Array[Byte]]) => (mmd.key, mmd.message)

                logger.info(s"starting Kafka direct stream on topics $inputTopics from offsets $fromOffsets")
                KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
                    ssc,
                    PreferConsistent,
                    Assign[Array[Byte], Array[Byte]](fromOffsets.keys.toList, kafkaParams, fromOffsets)
                )
            }

            // store current configuration version
            currentJobVersion = restApiSink.value.getJobApiClient.getJobVersion(appName)

            // do the parallel processing
            kafkaStream.foreachRDD(rdd => {
                /**
                  * check if conf needs to be refreshed
                  */
                if (lastCheckCount > throttling) {
                    lastCheckCount = 0
                    val version = restApiSink.value.getJobApiClient.getJobVersion(appName)
                    if (currentJobVersion != version) {
                        logger.info("Job version change detected from {} to {}, proceeding to update",
                            currentJobVersion,
                            version)

                        val componentFactory = new RestComponentFactory(agentQuorum)
                        val updatedEngineContext = componentFactory.getEngineContext(appName)
                        if (updatedEngineContext.isPresent) {

                            // find the corresponding stream
                            val it = updatedEngineContext.get().getStreamContexts.iterator()
                            while (it.hasNext) {
                                val updatedStreamingContext = it.next()

                                // if we found a streamContext with the same name from the factory
                                if (updatedStreamingContext.getName == this.streamContext.getName) {
                                    logger.info("new conf for stream {}", updatedStreamingContext.getName)
                                    this.streamContext = updatedStreamingContext
                                }
                            }
                        }
                        currentJobVersion = version
                    }
                }

                lastCheckCount += 1



                val offsetRanges = process(rdd)
                // some time later, after outputs have completed
                if (offsetRanges.isDefined)
                    kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges.get)
            })
        } catch {
            case ex: Throwable =>
                ex.printStackTrace()
                logger.error("something bad happened, please check Kafka or Zookeeper health : {}", ex)
        }
    }


    /**
      * to be overriden by subclasses
      *
      * @param rdd
      */
    def process(rdd: RDD[ConsumerRecord[Array[Byte], Array[Byte]]]): Option[Array[OffsetRange]]


    /**
      * build a serializer
      *
      * @param inSerializerClass the serializer type
      * @param schemaContent     an Avro schema
      * @return the serializer
      */
    def getSerializer(inSerializerClass: String, schemaContent: String): RecordSerializer = {
        // TODO move this in a utility class
        inSerializerClass match {
            case c if c == AbstractKafkaRecordStream.AVRO_SERIALIZER.getValue =>
                val parser = new Parser
                val inSchema = parser.parse(schemaContent)
                new AvroSerializer(inSchema)
            case c if c == AbstractKafkaRecordStream.JSON_SERIALIZER.getValue => new JsonSerializer()
            case c if c == AbstractKafkaRecordStream.BYTESARRAY_SERIALIZER.getValue => new BytesArraySerializer()
            case _ => new KryoSerializer(true)
        }
    }

    /**
      *
      * @param partition
      * @param serializer
      * @return
      */
    def deserializeRecords(partition: Iterator[ConsumerRecord[Array[Byte], Array[Byte]]], serializer: RecordSerializer): List[Record] = {
        partition.flatMap(rawEvent => {

            // TODO handle key also
            try {
                val bais = new ByteArrayInputStream(rawEvent.value())
                val deserialized = serializer.deserialize(bais)
                bais.close()

                Some(deserialized)
            } catch {
                case t: Throwable =>
                    logger.error(s"exception while deserializing events ${t.getMessage}")
                    None
            }

        }).toList
    }


    /**
      * Topic creation
      *
      * @param zkUtils
      * @param topics
      * @param topicDefaultPartitions
      * @param topicDefaultReplicationFactor
      */
    def createTopicsIfNeeded(zkUtils: ZkUtils,
                             topics: Set[String],
                             topicDefaultPartitions: Int,
                             topicDefaultReplicationFactor: Int): Unit = {

        topics.foreach(topic => {

            if (!topic.equals(NONE_TOPIC) && !AdminUtils.topicExists(zkUtils, topic)) {
                AdminUtils.createTopic(zkUtils, topic, topicDefaultPartitions, topicDefaultReplicationFactor)
                Thread.sleep(1000)
                logger.info(s"created topic $topic with" +
                    s" $topicDefaultPartitions partitions and" +
                    s" $topicDefaultReplicationFactor replicas")
            }
        })
    }
}


