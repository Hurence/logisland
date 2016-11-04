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
package com.hurence.logisland.stream.spark

import java.io.ByteArrayInputStream
import java.util
import java.util.Collections

import com.hurence.logisland.component.{AllowableValue, PropertyDescriptor}
import com.hurence.logisland.record.{Field, FieldType, Record}
import com.hurence.logisland.serializer.{AvroSerializer, JsonSerializer, KryoSerializer, RecordSerializer}
import com.hurence.logisland.stream.{AbstractRecordStream, StreamContext}
import com.hurence.logisland.util.kafka.KafkaSink
import com.hurence.logisland.util.processor.ProcessorMetrics
import com.hurence.logisland.util.spark.ZookeeperSink
import com.hurence.logisland.validator.StandardValidators
import kafka.admin.AdminUtils
import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.avro.Schema.Parser
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._


object AbstractKafkaRecordStream {

    val DEFAULT_RAW_TOPIC = new AllowableValue("logisland_raw", "default raw topic", "the incoming non structured topic")
    val DEFAULT_EVENTS_TOPIC = new AllowableValue("logisland_events", "default events topic", "the outgoing structured topic")
    val DEFAULT_ERRORS_TOPIC = new AllowableValue("logisland_errors", "default raw topic", "the outgoing structured error topic")
    val DEFAULT_METRICS_TOPIC = new AllowableValue("logisland_metrics", "default metrics topic", "the topic to place processing metrics")

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
        .defaultValue(DEFAULT_EVENTS_TOPIC.getValue)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

    val ERROR_TOPICS = new PropertyDescriptor.Builder()
        .name("kafka.error.topics")
        .description("Sets the error topics Kafka topic name")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue(DEFAULT_ERRORS_TOPIC.getValue)
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

    val AVRO_SERIALIZER = new AllowableValue(classOf[AvroSerializer].getName, "avro serialization", "serialize events as avro blocs")
    val JSON_SERIALIZER = new AllowableValue(classOf[JsonSerializer].getName, "avro serialization", "serialize events as json blocs")
    val KRYO_SERIALIZER = new AllowableValue(classOf[KryoSerializer].getName, "kryo serialization", "serialize events as json blocs")
    val NO_SERIALIZER = new AllowableValue("none", "no serialization", "send events as bytes")


    val INPUT_SERIALIZER = new PropertyDescriptor.Builder()
        .name("kafka.input.topics.serializer")
        .description("")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, AVRO_SERIALIZER, NO_SERIALIZER)
        .defaultValue(KRYO_SERIALIZER.getValue)
        .build

    val OUTPUT_SERIALIZER = new PropertyDescriptor.Builder()
        .name("kafka.output.topics.serializer")
        .description("")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, AVRO_SERIALIZER, NO_SERIALIZER)
        .defaultValue(KRYO_SERIALIZER.getValue)
        .build

    val ERROR_SERIALIZER = new PropertyDescriptor.Builder()
        .name("kafka.error.topics.serializer")
        .description("")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue(JSON_SERIALIZER.getValue)
        .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, AVRO_SERIALIZER, NO_SERIALIZER)
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
        .defaultValue("8")
        .build

    val KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR = new PropertyDescriptor.Builder()
        .name("kafka.topic.default.replicationFactor")
        .description("if autoCreate is set to true, this will set the number of replica for each partition at topic creation time")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("2")
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
        .defaultValue(LARGEST_OFFSET.getValue)
        .build
}

abstract class AbstractKafkaRecordStream extends AbstractRecordStream with KafkaRecordStream {


    private val logger = LoggerFactory.getLogger(classOf[AbstractKafkaRecordStream])
    protected var kafkaSink: Broadcast[KafkaSink] = null
    protected var zkSink: Broadcast[ZookeeperSink] = null
    protected var appName: String = ""
    @transient protected var ssc: StreamingContext = null
    protected var streamContext: StreamContext = null

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
        Collections.unmodifiableList(descriptors)
    }


    override def setup(appName: String, ssc: StreamingContext, streamContext: StreamContext) = {
        this.appName = appName
        this.ssc = ssc
        this.streamContext = streamContext
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
            val zkClient = new ZkClient(zkQuorum, 3000, 3000, ZKStringSerializer)

            val kafkaSinkParams = Map(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
                ProducerConfig.CLIENT_ID_CONFIG -> appName,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getCanonicalName,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
                ProducerConfig.ACKS_CONFIG -> "1",
                ProducerConfig.BATCH_SIZE_CONFIG -> "5000",
                ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> "1000",
                ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG -> "1000")

            kafkaSink = ssc.sparkContext.broadcast(KafkaSink(kafkaSinkParams))
            zkSink = ssc.sparkContext.broadcast(ZookeeperSink(zkQuorum))

            if (topicAutocreate) {
                createTopicsIfNeeded(zkClient, inputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
                createTopicsIfNeeded(zkClient, outputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
                createTopicsIfNeeded(zkClient, errorTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
                if (streamContext.getPropertyValue(AbstractKafkaRecordStream.METRICS_TOPIC).isSet) {
                    createTopicsIfNeeded(
                        zkClient,
                        Set(streamContext.getPropertyValue(AbstractKafkaRecordStream.METRICS_TOPIC).asString),
                        topicDefaultPartitions,
                        topicDefaultReplicationFactor)
                }
            }

            /**
              * create streams from latest zk offsets
              */
            val kafkaStreamsParams = Map(
                "metadata.broker.list" -> brokerList,
                "bootstrap.servers" -> brokerList,
                "group.id" -> appName,
                "refresh.leader.backoff.ms" -> "1000",
                "auto.offset.reset" -> streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_MANUAL_OFFSET_RESET).asString()
            )

            val offsets = zkSink.value.loadOffsetRangesFromZookeeper(appName, inputTopics)
            @transient val kafkaStream = if (
                streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_MANUAL_OFFSET_RESET).isSet
                    || offsets.isEmpty) {
                KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
                    ssc,
                    kafkaStreamsParams,
                    inputTopics
                )
            }
            else {
                val messageHandler: MessageAndMetadata[Array[Byte], Array[Byte]] => (Array[Byte], Array[Byte]) =
                    (mmd: MessageAndMetadata[Array[Byte], Array[Byte]]) => (mmd.key, mmd.message)

                KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder, (Array[Byte], Array[Byte])](
                    ssc,
                    kafkaStreamsParams,
                    offsets,
                    messageHandler)
            }


            // do the parallel processing
            kafkaStream.foreachRDD(rdd => process(rdd))
        } catch {
            case ex: Exception => logger.error("something bad happened, please check Kafka or cluster health : {}",
                ex.getMessage)
        }
    }


    /**
      * to be overriden by subclasses
      *
      * @param rdd
      */
    def process(rdd: RDD[(Array[Byte], Array[Byte])])


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
            case _ => new KryoSerializer(true)
        }
    }

    /**
      *
      * @param partition
      * @param serializer
      * @return
      */
    def deserializeRecords(partition: Iterator[(Array[Byte], Array[Byte])], serializer: RecordSerializer): List[Record] = {
        partition.flatMap(rawEvent => {

            try {
                val bais = new ByteArrayInputStream(rawEvent._2)
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

    def computeMetrics(appName: String,
                       streamContext: StreamContext,
                       inputTopics: String,
                       outputTopics: String,
                       partition: Iterator[(Array[Byte], Array[Byte])],
                       startTime: Long,
                       partitionId: Int,
                       serializer: RecordSerializer,
                       incomingEvents: util.Collection[Record],
                       outgoingEvents: util.Collection[Record],
                       osr: OffsetRange): util.Collection[Record] = {

        val processorFields = new util.HashMap[String, Field]()
        processorFields.put("spark_app_name", new Field("spark_app_name", FieldType.STRING, appName))
        processorFields.put("input_topics", new Field("input_topics", FieldType.STRING, inputTopics))
        processorFields.put("output_topics", new Field("output_topics", FieldType.STRING, outputTopics))
        processorFields.put("component_name", new Field("component_name", FieldType.STRING, streamContext.getName))
        processorFields.put("topic_offset_from", new Field("topic_offset_from", FieldType.LONG, osr.fromOffset))
        processorFields.put("topic_offset_until", new Field("topic_offset_until", FieldType.LONG, osr.untilOffset))
        processorFields.put("spark_partition_id", new Field("spark_partition_id", FieldType.INT, partitionId))
        processorFields.put("num_incoming_messages", new Field("num_incoming_messages", FieldType.INT, osr.untilOffset - osr.fromOffset))
        processorFields.put("num_incoming_events", new Field("num_incoming_events", FieldType.INT, incomingEvents.size))
        processorFields.put("num_outgoing_events", new Field("num_outgoing_events", FieldType.INT, outgoingEvents.size))

        val metrics = ProcessorMetrics.computeMetrics(incomingEvents, processorFields, System.currentTimeMillis() - startTime).toList
        metrics
    }

    /**
      * Topic creation
      *
      * @param zkClient
      * @param inputTopics
      * @param topicDefaultPartitions
      * @param topicDefaultReplicationFactor
      */
    def createTopicsIfNeeded(zkClient: ZkClient,
                             inputTopics: Set[String],
                             topicDefaultPartitions: Int,
                             topicDefaultReplicationFactor: Int): Unit = {

        inputTopics.foreach(topic => {
            if (!AdminUtils.topicExists(zkClient, topic)) {
                AdminUtils.createTopic(zkClient, topic, topicDefaultPartitions, topicDefaultReplicationFactor)
                Thread.sleep(1000)
                logger.info(s"created topic $topic with" +
                    s" $topicDefaultPartitions partitions and" +
                    s" $topicDefaultReplicationFactor replicas")
            }
        })
    }
}


