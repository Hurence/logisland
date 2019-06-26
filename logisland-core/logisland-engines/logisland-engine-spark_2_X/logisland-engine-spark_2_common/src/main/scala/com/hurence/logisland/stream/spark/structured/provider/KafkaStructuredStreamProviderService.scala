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
package com.hurence.logisland.stream.spark.structured.provider

import java.util
import java.util.Collections

import com.hurence.logisland.annotation.documentation.CapabilityDescription
import com.hurence.logisland.annotation.lifecycle.OnEnabled
import com.hurence.logisland.component.{InitializationException, PropertyDescriptor}
import com.hurence.logisland.controller.{AbstractControllerService, ControllerServiceInitializationContext}
import com.hurence.logisland.record.{FieldDictionary, FieldType, Record, StandardRecord}
import com.hurence.logisland.stream.StreamContext
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.util.kafka.KafkaSink
import com.hurence.logisland.util.spark.ControllerServiceLookupSink
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Compatible with kafka 0.10.0 or higher
  */
@CapabilityDescription("Provide a ways to use kafka as input or output in StructuredStream streams")
class KafkaStructuredStreamProviderService() extends AbstractControllerService with StructuredStreamProviderService {

  // private val logger = LoggerFactory.getLogger(this.getClass)


  var appName = ""
  var kafkaSinkParams: Map[String, Object] = _
  var kafkaParams: Map[String, Object] = _
  // Define the Kafka parameters, broker list must be specified
  var inputTopics = Set[String]()
  var outputTopics = Set[String]()
  var errorTopics = Set[String]()
  var metricsTopics = Set[String]()
  var topicAutocreate = true
  var topicDefaultPartitions = 3
  var topicDefaultReplicationFactor = 1
  var brokerList = ""
  var zkQuorum = ""
  var kafkaBatchSize = "16384"
  var kafkaLingerMs = "5"
  var kafkaAcks = "0"
  var kafkaOffset = "latest"
  var inputSerializerType = ""
  var outputSerializerType = ""

  @OnEnabled
  @throws[InitializationException]
  override def init(context: ControllerServiceInitializationContext): Unit = {
    this.synchronized {
      try {

        // Define the Kafka parameters, broker list must be specified
        inputTopics = context.getPropertyValue(INPUT_TOPICS).asString.split(",").toSet
        outputTopics = context.getPropertyValue(OUTPUT_TOPICS).asString.split(",").toSet
        errorTopics = context.getPropertyValue(ERROR_TOPICS).asString.split(",").toSet
        metricsTopics = DEFAULT_METRICS_TOPIC.getValue.split(",").toSet

        inputSerializerType = context.getPropertyValue(INPUT_SERIALIZER).asString()
        outputSerializerType = context.getPropertyValue(OUTPUT_SERIALIZER).asString()

        topicAutocreate = context.getPropertyValue(KAFKA_TOPIC_AUTOCREATE).asBoolean().booleanValue()
        topicDefaultPartitions = context.getPropertyValue(KAFKA_TOPIC_DEFAULT_PARTITIONS).asInteger().intValue()
        topicDefaultReplicationFactor = context.getPropertyValue(KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR).asInteger().intValue()
        brokerList = context.getPropertyValue(KAFKA_METADATA_BROKER_LIST).asString
        zkQuorum = context.getPropertyValue(KAFKA_ZOOKEEPER_QUORUM).asString


        kafkaBatchSize = context.getPropertyValue(KAFKA_BATCH_SIZE).asString
        kafkaLingerMs = context.getPropertyValue(KAFKA_LINGER_MS).asString
        kafkaAcks = context.getPropertyValue(KAFKA_ACKS).asString
        kafkaOffset = context.getPropertyValue(KAFKA_MANUAL_OFFSET_RESET).asString


        kafkaSinkParams = Map(
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
          ProducerConfig.CLIENT_ID_CONFIG -> appName,
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getCanonicalName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
          ProducerConfig.ACKS_CONFIG -> kafkaAcks,
          ProducerConfig.RETRIES_CONFIG -> "3",
          ProducerConfig.LINGER_MS_CONFIG -> kafkaLingerMs,
          ProducerConfig.BATCH_SIZE_CONFIG -> kafkaBatchSize,
          ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> "1000",
          ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG -> "1000")


        // TODO deprecate topic creation here (must be done through the agent)
        if (topicAutocreate) {
          val zkUtils = ZkUtils.apply(zkQuorum, 10000, 10000, JaasUtils.isZkSecurityEnabled)
          createTopicsIfNeeded(zkUtils, inputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
          createTopicsIfNeeded(zkUtils, outputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
          createTopicsIfNeeded(zkUtils, errorTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
          createTopicsIfNeeded(zkUtils, metricsTopics, 1, 1)
        }


        kafkaParams = Map[String, Object](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer],
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer],
          ConsumerConfig.GROUP_ID_CONFIG -> appName,
          ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG -> "50",
          ConsumerConfig.RETRY_BACKOFF_MS_CONFIG -> "100",
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> kafkaOffset,
          ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
          ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> "30000"
          /*,
          ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "5000"*/
        )

      } catch {
        case e: Exception =>
          throw new InitializationException(e)
      }
    }
  }

  /**
    * create a streaming DataFrame that represents data received
    *
    * @param spark
    * @param streamContext
    * @return DataFrame currently loaded
    */
  override def read(spark: SparkSession, streamContext: StreamContext) = {
    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Record]
    import spark.implicits._

    logger.info(s"starting Kafka direct stream on topics $inputTopics from $kafkaOffset offsets")
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerList)
      .option("subscribe", inputTopics.mkString(","))
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .map(r => {
        new StandardRecord(inputTopics.head)
          .setField(FieldDictionary.RECORD_KEY, FieldType.BYTES, r._1)
          .setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, r._2)
      })

    df
  }

  /**
    * Allows subclasses to register which property descriptor objects are
    * supported.
    *
    * @return PropertyDescriptor objects this processor currently supports
    */
  override def getSupportedPropertyDescriptors() = {
    val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
    descriptors.add(ERROR_TOPICS)
    descriptors.add(INPUT_TOPICS)
    descriptors.add(OUTPUT_TOPICS)
    descriptors.add(AVRO_INPUT_SCHEMA)
    descriptors.add(AVRO_OUTPUT_SCHEMA)
    descriptors.add(INPUT_SERIALIZER)
    descriptors.add(OUTPUT_SERIALIZER)
    descriptors.add(ERROR_SERIALIZER)
    descriptors.add(KAFKA_TOPIC_AUTOCREATE)
    descriptors.add(KAFKA_TOPIC_DEFAULT_PARTITIONS)
    descriptors.add(KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR)
    descriptors.add(KAFKA_METADATA_BROKER_LIST)
    descriptors.add(KAFKA_ZOOKEEPER_QUORUM)
    descriptors.add(KAFKA_MANUAL_OFFSET_RESET)
    descriptors.add(KAFKA_BATCH_SIZE)
    descriptors.add(KAFKA_LINGER_MS)
    descriptors.add(KAFKA_ACKS)
    descriptors.add(WINDOW_DURATION)
    descriptors.add(SLIDE_DURATION)
    Collections.unmodifiableList(descriptors)
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

  case class RecordWrapper(record:Record)

  /**
    * create a streaming DataFrame that represents data received
    *
    * @param streamContext
    * @return DataFrame currently loaded
    */
  override def write(df: Dataset[Record], controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink], streamContext: StreamContext) = {
    val sender = df.sparkSession.sparkContext.broadcast(KafkaSink(kafkaSinkParams))

    import df.sparkSession.implicits._

    // Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    df .map(r => {
      (r.getField(FieldDictionary.RECORD_ID).asString(), r.getField(FieldDictionary.RECORD_VALUE).asString())
    })
      .as[(String, String)]
      .toDF("key","value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerList)
      .option("topic", outputTopics.mkString(","))
      .option("checkpointLocation", "checkpoints")

  }

  private def getOrElse[T](record: Record, field: String, defaultValue: T): T = {
    val value = record.getField(field)
    if (value != null && value.isSet) {
      return value.getRawValue.asInstanceOf[T]
    }
    defaultValue
  }


}
