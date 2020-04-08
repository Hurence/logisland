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
import org.apache.kafka.common.security.JaasUtils
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

  var securityProtocol = ""
  var saslKbServiceName = ""
  var startingOffsets = ""
  var failOnDataLoss = true
  var maxOffsetsPerTrigger: Option[Long] = None


  @OnEnabled
  @throws[InitializationException]
  override def init(context: ControllerServiceInitializationContext): Unit = {
    super.init(context)
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

        securityProtocol = context.getPropertyValue(KAFKA_SECURITY_PROTOCOL).asString
        saslKbServiceName = context.getPropertyValue(KAFKA_SASL_KERBEROS_SERVICE_NAME).asString

        startingOffsets = context.getPropertyValue(KAFKA_STARTING_OFFSETS).asString
        failOnDataLoss = context.getPropertyValue(KAFKA_FAIL_ON_DATA_LOSS).asBoolean

        maxOffsetsPerTrigger = if (context.getPropertyValue(KAFKA_MAX_OFFSETS_PER_TRIGGER).isSet)
          Some(context.getPropertyValue(KAFKA_MAX_OFFSETS_PER_TRIGGER).asLong())
        else None

        // TODO deprecate topic creation here (must be done through the agent)
        if (topicAutocreate) {
          val zkUtils = ZkUtils.apply(zkQuorum, 10000, 10000, JaasUtils.isZkSecurityEnabled)
          createTopicsIfNeeded(zkUtils, inputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
          createTopicsIfNeeded(zkUtils, outputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
          createTopicsIfNeeded(zkUtils, errorTopics, 3, 1)
          createTopicsIfNeeded(zkUtils, metricsTopics, 1, 1)
        }


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
    import spark.implicits._
    implicit val recordEncoder = org.apache.spark.sql.Encoders.kryo[Record]

    logger.info(s"starting Kafka direct stream on topics $inputTopics from $startingOffsets offsets, and maxOffsetsPerTrigger :$maxOffsetsPerTrigger")
    var df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerList)
      .option("kafka.security.protocol", securityProtocol)
      .option("kafka.sasl.kerberos.service.name", saslKbServiceName)
      .option("startingOffsets", startingOffsets)
      .option("failOnDataLoss", failOnDataLoss)
      .option("subscribe", inputTopics.mkString(","))

    if (maxOffsetsPerTrigger.isDefined) {
      df = df.option("maxOffsetsPerTrigger", maxOffsetsPerTrigger.get)
    }


    df.load()
      .selectExpr("CAST(key AS BINARY)", "CAST(value AS BINARY)")
      .as[(Array[Byte], Array[Byte])]
      .map(r => {
        new StandardRecord(inputTopics.head)
          .setField(FieldDictionary.RECORD_KEY, FieldType.BYTES, r._1)
          .setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, r._2)
      })


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
    descriptors.add(KAFKA_STARTING_OFFSETS)
    descriptors.add(KAFKA_FAIL_ON_DATA_LOSS)
    descriptors.add(KAFKA_MAX_OFFSETS_PER_TRIGGER)
    descriptors.add(WINDOW_DURATION)
    descriptors.add(SLIDE_DURATION)
    descriptors.add(KAFKA_SECURITY_PROTOCOL)
    descriptors.add(KAFKA_SASL_KERBEROS_SERVICE_NAME)
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
    df.map(r => {
      (r.getField(FieldDictionary.RECORD_KEY).asBytes(), r.getField(FieldDictionary.RECORD_VALUE).asBytes())
    })
      .as[(Array[Byte], Array[Byte])]
      .toDF("key", "value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerList)
      .option("kafka.security.protocol", securityProtocol)
      .option("kafka.sasl.kerberos.service.name", saslKbServiceName)
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
