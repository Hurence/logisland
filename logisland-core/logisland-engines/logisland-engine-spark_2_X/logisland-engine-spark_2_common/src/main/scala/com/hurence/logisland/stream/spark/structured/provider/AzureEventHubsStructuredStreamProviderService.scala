/**
 * Copyright (C) 2020 Hurence (support@hurence.com)
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

import java.util
import java.util.Collections

import com.hurence.logisland.annotation.documentation.CapabilityDescription
import com.hurence.logisland.annotation.lifecycle.OnEnabled
import com.hurence.logisland.component.{InitializationException, PropertyDescriptor}
import com.hurence.logisland.controller.{AbstractControllerService, ControllerServiceInitializationContext}
import com.hurence.logisland.record.{FieldDictionary, FieldType, Record, StandardRecord}
import com.hurence.logisland.stream.StreamContext
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.util.spark.ControllerServiceLookupSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Service to allow reading/writing from/to azure event hub with structured streams
  * Developed using documentation at:
  * https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/structured-streaming-eventhubs-integration.md
  */
@CapabilityDescription("Provides a ways to use azure event hubs as input or output in StructuredStream streams")
class AzureEventHubsStructuredStreamProviderService() extends AbstractControllerService with StructuredStreamProviderService {

  var namespace : String = null
  var readPositionString: String = null
  var readPositionLong: Long = 0L
  var readPositionIsString: Boolean = true
  var readPositionType : String = null
  var readEventHub : String = null
  var readSasKeyName : String = null
  var readSasKey : String = null
  var readConsumerGroup : String = null
  var writeEventHub : String = null
  var writeSasKeyName : String = null
  var writeSasKey : String = null

//  var maxEventPerTrigger : Int = Int.MaxValue
//  var maxOperationTimeout : Int = null
//  var threadPoolSize : Int = null
//  var readReceiverTimeout : Int = null
//  var readPrefetchCount : Int = null

  var properties : Map[String, Any] = Map[String, Any]()

  @OnEnabled
  @throws[InitializationException]
  override def init(context: ControllerServiceInitializationContext): Unit = {
    super.init(context)
    this.synchronized {
      try {

        // namespace
        if (!context.getPropertyValue(EVENTHUBS_NAMESPACE).isSet) {
          throw new InitializationException("EventHubs service " + EVENTHUBS_NAMESPACE.getName + " not specified.")
        }
        namespace = context.getPropertyValue(EVENTHUBS_NAMESPACE).asString()

        // readEventHub and writeEventHub
        if (!context.getPropertyValue(EVENTHUBS_READ_EVENT_HUB).isSet &&
          !context.getPropertyValue(EVENTHUBS_WRITE_EVENT_HUB).isSet) {
          throw new InitializationException("EventHubs service must at least have a read or write event hub set.")
        }

        if (context.getPropertyValue(EVENTHUBS_READ_EVENT_HUB).isSet) {
          readEventHub = context.getPropertyValue(EVENTHUBS_READ_EVENT_HUB).asString()
        }

        if (context.getPropertyValue(EVENTHUBS_WRITE_EVENT_HUB).isSet) {
          writeEventHub = context.getPropertyValue(EVENTHUBS_WRITE_EVENT_HUB).asString()
        }

        // maxEventPerTrigger
        if (context.getPropertyValue(EVENTHUBS_MAX_EVENTS_PER_TRIGGER).isSet) {
          properties += (EVENTHUBS_MAX_EVENTS_PER_TRIGGER.getName
            -> context.getPropertyValue(EVENTHUBS_MAX_EVENTS_PER_TRIGGER).asInteger().toInt)
        }

        // maxOperationTimeout
        if (context.getPropertyValue(EVENTHUBS_MAX_OPERATION_TIMEOUT).isSet) {
          properties += (EVENTHUBS_MAX_OPERATION_TIMEOUT.getName
            -> context.getPropertyValue(EVENTHUBS_MAX_OPERATION_TIMEOUT).asInteger().toInt)
        }

        // threadPoolSize
        if (context.getPropertyValue(EVENTHUBS_MAX_THREAD_POOL_SIZE).isSet) {
          properties += (EVENTHUBS_MAX_THREAD_POOL_SIZE.getName
            -> context.getPropertyValue(EVENTHUBS_MAX_THREAD_POOL_SIZE).asInteger().toInt)
        }

        if ((readEventHub == null) && (writeEventHub == null)) {
          throw new InitializationException("EventHubs service must at least have a read or write event hub set.")
        }

        // Get read config properties
        if (readEventHub != null) {

          // readPosition
          val readPosition : Any = context.getPropertyValue(EVENTHUBS_READ_POSITION).asString()

          if ( (readPosition == EVENTHUBS_READ_POSITION_START_OF_STREAM)
            || (readPosition == EVENTHUBS_READ_POSITION_END_OF_STREAM)
            || (readPosition == EVENTHUBS_READ_POSITION_INSTANT_NOW)) {
            readPositionIsString = true
            readPositionString = readPosition.asInstanceOf[String]
          } else  {
            readPositionIsString = false
            readPositionLong = readPosition.asInstanceOf[String].toLong
          }

          // readPositionType
          readPositionType = context.getPropertyValue(EVENTHUBS_READ_POSITION_TYPE).asString()

          // readSasKeyName
          if (!context.getPropertyValue(EVENTHUBS_READ_SAS_KEY_NAME).isSet) {
            throw new InitializationException("EventHubs service read event hub requires "
              + EVENTHUBS_READ_SAS_KEY_NAME.getName)
          }
          readSasKeyName = context.getPropertyValue(EVENTHUBS_READ_SAS_KEY_NAME).asString()

          // readSasKey
          if (!context.getPropertyValue(EVENTHUBS_READ_SAS_KEY).isSet) {
            throw new InitializationException("EventHubs service read event hub requires "
              + EVENTHUBS_READ_SAS_KEY.getName)
          }
          readSasKey = context.getPropertyValue(EVENTHUBS_READ_SAS_KEY).asString()

          // readConsumerGroup
          if (context.getPropertyValue(EVENTHUBS_READ_CONSUMER_GROUP).isSet) {
            readConsumerGroup = context.getPropertyValue(EVENTHUBS_READ_CONSUMER_GROUP).asString()
          }

          // readReceiverTimeout
          if (context.getPropertyValue(EVENTHUBS_READ_RECEIVER_TIMEOUT).isSet) {
            properties += (EVENTHUBS_READ_RECEIVER_TIMEOUT.getName
              -> context.getPropertyValue(EVENTHUBS_READ_RECEIVER_TIMEOUT).asInteger().toInt)
          }

          // readPrefetchCount
          if (context.getPropertyValue(EVENTHUBS_READ_PREFETCH_COUNT).isSet) {
            properties += (EVENTHUBS_READ_PREFETCH_COUNT.getName
              -> context.getPropertyValue(EVENTHUBS_READ_PREFETCH_COUNT).asInteger().toInt)
          }
        }

        // Get write config properties
        if (writeEventHub != null) {

          // writeSasKeyName
          if (!context.getPropertyValue(EVENTHUBS_WRITE_SAS_KEY_NAME).isSet) {
            throw new InitializationException("EventHubs service write event hub requires "
              + EVENTHUBS_WRITE_SAS_KEY_NAME.getName)
          }
          writeSasKeyName = context.getPropertyValue(EVENTHUBS_WRITE_SAS_KEY_NAME).asString()

          // writeSasKey
          if (!context.getPropertyValue(EVENTHUBS_WRITE_SAS_KEY).isSet) {
            throw new InitializationException("EventHubs service write event hub requires "
              + EVENTHUBS_WRITE_SAS_KEY.getName)
          }
          writeSasKey = context.getPropertyValue(EVENTHUBS_WRITE_SAS_KEY).asString()
        }

      } catch {
        case e: Exception =>
          throw new InitializationException(e)
      }
    }
  }

  /**
    * Allows subclasses to register which property descriptor objects are
    * supported.
    *
    * @return PropertyDescriptor objects this processor currently supports
    */
  override def getSupportedPropertyDescriptors() = {
    val descriptors = new util.ArrayList[PropertyDescriptor]
    descriptors.add(EVENTHUBS_NAMESPACE)
    descriptors.add(EVENTHUBS_MAX_EVENTS_PER_TRIGGER)
    descriptors.add(EVENTHUBS_MAX_OPERATION_TIMEOUT)
    descriptors.add(EVENTHUBS_MAX_THREAD_POOL_SIZE)
    descriptors.add(EVENTHUBS_READ_EVENT_HUB)
    descriptors.add(EVENTHUBS_READ_SAS_KEY_NAME)
    descriptors.add(EVENTHUBS_READ_SAS_KEY)
    descriptors.add(EVENTHUBS_READ_CONSUMER_GROUP)
    descriptors.add(EVENTHUBS_READ_POSITION)
    descriptors.add(EVENTHUBS_READ_POSITION_TYPE)
    descriptors.add(EVENTHUBS_READ_RECEIVER_TIMEOUT)
    descriptors.add(EVENTHUBS_READ_PREFETCH_COUNT)
    descriptors.add(EVENTHUBS_WRITE_EVENT_HUB)
    descriptors.add(EVENTHUBS_WRITE_SAS_KEY_NAME)
    descriptors.add(EVENTHUBS_WRITE_SAS_KEY)
    Collections.unmodifiableList(descriptors)
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

    val connectionString = ConnectionStringBuilder()
      .setNamespaceName(namespace)
      .setEventHubName(readEventHub)
      .setSasKeyName(readSasKeyName)
      .setSasKey(readSasKey)
      .build

    var eventHubsConf = EventHubsConf(connectionString)

    logger.info(s"Starting azure event hubs structured stream on event hub $readEventHub in $namespace namespace")
    val df = spark.readStream
      .format("eventhubs")
      .options(eventHubsConf.toMap)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS BINARY)")
    //  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, Array[Byte])]
      .map(r => {
//        new StandardRecord(inputTopics.head)
//          .setField(FieldDictionary.RECORD_KEY, FieldType.STRING, r._1)
//          .setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, r._2)
        new StandardRecord("").asInstanceOf[Record];
      })

    df
  }

  /**
    * create a streaming DataFrame that represents data received
    *
    * @param streamContext
    * @return DataFrame currently loaded
    */
  override def write(df: Dataset[Record], controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink], streamContext: StreamContext) = {


    //    val sender = df.sparkSession.sparkContext.broadcast(KafkaSink(kafkaSinkParams))

    import df.sparkSession.implicits._

    val connectionString = ConnectionStringBuilder()
      .setNamespaceName(namespace)
      .setEventHubName(readEventHub)
      .setSasKeyName(readSasKeyName)
      .setSasKey(readSasKey)
      .build

    var eventHubsConf = EventHubsConf(connectionString)

    logger.info(s"Starting azure event hubs structured stream to event hub $readEventHub in $namespace namespace")

    // Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    df .map(r => {
      (r.getField(FieldDictionary.RECORD_KEY).asString(), r.getField(FieldDictionary.RECORD_VALUE).asBytes())
    })
      .as[(String, Array[Byte])]
      .toDF("key","value")
      .writeStream
      .format("eventhubs")
      .options(eventHubsConf.toMap)
      .option("checkpointLocation", "checkpoints")
  }
}
