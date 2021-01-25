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
package com.hurence.logisland.stream.spark.provider

import java.io.ByteArrayOutputStream
import java.util
import java.util.Collections

import com.hurence.logisland.annotation.lifecycle.OnEnabled
import com.hurence.logisland.component.{InitializationException, PropertyDescriptor}
import com.hurence.logisland.connect.Utils
import com.hurence.logisland.connect.sink.KafkaConnectStreamSink
import com.hurence.logisland.controller.ControllerServiceInitializationContext
import com.hurence.logisland.record.{FieldDictionary, FieldType, Record, StandardRecord}
import com.hurence.logisland.serializer.{RecordSerializer, SerializerProvider}
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.stream.spark.provider.KafkaConnectStructuredSinkProviderService.{AVRO_WRITE_VALUE_SCHEMA, WRITE_KEY_SERIALIZER, WRITE_TOPICS, WRITE_VALUE_SERIALIZER}
import com.hurence.logisland.stream.spark.structured.provider.SerializingTool
import com.hurence.logisland.util.spark.ControllerServiceLookupSink
import com.hurence.logisland.validator.StandardValidators
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.StreamingQuery

class KafkaConnectStructuredSinkProviderService extends KafkaConnectBaseProviderService {


  var maxPartitions = 1
  @transient var writer: KafkaConnectStreamSink = null

  var outputTopicName: String = null
  var writeValueSerializer: RecordSerializer = null
  var writeKeySerializer: RecordSerializer = null

  /**
    * Allows subclasses to register which property descriptor objects are
    * supported.
    *
    * @return PropertyDescriptor objects this processor currently supports
    */
  override def getSupportedPropertyDescriptors() = {
    val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
    descriptors.addAll(super.getSupportedPropertyDescriptors())
    descriptors.add(WRITE_TOPICS)
    descriptors.add(WRITE_VALUE_SERIALIZER)
    descriptors.add(AVRO_WRITE_VALUE_SCHEMA)
    descriptors.add(WRITE_KEY_SERIALIZER)
    Collections.unmodifiableList(descriptors)
  }

  @OnEnabled
  @throws[InitializationException]
  override def init(context: ControllerServiceInitializationContext): Unit = {
    super.init(context)
    this.synchronized {
      try {
        maxPartitions = maxConfigurations
        if (context.getPropertyValue(StreamOptions.KAFKA_CONNECT_MAX_PARTITIONS).isSet) {
          maxPartitions = context.getPropertyValue(StreamOptions.KAFKA_CONNECT_MAX_PARTITIONS).asInteger()
        }
        outputTopicName = context.getPropertyValue(WRITE_TOPICS).asString().split(",")(0).trim
        writeValueSerializer = SerializerProvider.getSerializer(
          context.getPropertyValue(WRITE_VALUE_SERIALIZER).asString,
          context.getPropertyValue(AVRO_WRITE_VALUE_SCHEMA).asString)
        writeKeySerializer = SerializerProvider.getSerializer(
          context.getPropertyValue(WRITE_KEY_SERIALIZER).asString, null)
      } catch {
        case e: Exception =>
          throw new InitializationException(e)
      }
    }
  }


  /**
    * create a streaming DataFrame that represents data to be written
    *
    * @return DataFrame currently loaded
    */
  override def write(df: Dataset[Record], controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink]): StreamingQuery = {
    implicit val encoder = Encoders.tuple(Encoders.BINARY, Encoders.BINARY)

    implicit val recordEncoder = org.apache.spark.sql.Encoders.kryo[Record]

    // Write key-value data from a DataFrame to a specific event hub specified in an option
    val df2 = df
      .mapPartitions(record => record.map(record => SerializingTool.serializeRecords(writeValueSerializer, writeKeySerializer, record)))
      .mapPartitions(record => record.map(record => (record.getField(FieldDictionary.RECORD_KEY).getRawValue().asInstanceOf[Array[Byte]],
        record.getField(FieldDictionary.RECORD_VALUE).getRawValue().asInstanceOf[Array[Byte]])))
      .toDF("key", "value")

    def writer() = controllerServiceLookupSink.value.getControllerService(getIdentifier).asInstanceOf[KafkaConnectStructuredSinkProviderService]
      .createWriter(SparkSession.builder().getOrCreate().sqlContext, outputTopicName)

    df2 /*.repartition(maxPartitions, df2.col("key"))*/
      .writeStream
      .foreach(new ForeachWriter[Row] {

        override def process(value: Row): Unit = {
          writer().enqueueOnPartition(TaskContext.getPartitionId(), value.getAs(0), value.getAs(1))
        }

        override def close(errorOrNull: Throwable): Unit = {
          if (errorOrNull != null) {
            getLogger.error("Error while storing data", errorOrNull)
          }
          writer().flushPartition(TaskContext.getPartitionId())
        }

        override def open(partitionId: Long, version: Long): Boolean = {
          writer().openPartition(partitionId.intValue())
        }
      })
      .start()
  }


  def createWriter(sqlContext: SQLContext, topic: String): KafkaConnectStreamSink =
    synchronized {

      if (writer == null) {
        val keyConverterInstance = Utils.createConverter(keyConverter, keyConverterProperties, true)
        val valueConverterInstance = Utils.createConverter(valueConverter, valueConverterProperties, false)
        //create the right backing store
        val offsetBackingStoreInstance = Utils.createOffsetBackingStore(offsetBackingStore, Utils.propertiesToMap(offsetBackingStoreProperties))

        writer = new KafkaConnectStreamSink(
          sqlContext,
          Utils.propertiesToMap(connectorProperties),
          keyConverterInstance,
          valueConverterInstance,
          offsetBackingStoreInstance,
          maxConfigurations,
          topic,
          delegateConnectorClass,
          getIdentifier)
        writer.start()
      }

      writer
    }


}

object KafkaConnectStructuredSinkProviderService {
  val WRITE_TOPICS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("write.topics")
    .description("the input path for any topic to be written to")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(true)
    .build

  val WRITE_VALUE_SERIALIZER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("write.value.serializer")
    .description("the serializer to use to serialize records into value topic messages")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER, KURA_PROTOCOL_BUFFER_SERIALIZER)
    .defaultValue(NO_SERIALIZER.getValue)
    .build

  val AVRO_WRITE_VALUE_SCHEMA: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("write.value.schema")
    .description("the avro schema definition")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val WRITE_KEY_SERIALIZER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("write.key.serializer")
    .description("The key serializer to use")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER, KURA_PROTOCOL_BUFFER_SERIALIZER)
    .defaultValue(NO_SERIALIZER.getValue)
    .build
}
