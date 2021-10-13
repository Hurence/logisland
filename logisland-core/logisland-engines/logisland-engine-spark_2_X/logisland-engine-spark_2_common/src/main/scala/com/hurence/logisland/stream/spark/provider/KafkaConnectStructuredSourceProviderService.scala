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

package com.hurence.logisland.stream.spark.provider

import java.util
import java.util.Collections

import com.hurence.logisland.annotation.lifecycle.OnEnabled
import com.hurence.logisland.component.{InitializationException, PropertyDescriptor}
import com.hurence.logisland.controller.ControllerServiceInitializationContext
import com.hurence.logisland.record.{FieldDictionary, FieldType, Record, StandardRecord}
import com.hurence.logisland.serializer.{NoopSerializer, RecordSerializer, SerializerProvider}
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.stream.spark.provider.KafkaConnectStructuredSourceProviderService.{AVRO_READ_VALUE_SCHEMA, READ_VALUE_SERIALIZER}
import com.hurence.logisland.stream.spark.structured.provider.SerializingTool
import com.hurence.logisland.validator.StandardValidators
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}

class KafkaConnectStructuredSourceProviderService extends KafkaConnectBaseProviderService {

  var maxPartitions = 1
  var readValueSerializer: RecordSerializer = null

  /**
    * Allows subclasses to register which property descriptor objects are
    * supported.
    *
    * @return PropertyDescriptor objects this processor currently supports
    */
  override def getSupportedPropertyDescriptors() = {
    val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
    descriptors.addAll(super.getSupportedPropertyDescriptors())
    descriptors.add(READ_VALUE_SERIALIZER)
    descriptors.add(AVRO_READ_VALUE_SCHEMA)
    Collections.unmodifiableList(descriptors)
  }

  @OnEnabled
  @throws[InitializationException]
  override def init(context: ControllerServiceInitializationContext): Unit = {
    super.init(context)
    this.synchronized {
      try {
        maxPartitions = SparkContext.getOrCreate().defaultParallelism
        if (context.getPropertyValue(StreamOptions.KAFKA_CONNECT_MAX_PARTITIONS).isSet) {
          maxPartitions = context.getPropertyValue(StreamOptions.KAFKA_CONNECT_MAX_PARTITIONS).asInteger()
        }
        readValueSerializer = SerializerProvider.getSerializer(
          context.getPropertyValue(READ_VALUE_SERIALIZER).asString,
          context.getPropertyValue(AVRO_READ_VALUE_SCHEMA).asString)
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
    * @return DataFrame currently loaded
    */
  override def read(spark: SparkSession): Dataset[Record] = {
    import spark.implicits._
    implicit val recordEncoder = org.apache.spark.sql.Encoders.kryo[Record]

    getLogger.info(s"Connecting kafka-connect source $delegateConnectorClass")
    spark.readStream
      .format("com.hurence.logisland.connect.source.KafkaConnectStreamSourceProvider")
      .option(StreamOptions.KAFKA_CONNECT_CONNECTOR_PROPERTIES.getName, connectorProperties)
      .option(StreamOptions.KAFKA_CONNECT_KEY_CONVERTER.getName, keyConverter)
      .option(StreamOptions.KAFKA_CONNECT_KEY_CONVERTER_PROPERTIES.getName, keyConverterProperties)
      .option(StreamOptions.KAFKA_CONNECT_VALUE_CONVERTER.getName, valueConverter)
      .option(StreamOptions.KAFKA_CONNECT_VALUE_CONVERTER_PROPERTIES.getName, valueConverterProperties)
      .option(StreamOptions.KAFKA_CONNECT_MAX_TASKS.getName, maxConfigurations)
      .option(StreamOptions.KAFKA_CONNECT_CONNECTOR_CLASS.getName, delegateConnectorClass)
      .option(StreamOptions.KAFKA_CONNECT_OFFSET_BACKING_STORE.getName, offsetBackingStore)
      .option(StreamOptions.KAFKA_CONNECT_OFFSET_BACKING_STORE_PROPERTIES.getName, offsetBackingStoreProperties)
      .load()
      //topic, sourcePartition, sourceOffset, key, value
      .as[(String, String, String, Array[Byte], Array[Byte])]
      .flatMap(r => {
        readValueSerializer match {
          case sr: NoopSerializer => Some(new StandardRecord("kafka_connect")
            .setField(FieldDictionary.RECORD_KEY, FieldType.BYTES, r._4)
            .setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, r._5))
          case _ => SerializingTool.deserializeRecords(readValueSerializer, r._5)
        }
      })
      .coalesce(maxPartitions)
  }

}

object KafkaConnectStructuredSourceProviderService {
  val READ_VALUE_SERIALIZER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("read.value.serializer")
    .description("the serializer to use to deserialize value of topic messages as record")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER, KURA_PROTOCOL_BUFFER_SERIALIZER)
    .defaultValue(NO_SERIALIZER.getValue)
    .build

  val AVRO_READ_VALUE_SCHEMA: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("read.value.schema")
    .description("the avro schema definition")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

}
