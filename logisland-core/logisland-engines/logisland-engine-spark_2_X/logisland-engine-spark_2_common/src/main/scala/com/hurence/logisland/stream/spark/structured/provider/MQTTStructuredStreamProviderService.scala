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
package com.hurence.logisland.stream.spark.structured.provider


import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.sql.Timestamp
import java.util
import java.util.Collections

import com.hurence.logisland.annotation.documentation.CapabilityDescription
import com.hurence.logisland.annotation.lifecycle.OnEnabled
import com.hurence.logisland.component.{InitializationException, PropertyDescriptor}
import com.hurence.logisland.controller.{AbstractControllerService, ControllerServiceInitializationContext}
import com.hurence.logisland.record.{FieldDictionary, FieldType, Record, StandardRecord}
import com.hurence.logisland.serializer.{NoopSerializer, RecordSerializer, SerializerProvider}
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.stream.spark.structured.provider.MQTTStructuredStreamProviderService.{AVRO_READ_VALUE_SCHEMA, AVRO_WRITE_VALUE_SCHEMA, READ_VALUE_SERIALIZER, WRITE_KEY_SERIALIZER, WRITE_VALUE_SERIALIZER}
import com.hurence.logisland.util.spark.ControllerServiceLookupSink
import com.hurence.logisland.validator.StandardValidators
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, SparkSession}

@CapabilityDescription("Provide a ways to use Mqtt a input or output in StructuredStream streams")
class MQTTStructuredStreamProviderService extends AbstractControllerService
  with StructuredStreamProviderServiceReader
  with StructuredStreamProviderServiceWriter {

    var brokerUrl = ""
    var persistence = ""
    var clientId = ""
    var QoS = 0
    var username = ""
    var password = ""
    var cleanSession = true
    var connectionTimeout = 5000
    var keepAlive = 30000
    var mqttVersion = "3.1.1"
    var topic = ""

    var readValueSerializer: RecordSerializer = null
    var writeValueSerializer: RecordSerializer = null
    var writeKeySerializer: RecordSerializer = null

    @OnEnabled
    @throws[InitializationException]
    override def init(context: ControllerServiceInitializationContext): Unit = {
        super.init(context)
        this.synchronized {
            try {

                // Define the MQTT parameters, broker list must be specified
                brokerUrl = context.getPropertyValue(MQTT_BROKER_URL).asString
                persistence = context.getPropertyValue(MQTT_PERSISTENCE).asString
                clientId = context.getPropertyValue(MQTT_CLIENTID).asString
                QoS = context.getPropertyValue(MQTT_QOS).asInteger().intValue()
                username = context.getPropertyValue(MQTT_USERNAME).asString
                password = context.getPropertyValue(MQTT_PASSWORD).asString
                cleanSession = context.getPropertyValue(MQTT_CLEAN_SESSION).asBoolean().booleanValue()
                connectionTimeout = context.getPropertyValue(MQTT_CONNECTION_TIMEOUT).asInteger().intValue()
                keepAlive = context.getPropertyValue(MQTT_KEEP_ALIVE).asInteger().intValue()
                mqttVersion = context.getPropertyValue(MQTT_VERSION).asString
                topic = context.getPropertyValue(MQTT_TOPIC).asString
                readValueSerializer = SerializerProvider.getSerializer(
                    context.getPropertyValue(READ_VALUE_SERIALIZER).asString,
                    context.getPropertyValue(AVRO_READ_VALUE_SCHEMA).asString)

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
      * Allows subclasses to register which property descriptor objects are
      * supported.
      *
      * @return PropertyDescriptor objects this processor currently supports
      */
    override def getSupportedPropertyDescriptors() = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
        descriptors.add(MQTT_BROKER_URL)
        descriptors.add(MQTT_CLEAN_SESSION)
        descriptors.add(MQTT_CLIENTID)
        descriptors.add(MQTT_CONNECTION_TIMEOUT)
        descriptors.add(MQTT_KEEP_ALIVE)
        descriptors.add(MQTT_PASSWORD)
        descriptors.add(MQTT_PERSISTENCE)
        descriptors.add(MQTT_VERSION)
        descriptors.add(MQTT_USERNAME)
        descriptors.add(MQTT_QOS)
        descriptors.add(MQTT_TOPIC)
        descriptors.add(READ_VALUE_SERIALIZER)
        descriptors.add(AVRO_READ_VALUE_SCHEMA)
        descriptors.add(WRITE_VALUE_SERIALIZER)
        descriptors.add(AVRO_WRITE_VALUE_SCHEMA)
        descriptors.add(WRITE_KEY_SERIALIZER)
        Collections.unmodifiableList(descriptors)
    }

    /**
      * create a streaming DataFrame that represents data received
      *
      * @param spark
      * @return DataFrame currently loaded
      */
    override def read(spark: SparkSession) = {
        import spark.implicits._
        implicit val recordEncoder = org.apache.spark.sql.Encoders.kryo[Record]

        getLogger.info("connecting to MQTT")
        spark.readStream
            .format("com.hurence.logisland.util.mqtt.MQTTStreamSourceProvider")
            .option("topic", topic)
            .option("persistence", persistence)
            .option("clientId", clientId)
            .option("QoS", QoS)
            .option("username", username)
            .option("password", password)
            .option("cleanSession", cleanSession)
            .option("connectionTimeout", connectionTimeout)
            .option("keepAlive", keepAlive)
            .option("mqttVersion", mqttVersion)
            .load(brokerUrl)
            .as[(String, Array[Byte], Timestamp)]
          .flatMap(r => {
              readValueSerializer match {
                  case sr: NoopSerializer => Some(new StandardRecord("kura_metric")
                    .setField(FieldDictionary.RECORD_KEY, FieldType.STRING, r._1)
                    .setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, r._2)
                    .setTime(r._3))
                  case _ => SerializingTool.deserializeRecords(readValueSerializer, r._2)
              }
          })
    }

    /**
      * create a streaming DataFrame that represents data received
      *
      * @return DataFrame currently loaded
      */
    override def write(df: Dataset[Record], controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink]): StreamingQuery = {

        implicit val recordEncoder = org.apache.spark.sql.Encoders.kryo[Record]

        df.mapPartitions(record => record.map(record => SerializingTool.serializeRecords(writeValueSerializer, writeKeySerializer, record)))
        // Create DataFrame representing the stream of input lines from connection to mqtt server
            .writeStream
            .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
            .option("topic", topic)
            .option("persistence", persistence)
            .option("clientId", clientId)
            .option("QoS", QoS)
            .option("username", username)
            .option("password", password)
            .option("cleanSession", cleanSession)
            .option("connectionTimeout", connectionTimeout)
            .option("keepAlive", keepAlive)
            .option("mqttVersion", mqttVersion)
            .start()

    }
}
object MQTTStructuredStreamProviderService {
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
