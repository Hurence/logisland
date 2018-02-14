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
package com.hurence.logisland.stream.spark.structured.provider


import java.sql.Timestamp
import java.util
import java.util.Collections

import com.hurence.logisland.annotation.lifecycle.OnEnabled
import com.hurence.logisland.component.{InitializationException, PropertyDescriptor}
import com.hurence.logisland.controller.{AbstractControllerService, ControllerServiceInitializationContext}
import com.hurence.logisland.record.{FieldDictionary, FieldType, Record, StandardRecord}
import com.hurence.logisland.stream.StreamContext
import com.hurence.logisland.stream.StreamProperties._
import org.apache.spark.sql.{Dataset, SparkSession}


class MQTTStructuredStreamProviderService extends AbstractControllerService with StructuredStreamProviderService {


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

    @OnEnabled
    @throws[InitializationException]
    override def init(context: ControllerServiceInitializationContext): Unit = {
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
        implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Record]


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
            .map(r => {
                new StandardRecord("kura_metric")
                    .setTime(r._3)
                    .setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, r._2)
                    .setField(FieldDictionary.RECORD_NAME, FieldType.STRING, r._1)
            })

    }


    /**
      * create a streaming DataFrame that represents data received
      *
      * @param streamContext
      * @return DataFrame currently loaded
      */
    override def write(df: Dataset[Record], streamContext: StreamContext) = {


        // Create DataFrame representing the stream of input lines from connection to mqtt server
        val lines = df.writeStream
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
