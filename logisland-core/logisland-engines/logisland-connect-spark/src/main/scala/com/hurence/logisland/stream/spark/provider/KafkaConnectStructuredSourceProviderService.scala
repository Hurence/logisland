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

import com.hurence.logisland.annotation.lifecycle.OnEnabled
import com.hurence.logisland.component.InitializationException
import com.hurence.logisland.controller.ControllerServiceInitializationContext
import com.hurence.logisland.record.{FieldDictionary, FieldType, Record, StandardRecord}
import com.hurence.logisland.stream.StreamContext
import com.hurence.logisland.stream.spark.StreamOptions
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}

class KafkaConnectStructuredSourceProviderService extends KafkaConnectBaseProviderService {

    var maxPartitions = 1

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
    override def read(spark: SparkSession, streamContext: StreamContext): Dataset[Record] = {
        import spark.implicits._
        implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Record]

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
            //Topic, Partition, Key, Value
            .as[(String, String, String, Array[Byte], Array[Byte])]
            .map(r =>
                new StandardRecord("kafka_connect")
                    .setField(FieldDictionary.RECORD_KEY, FieldType.BYTES, r._4)
                    .setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, r._5))
            .coalesce(maxPartitions)
    }


}
