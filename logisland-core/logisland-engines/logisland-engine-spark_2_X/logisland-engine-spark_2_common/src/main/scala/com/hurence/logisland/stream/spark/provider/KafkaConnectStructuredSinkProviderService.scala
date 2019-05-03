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
package com.hurence.logisland.stream.spark.provider

import com.hurence.logisland.annotation.lifecycle.OnEnabled
import com.hurence.logisland.component.InitializationException
import com.hurence.logisland.connect.Utils
import com.hurence.logisland.connect.sink.KafkaConnectStreamSink
import com.hurence.logisland.controller.ControllerServiceInitializationContext
import com.hurence.logisland.record.{FieldDictionary, Record}
import com.hurence.logisland.stream.{StreamContext, StreamProperties}
import com.hurence.logisland.util.spark.ControllerServiceLookupSink
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.DataStreamWriter

class KafkaConnectStructuredSinkProviderService extends KafkaConnectBaseProviderService {


    var maxPartitions = 1
    @transient var writer: KafkaConnectStreamSink = null

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
            } catch {
                case e: Exception =>
                    throw new InitializationException(e)
            }
        }
    }


    /**
      * create a streaming DataFrame that represents data to be written
      *
      * @param streamContext
      * @return DataFrame currently loaded
      */
    override def write(df: Dataset[Record], controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink], streamContext: StreamContext): DataStreamWriter[_] = {
        implicit val encoder = Encoders.tuple(Encoders.BINARY, Encoders.BINARY)
        val df2 = df
            .mapPartitions(record => record.map(record => (record.getField(FieldDictionary.RECORD_KEY).getRawValue().asInstanceOf[Array[Byte]],
                record.getField(FieldDictionary.RECORD_VALUE).getRawValue().asInstanceOf[Array[Byte]])))
            .toDF("key", "value")

        val topicName = streamContext.getPropertyValue(StreamProperties.WRITE_TOPICS).asString().split(",")(0).trim

        def writer() = controllerServiceLookupSink.value.getControllerService(getIdentifier).asInstanceOf[KafkaConnectStructuredSinkProviderService]
            .createWriter(SparkSession.builder().getOrCreate().sqlContext, streamContext, topicName)

        df2/*.repartition(maxPartitions, df2.col("key"))*/
            .writeStream
            .foreach(new ForeachWriter[Row] {

                override def process(value: Row): Unit = {
                    writer().enqueueOnPartition(TaskContext.getPartitionId(), value.getAs(0), value.getAs(1))
                }

                override def close(errorOrNull: Throwable): Unit = {
                    if (errorOrNull != null) {
                        logger.error("Error while storing data", errorOrNull)
                    }
                    writer().flushPartition(TaskContext.getPartitionId())
                }

                override def open(partitionId: Long, version: Long): Boolean = {
                    writer().openPartition(partitionId.intValue())
                }
            })
    }


    def createWriter(sqlContext: SQLContext, streamContext: StreamContext, topic: String): KafkaConnectStreamSink =
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
                    streamContext.getIdentifier)
                writer.start()
            }

            writer
        }


}
