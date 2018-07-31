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
package com.hurence.logisland.stream.spark.structured.provider

import com.hurence.logisland.util.kafka.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * A Kafka Sink
  * @param kafkaSink the broadcasted sink
  * @param topics the topics to send to (comma separated)
  */
class KafkaStreamWriter(kafkaSink: Broadcast[KafkaSink], topics: String) extends Sink with Serializable {

    override def addBatch(batchId: Long, data: DataFrame): Unit = {

        data.sparkSession.createDataFrame(
            data.sparkSession.sparkContext.parallelize(data.collect()), data.schema)
            .foreach(row => kafkaSink.value.send(topics, row.getAs[Array[Byte]](0), row.getAs[Array[Byte]](1)))
    }
}

class KafkaStreamWriterProvider extends StreamSinkProvider {
    override def createSink(sqlContext: SQLContext, parameters: Map[String, String],
                            partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
        new KafkaStreamWriter(sqlContext.sparkContext.broadcast(KafkaSink(parameters)),
            parameters.getOrElse("path", throw new IllegalArgumentException("path argument must indicate the output topics")))
    }
}
