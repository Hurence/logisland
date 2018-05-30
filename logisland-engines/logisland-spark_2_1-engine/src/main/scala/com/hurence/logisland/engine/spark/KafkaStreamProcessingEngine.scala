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

package com.hurence.logisland.engine.spark


import java.util
import java.util.Collections

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.engine.EngineContext
import com.hurence.logisland.stream.spark.SparkRecordStream
import com.hurence.logisland.validator.StandardValidators
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object KafkaStreamProcessingEngine {

    val SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION = new PropertyDescriptor.Builder()
        .name("spark.streaming.kafka.maxRatePerPartition")
        .description("Maximum rate (number of records per second) at which data will be read from each Kafka partition")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("5000")
        .build

    val SPARK_STREAMING_KAFKA_MAXRETRIES = new PropertyDescriptor.Builder()
        .name("spark.streaming.kafka.maxRetries")
        .description("Maximum rate (number of records per second) at which data will be read from each Kafka partition")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("3")
        .build
}


class KafkaStreamProcessingEngine extends BaseStreamProcessingEngine {

    private val logger = LoggerFactory.getLogger(classOf[KafkaStreamProcessingEngine])



    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
        descriptors.addAll(super.getSupportedPropertyDescriptors)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAXRETRIES)
        Collections.unmodifiableList(descriptors)
    }


    override protected def customizeSparkConfiguration(sparkConf: SparkConf, engineContext: EngineContext): Unit = {
        setConfProperty(sparkConf, engineContext, KafkaStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAXRETRIES)
        setConfProperty(sparkConf, engineContext, KafkaStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION)

    }

    override protected def setupStreamingContexts(engineContext: EngineContext, ssc: StreamingContext): Unit = {
        val appName = engineContext.getPropertyValue(BaseStreamProcessingEngine.SPARK_APP_NAME).asString
        engineContext.getStreamContexts.asScala.foreach(streamContext => {
            try {
                val kafkaStream = streamContext.getStream.asInstanceOf[SparkRecordStream]
                kafkaStream.setup(appName, ssc, streamContext, engineContext)
                kafkaStream.start()
            } catch {
                case ex: Exception =>
                    logger.error("something bad happened, please check Kafka or cluster health : {}", ex.getMessage)
            }
        })
    }
}


