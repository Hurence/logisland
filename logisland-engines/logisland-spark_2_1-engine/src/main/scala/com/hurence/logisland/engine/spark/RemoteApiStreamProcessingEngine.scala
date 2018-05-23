/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.engine.spark

import java.util
import java.util.Collections

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.engine.{AbstractProcessingEngine, EngineContext}
import com.hurence.logisland.stream.spark.SparkRecordStream
import com.hurence.logisland.util.spark.SparkUtils
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._


class RemoteApiStreamProcessingEngine extends BaseStreamProcessingEngine {

    private val logger = LoggerFactory.getLogger(classOf[RemoteApiStreamProcessingEngine])

    override protected  def customizeSparkConfiguration(sparkConf: SparkConf, engineContext: EngineContext): Unit = {

    }

    /**
      * Override to setup streaming context before starting them.
      *
      * @param engineContext the engine context.
      * @param scc           the spark streaming context.
      */
    override protected def setupStreamingContexts(engineContext: EngineContext, scc: StreamingContext): Unit = {
        
    }

}


