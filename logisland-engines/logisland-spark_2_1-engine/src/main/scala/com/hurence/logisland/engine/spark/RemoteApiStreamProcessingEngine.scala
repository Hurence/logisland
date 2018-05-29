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

import com.hurence.logisland.engine.{EngineContext, ProcessingEngine}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext

class RemoteApiStreamProcessingEngine(processingEngine: ProcessingEngine) extends BaseStreamProcessingEngine {




    /**
      * Hook to customize spark configuration before creating a spark context.
      *
      * @param sparkConf     the preinitialized configuration.
      * @param engineContext the engine context.
      */
    override protected def customizeSparkConfiguration(sparkConf: SparkConf, engineContext: EngineContext): Unit = ???

    /**
      * Override to setup streaming context before starting them.
      *
      * @param engineContext the engine context.
      * @param scc           the spark streaming context.
      */
    override protected def setupStreamingContexts(engineContext: EngineContext, scc: StreamingContext): Unit = ???
}
