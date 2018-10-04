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
package com.hurence.logisland.stream.spark

import java.util

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.engine.EngineContext
import com.hurence.logisland.stream.{AbstractRecordStream, StreamContext}
import com.hurence.logisland.util.spark.SparkUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver

class DummyRecordStream extends AbstractRecordStream with SparkRecordStream {

    @transient private var streamingContext: StreamingContext = _

    /**
      * Allows subclasses to register which property descriptor objects are
      * supported.
      *
      * @return PropertyDescriptor objects this processor currently supports
      */
    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        return new util.ArrayList[PropertyDescriptor]()
    }

    override def start(): Unit = {
        val stream = streamingContext.receiverStream(new Receiver[Long](StorageLevel.NONE) {
            override def onStart(): Unit = {}

            override def onStop(): Unit = {}
        })
        stream.foreachRDD(rdd => {
            //do nothing :)
        })
        stream.start()

    }

    /**
      * setup the stream with spark app properties
      *
      * @param appName
      * @param ssc
      * @param streamContext
      */
    override def setup(appName: String, ssc: StreamingContext, streamContext: StreamContext, engineContext: EngineContext): Unit = {
        streamingContext = ssc
        SparkUtils.customizeLogLevels
    }

    override def getStreamContext(): StreamingContext = streamingContext
}
