package com.hurence.logisland.stream.spark.structured.handler

import java.util

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.stream.StreamContext
import com.hurence.logisland.util.spark.ControllerServiceLookupSink
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.OffsetRange

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
trait StructuredStreamHandler {

    /**
      * to be overriden by subclasses
      *
      * @param rdd
      */
    def process(streamContext: StreamContext,
                controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink],
                rdd: RDD[ConsumerRecord[Array[Byte], Array[Byte]]]): Option[Array[OffsetRange]]

    /**
      * Allows subclasses to register which property descriptor objects are
      * supported.
      *
      * @return PropertyDescriptor objects this processor currently supports
      */
    def getSupportedPropertyDescriptors: util.List[PropertyDescriptor]
}
