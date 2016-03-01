/*
 Copyright 2016 Hurence

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.hurence.logisland.integration

import _root_.kafka.producer.Partitioner
import _root_.kafka.utils.VerifiableProperties
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
  * Created by tom on 15/01/16.
  */
class KafkaEventPartitioner(props: VerifiableProperties) extends Partitioner with LazyLogging {


    override def partition(key: Any, numPartitions: Int): Int = {
        var partition = 0
        val stringKey = key.toString.split(",")(0)

        logger.debug(s"$stringKey -> partition $partition")
        partition
    }
}
