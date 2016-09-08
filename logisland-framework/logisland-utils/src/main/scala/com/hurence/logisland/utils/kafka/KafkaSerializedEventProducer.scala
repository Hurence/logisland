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

package com.hurence.logisland.utils.kafka

import java.io.ByteArrayOutputStream
import java.util.Properties

import _root_.kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import com.hurence.logisland.event.Event
import com.hurence.logisland.serializer.EventSerializer
import com.typesafe.scalalogging.slf4j.LazyLogging


/**
  * Created by tom on 13/01/16.
  */
class KafkaSerializedEventProducer(brokerList: String, topic: String, serializer: EventSerializer) extends LazyLogging with Serializable {

    // Zookeper connection properties
    val props = new Properties()
    props.setProperty("metadata.broker.list", brokerList)
    props.setProperty("key.serializer.class", "kafka.serializer.DefaultEncoder")
    props.setProperty("serializer.class", "kafka.serializer.DefaultEncoder")
    //  props.setProperty("partitioner.class", "example.producer.SimplePartitioner")
    props.setProperty("request.required.acks", "1")
    props.setProperty("producer.type", "async")
    props.setProperty("batch.num.messages", "500")
    props.setProperty("compression.codec", "1")
    //props.setProperty("message.max.bytes", "2000024")

    val config = new ProducerConfig(props)

    val producer = new Producer[Array[Byte], Array[Byte]](config)

    /**
      * Send events to Kafka topics
      *
      * @param events
      */
    def produce(events: List[Event]) = {

        val messages = events.map(event => {
            // messages are serialized with kryo first
            val baos: ByteArrayOutputStream = new ByteArrayOutputStream
            serializer.serialize(baos, event)

            // and then converted to KeyedMessage
            val message = new KeyedMessage[Array[Byte], Array[Byte]](topic, "key-event".getBytes(), baos.toByteArray)
            baos.close()

            message
        }).toArray

        producer.send(messages: _*)
        producer.close()
        if (messages.length != 0) {
            logger.debug(s"sent ${messages.size} serialized events on topic $topic")

        }
    }


}
