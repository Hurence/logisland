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
package com.hurence.logisland.util.spark

import java.io.ByteArrayOutputStream

import com.hurence.logisland.record.{FieldDictionary, Record}
import com.hurence.logisland.serializer.RecordSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConversions._

class KafkaSink(createProducer: () => KafkaProducer[Array[Byte], Array[Byte]]) extends Serializable {

    lazy val producer = createProducer()


    def send(topic: String, key: Array[Byte], value: Array[Byte]): Unit =
        producer.send(new ProducerRecord(topic, value))

    /**
      * Send events to Kafka topics
      *
      * @param events
      */
    def produce(topic: String, events: List[Record], serializer:RecordSerializer) = {

        val messages = events.map(event => {
            // messages are serialized with kryo first
            val baos: ByteArrayOutputStream = new ByteArrayOutputStream
            serializer.serialize(baos, event)

            // and then converted to KeyedMessage
            val key = if( event.hasField(FieldDictionary.RECORD_ID))
                event.getField(FieldDictionary.RECORD_ID).asString()
            else
                ""
            val message = new ProducerRecord(topic, key.getBytes(), baos.toByteArray)
            baos.close()


            producer.send(message)
        })
    }
}

object KafkaSink {
    def apply(config: Map[String, Object]): KafkaSink = {
        val f = () => {
            val producer = new KafkaProducer[Array[Byte], Array[Byte]](config)

        /*    sys.addShutdownHook {
                producer.close()
            }
*/
            producer
        }
        new KafkaSink(f)
    }
}