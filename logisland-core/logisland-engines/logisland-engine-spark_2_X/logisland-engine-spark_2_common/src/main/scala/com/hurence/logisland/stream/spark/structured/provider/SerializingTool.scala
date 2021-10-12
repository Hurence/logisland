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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.hurence.logisland.record.{FieldDictionary, FieldType, Record, StandardRecord}
import com.hurence.logisland.serializer.RecordSerializer
import org.slf4j.LoggerFactory

object SerializingTool {

  private val logger = LoggerFactory.getLogger(SerializingTool.getClass)

  def deserializeRecords(serializer: RecordSerializer, bytes: Array[Byte]) = {
    try {
      val deserialized = doDeserialize(serializer, bytes)
      Some(deserialized)
    } catch {
      case t: Throwable =>
        logger.error(s"exception while deserializing events ${t.getMessage} ! This means the event will be ignored !", t)
        None
    }
  }

  def serializeRecords(valueSerializer: RecordSerializer, keySerializer: RecordSerializer, record: Record) = {
      val ret = new StandardRecord()
          .setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, doSerialize(valueSerializer, record))
      val fieldKey = record.getField(FieldDictionary.RECORD_KEY)
      if (fieldKey != null) {
        ret.setField(FieldDictionary.RECORD_KEY, FieldType.BYTES, doSerialize(keySerializer, new StandardRecord().setField(fieldKey)))
      } else {
        ret.setField(FieldDictionary.RECORD_KEY, FieldType.NULL, null)
      }
      ret
  }

  private def doDeserialize(serializer: RecordSerializer, bytes: Array[Byte]): Record = {
    val bais = new ByteArrayInputStream(bytes)
    try {
      serializer.deserialize(bais)
    } finally {
      bais.close()
    }
  }

  private def doSerialize(serializer: RecordSerializer, record: Record): Array[Byte] = {
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream
    serializer.serialize(baos, record)
    val bytes = baos.toByteArray
    baos.close()
    bytes
  }

}


