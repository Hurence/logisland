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
package com.hurence.logisland.stream.spark.structured.provider

import java.io.{File, FileReader}
import java.util
import java.util.Collections

import com.hurence.logisland.annotation.documentation.CapabilityDescription
import com.hurence.logisland.annotation.lifecycle.OnEnabled
import com.hurence.logisland.component.{InitializationException, PropertyDescriptor}
import com.hurence.logisland.controller.{AbstractControllerService, ControllerServiceInitializationContext}
import com.hurence.logisland.logging.ComponentLog
import com.hurence.logisland.record.{FieldDictionary, FieldType, Record, StandardRecord}
import com.hurence.logisland.stream.StreamContext
import com.hurence.logisland.util.spark.ControllerServiceLookupSink
import com.hurence.logisland.validator.StandardValidators
import org.apache.commons.csv.CSVFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.Logger

import scala.collection.JavaConversions._

/**
  *
  * @author bailett
  */

@CapabilityDescription("Generates data at the specified number of rows per second, each output row contains a timestamp and value. " +
  "Where timestamp is a Timestamp type containing the time of message dispatch, and value is of Long type containing the message count, " +
  "starting from 0 as the first row. This source is intended for testing and benchmarking. Used in StructuredStream streams.")
class RateStructuredStreamProviderService extends AbstractControllerService
  with StructuredStreamProviderServiceReader {


  val LOCAL_FILE_INPUT_PATH: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("local.file.input.path")
    .description("the location of the file to be loaded")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(true)
    .build

  val HAS_CSV_HEADER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("has.csv.header")
    .description("Is this a csv file with the first line as a header")
    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
    .required(false)
    .defaultValue("true")
    .build

  val CSV_DELIMITER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("csv.delimiter")
    .description("the delimiter")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(false)
    .defaultValue(",")
    .build

  val LOCAL_FILE_OUTPUT_PATH: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("local.file.output.path")
    .description("the location of the file to be writen")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(false)
    .build


  var recordSeq:Seq[Record] = _

  @OnEnabled
  @throws[InitializationException]
  override def init(context: ControllerServiceInitializationContext): Unit = {
    super.init(context)
    this.synchronized {
      try {

        val delimiter = context.getPropertyValue(CSV_DELIMITER).asString()
        val path = context.getPropertyValue(LOCAL_FILE_INPUT_PATH).asString()
        val f = new File(path)

        if (f.exists && !f.isDirectory) {
          val in = new FileReader(path)
          val csv = CSVFormat.DEFAULT.withDelimiter(delimiter.charAt(0)).withFirstRecordAsHeader
          val records = csv.withHeader().withSkipHeaderRecord(false).parse(in)
          recordSeq = records.map(record => {
            val logislandRecord:Record = new StandardRecord()
              .setField(FieldDictionary.RECORD_VALUE, FieldType.STRING, record.toString)
            for (columnName <- record.toMap) {
              logislandRecord.setField(columnName._1, FieldType.STRING, columnName._2)
            }
            logislandRecord
          }).toSeq
        }else{
          val resourcePath = classOf[RateStructuredStreamProviderService].getResource(path).getPath
          val in = new FileReader(resourcePath)
          val csv = CSVFormat.DEFAULT.withDelimiter(delimiter.charAt(0)).withFirstRecordAsHeader
          val records = csv.withHeader().withSkipHeaderRecord(false).parse(in).getRecords

          recordSeq = records.map(record => {
            val logislandRecord:Record = new StandardRecord()
              .setField(FieldDictionary.RECORD_VALUE, FieldType.STRING, record.toString)
            for (columnName <- record.toMap) {
              logislandRecord.setField(columnName._1, FieldType.STRING, columnName._2)
            }
            logislandRecord
          }).toSeq
        }



      } catch {
        case e: Exception =>
          throw new InitializationException(e)
      }
    }
  }

  /**
    * Allows subclasses to register which property descriptor objects are
    * supported.
    *
    * @return PropertyDescriptor objects this processor currently supports
    */
  override def getSupportedPropertyDescriptors() = {
    val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]

    descriptors.add(LOCAL_FILE_INPUT_PATH)
    descriptors.add(LOCAL_FILE_OUTPUT_PATH)
    descriptors.add(HAS_CSV_HEADER)
    descriptors.add(CSV_DELIMITER)
    Collections.unmodifiableList(descriptors)
  }

  /**
    * create a streaming DataFrame that represents data received
    *
    * @param spark
    * @return DataFrame currently loaded
    */
  override def read(spark: SparkSession) = {
    import spark.implicits._
    implicit val recordEncoder = org.apache.spark.sql.Encoders.kryo[Record]

   // val headers = records.iterator.next.toMap.keySet



    recordSeq.toDS()
  }

}
