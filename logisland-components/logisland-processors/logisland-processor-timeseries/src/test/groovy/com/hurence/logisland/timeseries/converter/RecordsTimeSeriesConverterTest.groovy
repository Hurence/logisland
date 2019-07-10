/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.hurence.logisland.timeseries.converter

import com.hurence.logisland.record.FieldDictionary
import com.hurence.logisland.record.FieldType
import com.hurence.logisland.record.StandardRecord
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionConverter
import spock.lang.Specification

import java.time.Instant

/**
 * Tests the creation of a binary storage of records
 *
 * @author bailett
 */
class RecordsTimeSeriesConverterTest extends Specification {


    def "test creation of a binary storage document"() {

        given:
        def start = Instant.now().toEpochMilli()
        def start2 = Instant.now().plusSeconds(1000).toEpochMilli()
        def end = Instant.now().plusSeconds(64000).toEpochMilli()
        def name = "\\CPU\\LOAD"
        def host = "host1"
        def converter = new BinaryCompactionConverter.Builder()
            .ddcThreshold(0)
            .build()
        def records = [new StandardRecord("measure")
                               .setStringField("host", host)
                               .setStringField(FieldDictionary.RECORD_NAME, name)
                               .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, start)
                               .setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, 2.3),
                       new StandardRecord("measure")
                               .setStringField("host", host)
                               .setStringField(FieldDictionary.RECORD_NAME, name)
                               .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, start2)
                               .setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, 0.3),
                       new StandardRecord("measure")
                               .setStringField("host", host)
                               .setStringField(FieldDictionary.RECORD_NAME, name)
                               .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, end)
                               .setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, 87.2)]

        when:
        def chunkRecord = converter.chunk(records)
        def revertedRecords = converter.unchunk(chunkRecord)

        then:
        chunkRecord != null
        chunkRecord.getAllFields().size() == 8
        chunkRecord.getField(FieldDictionary.RECORD_CHUNK_START).asLong() == start
        chunkRecord.getField(FieldDictionary.RECORD_CHUNK_END).asLong() == end
        chunkRecord.getField(FieldDictionary.RECORD_NAME).asString() == name
        chunkRecord.getTimeSeries().attributes().get("host") == host

        revertedRecords.size() == 3
        revertedRecords*.every {
            it.size() == 5
            if( it.getField(FieldDictionary.RECORD_TIME).asLong() == start ){
                it.getField(FieldDictionary.RECORD_VALUE).asDouble() == 2.3
            }else if( it.getField(FieldDictionary.RECORD_TIME).asLong() == start2 ){
                it.getField(FieldDictionary.RECORD_VALUE).asDouble() == 0.3
            }else if( it.getField(FieldDictionary.RECORD_TIME).asLong() == end ){
                it.getField(FieldDictionary.RECORD_VALUE).asDouble() == 87.2
            }
        }

    }




}
