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
package com.hurence.logisland.service.solr;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import org.junit.Assert;
import org.junit.Test;

public class SolrRecordConverterTest {

    @Test
    public void testBasics() throws Exception {

        Record record = new StandardRecord("factory")
                .setId("Modane")
                .setStringField("address", "rue du Frejus")
                .setField("latitude", FieldType.FLOAT, 45.4f)
                .setField("longitude", FieldType.FLOAT, 41.4f);

        //String  convertedRecord = ElasticsearchRecordConverter.convertToString(record);

        //System.out.println(convertedRecord);

        // Verify the index does not exist
        //Assert.assertEquals(true, convertedRecord.contains("location"));
    }
}
