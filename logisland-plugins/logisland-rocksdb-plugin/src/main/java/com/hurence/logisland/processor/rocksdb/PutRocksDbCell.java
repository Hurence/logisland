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
package com.hurence.logisland.processor.rocksdb;


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.service.rocksdb.put.PutRecord;
import com.hurence.logisland.service.rocksdb.put.ValuePutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;



@Tags({"rocksdb"})
@CapabilityDescription("Adds the Contents of a Record to Rocksdb as the value of a single cell")
public class PutRocksDbCell extends AbstractPutRocksDb {

    private static Logger logger = LoggerFactory.getLogger(PutRocksDbCell.class);

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ROCKSDB_CLIENT_SERVICE);
        properties.add(FAMILY_NAME_DEFAULT);
        properties.add(FAMILY_NAME_FIELD);
        properties.add(KEY_DEFAULT);
        properties.add(KEY_FIELD);
        properties.add(RECORD_SERIALIZER);
        properties.add(RECORD_SCHEMA);
        return properties;
    }


    @Override
    protected PutRecord createPut(final ProcessContext context, final Record record, final RecordSerializer serializer) {

        String family = context.getPropertyValue(FAMILY_NAME_DEFAULT).asString();
        String key = context.getPropertyValue(KEY_DEFAULT).asString();

        try {
            if (record.hasField(context.getPropertyValue(FAMILY_NAME_FIELD).asString()))
                family = record.getField(context.getPropertyValue(FAMILY_NAME_FIELD).asString()).asString();

            if (record.hasField(context.getPropertyValue(KEY_FIELD).asString()))
                key = record.getField(context.getPropertyValue(KEY_FIELD).asString()).asString();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            serializer.serialize(baos, record);
            final byte[] buffer = baos.toByteArray();
            baos.close();
            final Collection<ValuePutRequest> putRequests = Collections.singletonList(new ValuePutRequest(
                    family,
                    key.getBytes(StandardCharsets.UTF_8),
                    buffer,
                    null));

            return new PutRecord(putRequests, record);

        } catch (Exception e) {
            logger.error(e.toString());
        }

        return new PutRecord(Collections.emptyList(), record);
    }


}
