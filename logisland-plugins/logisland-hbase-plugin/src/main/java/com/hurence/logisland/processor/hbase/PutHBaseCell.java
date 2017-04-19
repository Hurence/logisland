package com.hurence.logisland.processor.hbase;


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.hbase.put.PutColumn;
import com.hurence.logisland.processor.hbase.put.PutRecord;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.RecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


@Tags({"hadoop", "hbase"})
@CapabilityDescription("Adds the Contents of a Record to HBase as the value of a single cell")
public class PutHBaseCell extends AbstractPutHBase {

    private static Logger logger = LoggerFactory.getLogger(PutHBaseCell.class);

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HBASE_CLIENT_SERVICE);
        properties.add(HBASE_CLIENT_SERVICE_ID);
        properties.add(TABLE_NAME_FIELD);
        properties.add(ROW_ID_FIELD);
        properties.add(ROW_ID_ENCODING_STRATEGY);
        properties.add(COLUMN_FAMILY_FIELD);
        properties.add(COLUMN_QUALIFIER_FIELD);
        properties.add(BATCH_SIZE);
        properties.add(RECORD_SCHEMA);
        properties.add(RECORD_SERIALIZER);
        return properties;
    }


    @Override
    protected PutRecord createPut(final ProcessContext context, final Record record, final RecordSerializer serializer) {
        final String tableName = record.getField(context.getPropertyValue(TABLE_NAME_FIELD).asString()).asString();
        final String row = record.getField(context.getPropertyValue(ROW_ID_FIELD).asString()).asString();
        final String columnFamily = record.getField(context.getPropertyValue(COLUMN_FAMILY_FIELD).asString()).asString();
        final String columnQualifier = record.getField(context.getPropertyValue(COLUMN_QUALIFIER_FIELD).asString()).asString();


        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            serializer.serialize(baos, record);
            final byte[] buffer = baos.toByteArray();
            baos.close();
            final Collection<PutColumn> columns = Collections.singletonList(new PutColumn(
                    columnFamily.getBytes(StandardCharsets.UTF_8),
                    columnQualifier.getBytes(StandardCharsets.UTF_8),
                    buffer));
            byte[] rowKeyBytes = getRow(row, context.getPropertyValue(ROW_ID_ENCODING_STRATEGY).asString());

            return new PutRecord(tableName, rowKeyBytes, columns, record);

        } catch (IOException e) {
            logger.error(e.toString());
        }


        return new PutRecord(tableName, null, Collections.emptyList(), record);
    }


}
