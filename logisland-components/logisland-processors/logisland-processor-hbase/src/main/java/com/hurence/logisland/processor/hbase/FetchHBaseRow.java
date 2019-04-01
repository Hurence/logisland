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
package com.hurence.logisland.processor.hbase;

import com.hurence.logisland.annotation.behavior.WritesAttribute;
import com.hurence.logisland.annotation.behavior.WritesAttributes;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.StandardComponentLogger;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.*;
import com.hurence.logisland.service.hbase.HBaseClientService;
import com.hurence.logisland.service.hbase.scan.Column;
import com.hurence.logisland.service.hbase.scan.ResultCell;
import com.hurence.logisland.service.hbase.scan.ResultHandler;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

@Tags({"hbase", "scan", "fetch", "get", "enrich"})
@CapabilityDescription("Fetches a row from an HBase table. The Destination property controls whether the cells are added as flow file attributes, " +
        "or the row is written to the flow file content as JSON. This processor may be used to fetch a fixed row on a interval by specifying the " +
        "table and row id directly in the processor, or it may be used to dynamically fetch rows by referencing the table and row id from " +
        "incoming flow files.")
@WritesAttributes({
        @WritesAttribute(attribute = "hbase.table", description = "The name of the HBase table that the row was fetched from"),
        @WritesAttribute(attribute = "hbase.row", description = "A JSON document representing the row. This property is only written when a Destination of flowfile-attributes is selected."),
        @WritesAttribute(attribute = "mime.type", description = "Set to application/json when using a Destination of flowfile-content, not set or modified otherwise")
})
public class FetchHBaseRow extends AbstractProcessor {

    static final Pattern COLUMNS_PATTERN = Pattern.compile("\\w+(:\\w+)?(?:,\\w+(:\\w+)?)*");

    public static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("hbase.client.service")
            .description("The instance of the Controller Service to use for accessing HBase.")
            .required(true)
            .identifiesControllerService(HBaseClientService.class)
            .build();


    public static final PropertyDescriptor TABLE_NAME_DEFAULT = new PropertyDescriptor.Builder()
            .name("table.name.default")
            .description("The table to use if table name field is not set")
            .required(false)
            .build();

    public static final PropertyDescriptor TABLE_NAME_FIELD = new PropertyDescriptor.Builder()
            .name("table.name.field")
            .description("The field containing the name of the HBase Table to fetch from.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ROW_ID_FIELD = new PropertyDescriptor.Builder()
            .name("row.identifier.field")
            .description("The field containing the identifier of the row to fetch.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor COLUMNS_FIELD = new PropertyDescriptor.Builder()
            .name("columns.field")
            .description("The field containing an optional comma-separated list of \"\"<colFamily>:<colQualifier>\"\" pairs to fetch. To return all columns " +
                    "for a given family, leave off the qualifier such as \"\"<colFamily1>,<colFamily2>\"\".")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(COLUMNS_PATTERN))
            .build();


    public static final AllowableValue AVRO_SERIALIZER =
            new AllowableValue(AvroSerializer.class.getName(), "avro serialization", "serialize events as avro blocs");

    public static final AllowableValue JSON_SERIALIZER =
            new AllowableValue(JsonSerializer.class.getName(), "json serialization", "serialize events as json blocs");

    public static final AllowableValue KRYO_SERIALIZER =
            new AllowableValue(KryoSerializer.class.getName(), "kryo serialization", "serialize events as json blocs");

    public static final AllowableValue NO_SERIALIZER =
            new AllowableValue("none", "no serialization", "send events as bytes");


    public static final PropertyDescriptor RECORD_SERIALIZER = new PropertyDescriptor.Builder()
            .name("record.serializer")
            .description("the serializer needed to i/o the record in the HBase row")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, AVRO_SERIALIZER, NO_SERIALIZER)
            .defaultValue(KRYO_SERIALIZER.getValue())
            .build();

    public static final PropertyDescriptor RECORD_SCHEMA = new PropertyDescriptor.Builder()
            .name("record.schema")
            .description("the avro schema definition for the Avro serialization")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public HBaseClientService clientService;
    public RecordSerializer serializer;


    static final String HBASE_TABLE_ATTR = "hbase.table";
    static final String HBASE_ROW_ATTR = "hbase.row";

    static final List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HBASE_CLIENT_SERVICE);
        props.add(TABLE_NAME_FIELD);
        props.add(ROW_ID_FIELD);
        props.add(COLUMNS_FIELD);
        props.add(RECORD_SERIALIZER);
        props.add(RECORD_SCHEMA);
        props.add(TABLE_NAME_DEFAULT);
        properties = Collections.unmodifiableList(props);
    }


    @Override
    public boolean hasControllerService() {
        return true;
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }


    @Override
    public void init(ProcessContext context) {
        super.init(context);
        this.clientService = PluginProxy.rewrap(context.getPropertyValue(HBASE_CLIENT_SERVICE).asControllerService());
        if (context.getPropertyValue(RECORD_SCHEMA).isSet()) {
            serializer = SerializerProvider.getSerializer(
                    context.getPropertyValue(RECORD_SERIALIZER).asString(),
                    context.getPropertyValue(RECORD_SCHEMA).asString());
        } else {
            serializer = SerializerProvider.getSerializer(context.getPropertyValue(RECORD_SERIALIZER).asString(), null);
        }


    }


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) throws ProcessException {


        List<Record> outputRecords = new ArrayList<>();
        for (Record record : records) {

            try {
                String tableName = context.getPropertyValue(TABLE_NAME_DEFAULT).asString();
                if (record.hasField(context.getPropertyValue(TABLE_NAME_FIELD).asString()))
                    tableName = record.getField(context.getPropertyValue(TABLE_NAME_FIELD).asString()).asString();

                if (StringUtils.isBlank(tableName)) {
                    record.addError(
                            ProcessError.BAD_RECORD.toString(),
                            getLogger(),
                            "Table Name is blank or null for {}",
                            new Object[]{record});

                    continue;
                }

                final String rowId = record.getField(context.getPropertyValue(ROW_ID_FIELD).asString()).asString();
                if (StringUtils.isBlank(rowId)) {
                    record.addError(
                            ProcessError.BAD_RECORD.toString(),
                            getLogger(),
                            "Row Identifier is blank or null for {}",
                            new Object[]{record});
                    continue;
                }

                List<Column> columns = null;
                if (record.hasField(context.getPropertyValue(COLUMNS_FIELD).asString()))
                    columns = getColumns(record.getField(context.getPropertyValue(COLUMNS_FIELD).asString()).asString());


                final RecordContentHandler handler = new RecordContentHandler(serializer);

                final byte[] rowIdBytes = rowId.getBytes(StandardCharsets.UTF_8);

                try {
                    clientService.scan(tableName, rowIdBytes, rowIdBytes, columns, handler);
                } catch (Exception e) {
                    record.addError(
                            ProcessError.BAD_RECORD.toString(),
                            getLogger(),
                            "Unable to fetch row {} from  {} due to {}",
                            new Object[]{rowId, tableName, e});
                    continue;
                }

                Collection<Record> handlerRecords = handler.getRecords();
                if (!handler.handledRow()) {
                    record.addError(
                            ProcessError.BAD_RECORD.toString(),
                            getLogger(),
                            "Row {} not found in {}",
                            new Object[]{rowId, tableName});
                    continue;
                }

                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("Fetched {} from {} with row id {}", new Object[]{handlerRecords, tableName, rowId});
                }

                final Map<String, String> attributes = new HashMap<>();
                attributes.put(HBASE_TABLE_ATTR, tableName);


                outputRecords.addAll(handlerRecords);


            } catch (Exception ex) {
                record.addError(ProcessError.RUNTIME_ERROR.toString(),
                        getLogger(),
                        "Unable to fetch row {}",
                        new Object[]{ex});
            }

        }

        return outputRecords;

    }

    /**
     * @param columnsValue a String in the form colFam:colQual,colFam:colQual
     * @return a list of Columns based on parsing the given String
     */
    private List<Column> getColumns(final String columnsValue) {
        final String[] columns = (columnsValue == null || columnsValue.isEmpty() ? new String[0] : columnsValue.split(","));

        List<Column> columnsList = new ArrayList<>(columns.length);

        for (final String column : columns) {
            if (column.contains(":")) {
                final String[] parts = column.split(":");
                final byte[] cf = parts[0].getBytes(StandardCharsets.UTF_8);
                final byte[] cq = parts[1].getBytes(StandardCharsets.UTF_8);
                columnsList.add(new Column(cf, cq));
            } else {
                final byte[] cf = column.getBytes(StandardCharsets.UTF_8);
                columnsList.add(new Column(cf, null));
            }
        }

        return columnsList;
    }


    /**
     * A FetchHBaseRowHandler that writes the resulting row to the Record content.
     */
    private static class RecordContentHandler implements ResultHandler {

        private ArrayList<Record> records;
        private final RecordSerializer serializer;
        private boolean handledRow = false;

        private ComponentLog logger = new StandardComponentLogger("", this.getClass());

        public RecordContentHandler(final RecordSerializer serializer) {
            this.records = new ArrayList<>();
            this.serializer = serializer;
        }

        @Override
        public void handle(byte[] row, ResultCell[] resultCells) {

            for (ResultCell cell : resultCells) {

                try {
                    final byte[] cellValue = Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength() + cell.getValueOffset());
                    ByteArrayInputStream bais = new ByteArrayInputStream(cellValue);
                    Record deserializedRecord = serializer.deserialize(bais);
                    records.add(deserializedRecord);
                    bais.close();
                } catch (Exception e) {
                    logger.error("error while handling ResultCell for {} : {}", new Object[]{cell, e});
                }
            }
            handledRow = true;
        }


        public Collection<Record> getRecords() {
            return records;
        }

        public boolean handledRow() {
            return handledRow;
        }
    }


}
