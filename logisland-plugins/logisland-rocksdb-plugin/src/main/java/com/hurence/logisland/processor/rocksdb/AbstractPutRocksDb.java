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
import com.hurence.logisland.service.rocksdb.RocksdbClientService;
import com.hurence.logisland.service.rocksdb.put.PutRecord;
import com.hurence.logisland.service.rocksdb.put.ValuePutRequest;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Base class for processors that put data to HBase.
 */
public abstract class AbstractPutRocksDb extends AbstractProcessor {

    private ComponentLog logger = new StandardComponentLogger(this.getIdentifier(), AbstractPutRocksDb.class);

    public static final PropertyDescriptor ROCKSDB_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("rocksdb.client.service")
            .description("The instance of the Controller Service to use for accessing Rocksdb.")
            .required(true)
            .identifiesControllerService(RocksdbClientService.class)
            .build();


    public static final PropertyDescriptor FAMILY_NAME_DEFAULT = new PropertyDescriptor.Builder()
            .name("family.name.default")
            .description("The family to use if family name field is not set")
            .required(false)
            .build();

    public static final PropertyDescriptor FAMILY_NAME_FIELD = new PropertyDescriptor.Builder()
            .name("family.name.field")
            .description("The field containing the name of the Rocksdb family to put data into")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_DEFAULT = new PropertyDescriptor.Builder()
            .name("key.default")
            .description("The key to use if key field is not set")
            .required(false)
            .build();

    public static final PropertyDescriptor KEY_FIELD = new PropertyDescriptor.Builder()
            .name("key.field")
            .description("The field containing the key to use when inserting data into Rocksdb")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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


    protected RocksdbClientService clientService;
    protected RecordSerializer serializer;


    @Override
    public boolean hasControllerService() {
        return true;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final boolean isAvroSerializer = validationContext.getPropertyValue(RECORD_SERIALIZER).asString().toLowerCase().contains("avro");
        final boolean isAvroSchemaSet = validationContext.getPropertyValue(RECORD_SCHEMA).isSet();

        final List<ValidationResult> problems = new ArrayList<>();

        if (isAvroSerializer && !isAvroSchemaSet) {
            problems.add(new ValidationResult.Builder()
                    .subject(RECORD_SERIALIZER.getDisplayName())
                    .valid(false)
                    .explanation("an avro schema must be provided with an avro serializer")
                    .build());
        }

        return problems;
    }

    @Override
    public void init(final ProcessContext context) {
        clientService = context.getPropertyValue(ROCKSDB_CLIENT_SERVICE).asControllerService(RocksdbClientService.class);
        if(clientService == null)
            logger.error("Rocksdb client service is not initialized!");

        if (context.getPropertyValue(RECORD_SCHEMA).isSet()) {
            serializer = SerializerProvider.getSerializer(
                    context.getPropertyValue(RECORD_SERIALIZER).asString(),
                    context.getPropertyValue(RECORD_SCHEMA).asString());
        } else {
            serializer = SerializerProvider.getSerializer(context.getPropertyValue(RECORD_SERIALIZER).asString(), null);
        }
    }

    @Override
    public Collection<Record> process(final ProcessContext context, final Collection<Record> records) throws ProcessException {

        if (records == null || records.size() == 0) {
            return Collections.emptyList();
        }

        final List<PutRecord> putRecords = new ArrayList<>();

        // Group Records by HBase Table
        for (final Record record : records) {
            final PutRecord putRecord = createPut(context, record, serializer);

            if (putRecord == null) {
                // sub-classes should log appropriate error messages before returning null
                record.addError(ProcessError.RECORD_CONVERSION_ERROR.toString(),
                        logger,
                        "Failed to produce a put for Record from " + record.toString());
            } else if (!putRecord.isValid()) {//TODO
                if (putRecord.getPutRequests() == null || putRecord.getPutRequests().isEmpty()) {
                    record.addError(ProcessError.BAD_RECORD.toString(),
                            logger,
                            "No key value pair (putRequest) provided for Record " + record.toString());
                } else {
                    // really shouldn't get here, but just in case
                    record.addError(ProcessError.RECORD_CONVERSION_ERROR.toString(),
                            logger,
                            "Failed to produce a put for Record from " + record.toString()
                            );
                }
            } else {
                putRecords.add(putRecord);
            }
        }

        logger.debug("Sending {} Records to Rocksdb in {} put operations", new Object[]{records.size(), putRecords.size()});

        final long start = System.nanoTime();

        final List<ValuePutRequest> puts = new ArrayList<>();
        for (PutRecord putRecord: putRecords){
            puts.addAll(putRecord.getPutRequests());
        }
        try {
            clientService.multiPut(puts);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            for (PutRecord putRecord : putRecords) {
                String msg = String.format("Failed to send {} to HBase due to {}; routing to failure", putRecord.getRecord(), e);
                putRecord.getRecord().addError("HBASE_PUT_RECORD_FAILURE", logger, msg);
            }
        }

        final long sendMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        logger.debug("Sent {} Records to Rocksdb successfully in {} milliseconds", new Object[]{puts.size(), sendMillis});

        return records;

    }

    /**
     * Sub-classes provide the implementation to create a put from a Record.
     *
     * @param context the current context
     * @param record  the Record to create a Put from
     * @return a PutRecord instance for the given Record
     */
    protected abstract PutRecord createPut(final ProcessContext context, final Record record, final RecordSerializer serializer);

}
