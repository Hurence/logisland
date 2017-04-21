package com.hurence.logisland.processor.hbase;


import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.hbase.put.PutRecord;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.StandardComponentLogger;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.*;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Base class for processors that put data to HBase.
 */
public abstract class AbstractPutHBase extends AbstractProcessor {

    private ComponentLog logger = new StandardComponentLogger(this.getIdentifier(), AbstractPutHBase.class);




    public static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("hbase.client.service")
            .description("The instance of the Controller Service to use for accessing HBase.")
            .required(true)
            .identifiesControllerService(HBaseClientService.class)
            .build();


    public static final PropertyDescriptor TABLE_NAME_DEFAULT = new PropertyDescriptor.Builder()
            .name("table.name.default")
            .description("The table table to use if table name field is not set")
            .required(false)
            .build();

    public static final PropertyDescriptor TABLE_NAME_FIELD = new PropertyDescriptor.Builder()
            .name("table.name.field")
            .description("The field containing the name of the HBase Table to put data into")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ROW_ID_FIELD = new PropertyDescriptor.Builder()
            .name("row.identifier.field")
            .description("Specifies  field containing the Row ID to use when inserting data into HBase")
            .required(false) // not all sub-classes will require this
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final String STRING_ENCODING_VALUE = "String";
    static final String BYTES_ENCODING_VALUE = "Bytes";
    static final String BINARY_ENCODING_VALUE = "Binary";


    public static final AllowableValue ROW_ID_ENCODING_STRING = new AllowableValue(STRING_ENCODING_VALUE, STRING_ENCODING_VALUE,
            "Stores the value of row id as a UTF-8 String.");

    public static final AllowableValue ROW_ID_ENCODING_BINARY = new AllowableValue(BINARY_ENCODING_VALUE, BINARY_ENCODING_VALUE,
            "Stores the value of the rows id as a binary byte array. It expects that the row id is a binary formatted string.");

    static final PropertyDescriptor ROW_ID_ENCODING_STRATEGY = new PropertyDescriptor.Builder()
            .name("row.identifier.encoding.strategy")
            .description("Specifies the data type of Row ID used when inserting data into HBase. The default behavior is" +
                    " to convert the row id to a UTF-8 byte array. Choosing Binary will convert a binary formatted string" +
                    " to the correct byte[] representation. The Binary option should be used if you are using Binary row" +
                    " keys in HBase")
            .required(false) // not all sub-classes will require this
            .expressionLanguageSupported(false)
            .defaultValue(ROW_ID_ENCODING_STRING.getValue())
            .allowableValues(ROW_ID_ENCODING_STRING, ROW_ID_ENCODING_BINARY)
            .build();

    public static final PropertyDescriptor COLUMN_FAMILY_DEFAULT = new PropertyDescriptor.Builder()
            .name("column.family.default")
            .description("The column family to use if column family field is not set")
            .required(false)
            .build();

    public static final PropertyDescriptor COLUMN_FAMILY_FIELD = new PropertyDescriptor.Builder()
            .name("column.family.field")
            .description("The field containing the  Column Family to use when inserting data into HBase")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor COLUMN_QUALIFIER_DEFAULT = new PropertyDescriptor.Builder()
            .name("column.qualifier.default")
            .description("The column qualifier to use if column qualifier field is not set")
            .required(false)
            .build();

    public static final PropertyDescriptor COLUMN_QUALIFIER_FIELD = new PropertyDescriptor.Builder()
            .name("column.qualifier.field")
            .description("The field containing the  Column Qualifier to use when inserting data into HBase")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch.size")
            .description("The maximum number of Records to process in a single execution. The Records will be " +
                    "grouped by table, and a single Put per table will be performed.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("25")
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


    protected HBaseClientService clientService;
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
        clientService = context.getPropertyValue(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);
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

        final int batchSize = context.getPropertyValue(BATCH_SIZE).asInteger();

        if (records == null || records.size() == 0) {
            return Collections.emptyList();
        }

        final Map<String, List<PutRecord>> tablePuts = new HashMap<>();

        // Group Records by HBase Table
        for (final Record record : records) {
            final PutRecord putRecord = createPut(context, record, serializer);

            if (putRecord == null) {
                // sub-classes should log appropriate error messages before returning null
                record.addError(ProcessError.RECORD_CONVERSION_ERROR.toString(),
                        logger,
                        "Failed to produce a put for Record from {}" + record.toString());
            } else if (!putRecord.isValid()) {
                if (StringUtils.isBlank(putRecord.getTableName())) {
                    record.addError(ProcessError.BAD_RECORD.toString(),
                            logger,
                            "Missing table name for Record " + record.toString()                            );
                } else if (null == putRecord.getRow()) {
                    record.addError(ProcessError.BAD_RECORD.toString(),
                            logger,
                            "Missing row id for Record " + record.toString());
                } else if (putRecord.getColumns() == null || putRecord.getColumns().isEmpty()) {
                    record.addError(ProcessError.BAD_RECORD.toString(),
                            logger,
                            "No columns provided for Record " + record.toString());
                } else {
                    // really shouldn't get here, but just in case
                    record.addError(ProcessError.RECORD_CONVERSION_ERROR.toString(),
                            logger,
                            "Failed to produce a put for Record from " + record.toString()
                            );
                }
            } else {
                List<PutRecord> putRecords = tablePuts.get(putRecord.getTableName());
                if (putRecords == null) {
                    putRecords = new ArrayList<>();
                    tablePuts.put(putRecord.getTableName(), putRecords);
                }
                putRecords.add(putRecord);
            }
        }

        logger.debug("Sending {} Records to HBase in {} put operations", new Object[]{records.size(), tablePuts.size()});

        final long start = System.nanoTime();
        final List<PutRecord> successes = new ArrayList<>();

        for (Map.Entry<String, List<PutRecord>> entry : tablePuts.entrySet()) {
            try {
                clientService.put(entry.getKey(), entry.getValue());
                successes.addAll(entry.getValue());
            } catch (Exception e) {
                logger.error(e.getMessage(), e);

                for (PutRecord putRecord : entry.getValue()) {
                    String msg = String.format("Failed to send {} to HBase due to {}; routing to failure", putRecord.getRecord(), e);
                    putRecord.getRecord().addError("HBASE_PUT_RECORD_FAILURE", logger, msg);
                }
            }
        }

        final long sendMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        logger.debug("Sent {} Records to HBase successfully in {} milliseconds", new Object[]{successes.size(), sendMillis});

        for (PutRecord putRecord : successes) {
            final String details = "Put " + putRecord.getColumns().size() + " cells to HBase";
            //session.getProvenanceReporter().send(putRecord.getRecord(), getTransitUri(putRecord), details, sendMillis);
        }
        return records;

    }

    protected String getTransitUri(PutRecord putRecord) {
        return "hbase://" + putRecord.getTableName() + "/" + new String(putRecord.getRow(), StandardCharsets.UTF_8);
    }

    protected byte[] getRow(final String row, final String encoding) {
        //check to see if we need to modify the rowKey before we pass it down to the PutRecord
        byte[] rowKeyBytes = null;
        if (BINARY_ENCODING_VALUE.contentEquals(encoding)) {
            rowKeyBytes = clientService.toBytesBinary(row);
        } else {
            rowKeyBytes = row.getBytes(StandardCharsets.UTF_8);
        }
        return rowKeyBytes;
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
