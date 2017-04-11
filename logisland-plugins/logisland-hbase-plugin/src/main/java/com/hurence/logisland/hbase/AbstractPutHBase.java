package com.hurence.logisland.hbase;


import com.hurence.logisland.annotation.lifecycle.OnScheduled;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.hbase.put.PutRecord;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Base class for processors that put data to HBase.
 */
public abstract class AbstractPutHBase extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(AbstractPutHBase.class);

    protected static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("HBase Client Service")
            .description("Specifies the Controller Service to use for accessing HBase.")
            .required(true)
            .identifiesControllerService(HBaseClientService.class)
            .build();

    protected static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the HBase Table to put data into")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor ROW_ID = new PropertyDescriptor.Builder()
            .name("Row Identifier")
            .description("Specifies the Row ID to use when inserting data into HBase")
            .required(false) // not all sub-classes will require this
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final String STRING_ENCODING_VALUE = "String";
    static final String BYTES_ENCODING_VALUE = "Bytes";
    static final String BINARY_ENCODING_VALUE = "Binary";


    protected static final AllowableValue ROW_ID_ENCODING_STRING = new AllowableValue(STRING_ENCODING_VALUE, STRING_ENCODING_VALUE,
            "Stores the value of row id as a UTF-8 String.");

    protected static final AllowableValue ROW_ID_ENCODING_BINARY = new AllowableValue(BINARY_ENCODING_VALUE, BINARY_ENCODING_VALUE,
            "Stores the value of the rows id as a binary byte array. It expects that the row id is a binary formatted string.");

    static final PropertyDescriptor ROW_ID_ENCODING_STRATEGY = new PropertyDescriptor.Builder()
            .name("Row Identifier Encoding Strategy")
            .description("Specifies the data type of Row ID used when inserting data into HBase. The default behavior is" +
                    " to convert the row id to a UTF-8 byte array. Choosing Binary will convert a binary formatted string" +
                    " to the correct byte[] representation. The Binary option should be used if you are using Binary row" +
                    " keys in HBase")
            .required(false) // not all sub-classes will require this
            .expressionLanguageSupported(false)
            .defaultValue(ROW_ID_ENCODING_STRING.getValue())
            .allowableValues(ROW_ID_ENCODING_STRING, ROW_ID_ENCODING_BINARY)
            .build();

    protected static final PropertyDescriptor COLUMN_FAMILY = new PropertyDescriptor.Builder()
            .name("Column Family")
            .description("The Column Family to use when inserting data into HBase")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor COLUMN_QUALIFIER = new PropertyDescriptor.Builder()
            .name("Column Qualifier")
            .description("The Column Qualifier to use when inserting data into HBase")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of Records to process in a single execution. The Records will be " +
                    "grouped by table, and a single Put per table will be performed.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("25")
            .build();


    protected HBaseClientService clientService;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        clientService = context.getPropertyValue(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);
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
            final PutRecord putRecord = createPut(session, context, record);

            if (putRecord == null) {
                // sub-classes should log appropriate error messages before returning null
                session.transfer(record, REL_FAILURE);
            } else if (!putRecord.isValid()) {
                if (StringUtils.isBlank(putRecord.getTableName())) {
                    logger.error("Missing table name for Record {}; routing to failure", new Object[]{record});
                } else if (null == putRecord.getRow()) {
                    logger.error("Missing row id for Record {}; routing to failure", new Object[]{record});
                } else if (putRecord.getColumns() == null || putRecord.getColumns().isEmpty()) {
                    logger.error("No columns provided for Record {}; routing to failure", new Object[]{record});
                } else {
                    // really shouldn't get here, but just in case
                    logger.error("Failed to produce a put for Record {}; routing to failure", new Object[]{record});
                }
                session.transfer(record, REL_FAILURE);
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
                    logger.error("Failed to send {} to HBase due to {}; routing to failure", new Object[]{putRecord.getRecord(), e});
                    final Record failure = session.penalize(putRecord.getRecord());
                    session.transfer(failure, REL_FAILURE);
                }
            }
        }

        final long sendMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        logger.debug("Sent {} Records to HBase successfully in {} milliseconds", new Object[]{successes.size(), sendMillis});

        for (PutRecord putRecord : successes) {
            session.transfer(putRecord.getRecord(), REL_SUCCESS);
            final String details = "Put " + putRecord.getColumns().size() + " cells to HBase";
            session.getProvenanceReporter().send(putRecord.getRecord(), getTransitUri(putRecord), details, sendMillis);
        }

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
