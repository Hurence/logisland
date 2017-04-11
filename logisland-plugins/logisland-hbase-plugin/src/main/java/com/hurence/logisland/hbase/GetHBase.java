package com.hurence.logisland.hbase;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.hurence.logisland.annotation.behavior.Stateful;
import com.hurence.logisland.annotation.behavior.WritesAttribute;
import com.hurence.logisland.annotation.behavior.WritesAttributes;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnScheduled;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.hbase.io.JsonRowSerializer;
import com.hurence.logisland.hbase.io.RowSerializer;
import com.hurence.logisland.hbase.scan.Column;
import com.hurence.logisland.hbase.scan.ResultCell;
import com.hurence.logisland.hbase.scan.ResultHandler;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Tags({"hbase", "get", "ingest"})
@CapabilityDescription("This Processor polls HBase for any records in the specified table. The processor keeps track of the timestamp of the cells that "
        + "it receives, so that as new records are pushed to HBase, they will automatically be pulled. Each record is output in JSON format, as "
        + "{\"row\": \"<row key>\", \"cells\": { \"<column 1 family>:<column 1 qualifier>\": \"<cell 1 value>\", \"<column 2 family>:<column 2 qualifier>\": \"<cell 2 value>\", ... }}. "
        + "For each record received, a Provenance RECEIVE event is emitted with the format hbase://<table name>/<row key>, where <row key> is the UTF-8 encoded value of the row's key.")
@WritesAttributes({
    @WritesAttribute(attribute = "hbase.table", description = "The name of the HBase table that the data was pulled from"),
    @WritesAttribute(attribute = "mime.type", description = "Set to application/json to indicate that output is JSON")
})
@Stateful(scopes = Scope.CLUSTER, description = "After performing a fetching from HBase, stores a timestamp of the last-modified cell that was found. In addition, it stores the ID of the row(s) "
    + "and the value of each cell that has that timestamp as its modification date. This is stored across the cluster and allows the next fetch to avoid duplicating data, even if this Processor is "
    + "run on Primary Node only and the Primary Node changes.")
public class GetHBase extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(GetHBase.class);

    static final Pattern COLUMNS_PATTERN = Pattern.compile("\\w+(:\\w+)?(?:,\\w+(:\\w+)?)*");

    static final AllowableValue NONE = new AllowableValue("None", "None");
    static final AllowableValue CURRENT_TIME = new AllowableValue("Current Time", "Current Time");

    static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("HBase Client Service")
            .description("Specifies the Controller Service to use for accessing HBase.")
            .required(true)
            .identifiesControllerService(HBaseClientService.class)
            .build();

    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies which character set is used to encode the data in HBase")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the HBase Table to put data into")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor COLUMNS = new PropertyDescriptor.Builder()
            .name("Columns")
            .description("A comma-separated list of \"<colFamily>:<colQualifier>\" pairs to return when scanning. To return all columns " +
                    "for a given family, leave off the qualifier such as \"<colFamily1>,<colFamily2>\".")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.createRegexMatchingValidator(COLUMNS_PATTERN))
            .build();

    static final PropertyDescriptor FILTER_EXPRESSION = new PropertyDescriptor.Builder()
            .name("Filter Expression")
            .description("An HBase filter expression that will be applied to the scan. This property can not be used when also using the Columns property.")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor INITIAL_TIMERANGE = new PropertyDescriptor.Builder()
            .name("Initial Time Range")
            .description("The time range to use on the first scan of a table. None will pull the entire table on the first scan, " +
                    "Current Time will pull entries from that point forward.")
            .required(true)
            .expressionLanguageSupported(false)
            .allowableValues(NONE, CURRENT_TIME)
            .defaultValue(NONE.getValue())
            .build();


    private volatile ScanResult lastResult = null;
    private volatile List<Column> columns = new ArrayList<>();
    private volatile String previousTable = null;



    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HBASE_CLIENT_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(COLUMNS);
        properties.add(FILTER_EXPRESSION);
        properties.add(INITIAL_TIMERANGE);
        properties.add(CHARSET);
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final String columns = validationContext.getPropertyValue(COLUMNS).asString();
        final String filter = validationContext.getPropertyValue(FILTER_EXPRESSION).asString();

        final List<ValidationResult> problems = new ArrayList<>();

        if (!StringUtils.isBlank(columns) && !StringUtils.isBlank(filter)) {
            problems.add(new ValidationResult.Builder()
                    .subject(FILTER_EXPRESSION.getDisplayName())
                    .input(filter).valid(false)
                    .explanation("a filter expression can not be used in conjunction with the Columns property")
                    .build());
        }

        return problems;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(TABLE_NAME)) {
            lastResult = null;
        }
    }

    @OnScheduled
    public void parseColumns(final ProcessContext context) throws IOException {


        final String columnsValue = context.getPropertyValue(COLUMNS).asString();
        final String[] columns = (columnsValue == null || columnsValue.isEmpty() ? new String[0] : columnsValue.split(","));

        this.columns.clear();
        for (final String column : columns) {
            if (column.contains(":"))  {
                final String[] parts = column.split(":");
                final byte[] cf = parts[0].getBytes(Charset.forName("UTF-8"));
                final byte[] cq = parts[1].getBytes(Charset.forName("UTF-8"));
                this.columns.add(new Column(cf, cq));
            } else {
                final byte[] cf = column.getBytes(Charset.forName("UTF-8"));
                this.columns.add(new Column(cf, null));
            }
        }
    }




    @Override
    public void onTrigger(final ProcessContext context) throws ProcessException {
        final String tableName = context.getPropertyValue(TABLE_NAME).asString();
        final String initialTimeRange = context.getPropertyValue(INITIAL_TIMERANGE).asString();
        final String filterExpression = context.getPropertyValue(FILTER_EXPRESSION).asString();
        final HBaseClientService hBaseClientService = context.getPropertyValue(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);

        // if the table was changed then remove any previous state
        if (previousTable != null && !tableName.equals(previousTable)) {
            previousTable = tableName;
        }

        try {
            final Charset charset = Charset.forName(context.getPropertyValue(CHARSET).asString());
            final RowSerializer serializer = new JsonRowSerializer(charset);

            this.lastResult = getState(context.getStateManager());
            final long defaultMinTime = (initialTimeRange.equals(NONE.getValue()) ? 0L : System.currentTimeMillis());
            final long minTime = (lastResult == null ? defaultMinTime : lastResult.getTimestamp());

            final Map<String, Set<String>> cellsMatchingTimestamp = new HashMap<>();

            final AtomicReference<Long> rowsPulledHolder = new AtomicReference<>(0L);
            final AtomicReference<Long> latestTimestampHolder = new AtomicReference<>(minTime);


            hBaseClientService.scan(tableName, columns, filterExpression, minTime, new ResultHandler() {
                @Override
                public void handle(final byte[] rowKey, final ResultCell[] resultCells) {

                    final String rowKeyString = new String(rowKey, StandardCharsets.UTF_8);

                    // check if latest cell timestamp is equal to our cutoff.
                    // if any of the cells have a timestamp later than our cutoff, then we
                    // want the row. But if the cell with the latest timestamp is equal to
                    // our cutoff, then we want to check if that's one of the cells that
                    // we have already seen.
                    long latestCellTimestamp = 0L;
                    for (final ResultCell cell : resultCells) {
                        if (cell.getTimestamp() > latestCellTimestamp) {
                            latestCellTimestamp = cell.getTimestamp();
                        }
                    }

                    // we've already seen this.
                    if (latestCellTimestamp < minTime) {
                        logger.debug("latest cell timestamp for row {} is {}, which is earlier than the minimum time of {}",
                                new Object[] {rowKeyString, latestCellTimestamp, minTime});
                        return;
                    }

                    if (latestCellTimestamp == minTime) {
                        // latest cell timestamp is equal to our minimum time. Check if all cells that have
                        // that timestamp are in our list of previously seen cells.
                        boolean allSeen = true;
                        for (final ResultCell cell : resultCells) {
                            if (cell.getTimestamp() == latestCellTimestamp) {
                                if (lastResult == null || !lastResult.contains(cell)) {
                                    allSeen = false;
                                    break;
                                }
                            }
                        }

                        if (allSeen) {
                            // we have already seen all of the cells for this row. We do not want to
                            // include this cell in our output.
                            logger.debug("all cells for row {} have already been seen", new Object[] { rowKeyString });
                            return;
                        }
                    }

                    // If the latest timestamp of the cell is later than the latest timestamp we have already seen,
                    // we want to keep track of the cells that match this timestamp so that the next time we scan,
                    // we can ignore these cells.
                    if (latestCellTimestamp >= latestTimestampHolder.get()) {
                        // new timestamp, so clear all of the 'matching cells'
                        if (latestCellTimestamp > latestTimestampHolder.get()) {
                            latestTimestampHolder.set(latestCellTimestamp);
                            cellsMatchingTimestamp.clear();
                        }

                        for (final ResultCell cell : resultCells) {
                            final long ts = cell.getTimestamp();
                            if (ts == latestCellTimestamp) {
                                final byte[] rowValue = Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength() + cell.getRowOffset());
                                final byte[] cellValue = Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength() + cell.getValueOffset());

                                final String rowHash = new String(rowValue, StandardCharsets.UTF_8);
                                Set<String> cellHashes = cellsMatchingTimestamp.get(rowHash);
                                if (cellHashes == null) {
                                    cellHashes = new HashSet<>();
                                    cellsMatchingTimestamp.put(rowHash, cellHashes);
                                }
                                cellHashes.add(new String(cellValue, StandardCharsets.UTF_8));
                            }
                        }
                    }

                    // write the row to a new FlowFile.
                    FlowFile flowFile = session.create();
                    flowFile = session.write(flowFile, new OutputStreamCallback() {
                        @Override
                        public void process(final OutputStream out) throws IOException {
                            serializer.serialize(rowKey, resultCells, out);
                        }
                    });

                    final Map<String, String> attributes = new HashMap<>();
                    attributes.put("hbase.table", tableName);
                    attributes.put("mime.type", "application/json");
                    flowFile = session.putAllAttributes(flowFile, attributes);

                    session.getProvenanceReporter().receive(flowFile, "hbase://" + tableName + "/" + rowKeyString);
                    session.transfer(flowFile, REL_SUCCESS);
                    logger.debug("Received {} from HBase with row key {}", new Object[]{flowFile, rowKeyString});

                    // we could potentially have a huge number of rows. If we get to 500, go ahead and commit the
                    // session so that we can avoid buffering tons of FlowFiles without ever sending any out.
                    long rowsPulled = rowsPulledHolder.get();
                    rowsPulledHolder.set(++rowsPulled);

                    if (++rowsPulled % getBatchSize() == 0) {
                        session.commit();
                    }
                }
            });

            final ScanResult scanResults = new ScanResult(latestTimestampHolder.get(), cellsMatchingTimestamp);

            // Commit session before we replace the lastResult; if session commit fails, we want
            // to pull these records again.
            session.commit();
            if (lastResult == null || scanResults.getTimestamp() > lastResult.getTimestamp()) {
                lastResult = scanResults;
            } else if (scanResults.getTimestamp() == lastResult.getTimestamp()) {
                final Map<String, Set<String>> combinedResults = new HashMap<>(scanResults.getMatchingCells());

                // copy the results of result.getMatchingCells() to combinedResults.
                // do a deep copy because the Set may be modified below.
                for (final Map.Entry<String, Set<String>> entry : scanResults.getMatchingCells().entrySet()) {
                    combinedResults.put(entry.getKey(), new HashSet<>(entry.getValue()));
                }

                // combined the results from 'lastResult'
                for (final Map.Entry<String, Set<String>> entry : lastResult.getMatchingCells().entrySet()) {
                    final Set<String> existing = combinedResults.get(entry.getKey());
                    if (existing == null) {
                        combinedResults.put(entry.getKey(), new HashSet<>(entry.getValue()));
                    } else {
                        existing.addAll(entry.getValue());
                    }
                }
                final ScanResult scanResult = new ScanResult(scanResults.getTimestamp(), combinedResults);
                lastResult = scanResult;
            }

        } catch (final IOException e) {
            logger.error("Failed to receive data from HBase due to {}", e);
            session.rollback();
        } finally {
            // if we failed, we want to yield so that we don't hammer hbase. If we succeed, then we have
            // pulled all of the records, so we want to wait a bit before hitting hbase again anyway.
            context.yield();
        }
    }

    // present for tests
    protected int getBatchSize() {
        return 500;
    }

    protected File getStateDir() {
        return new File("conf/state");
    }

    protected File getStateFile() {
        return new File(getStateDir(), "getHBase-" + getIdentifier());
    }

    protected String getKey() {
        return "getHBase-" + getIdentifier() + "-state";
    }

    protected List<Column> getColumns() {
        return columns;
    }


    public static class ScanResult implements Serializable {

        private static final long serialVersionUID = 1L;

        private final long latestTimestamp;
        private final Map<String, Set<String>> matchingCellHashes;

        private static final Pattern CELL_ID_PATTERN = Pattern.compile(Pattern.quote(StateKeys.ROW_ID_PREFIX) + "(\\d+)(\\.(\\d+))?");

        public static class StateKeys {
            public static final String TIMESTAMP = "timestamp";
            public static final String ROW_ID_PREFIX = "row.";
        }

        public ScanResult(final long timestamp, final Map<String, Set<String>> cellHashes) {
            latestTimestamp = timestamp;
            matchingCellHashes = cellHashes;
        }

        public long getTimestamp() {
            return latestTimestamp;
        }

        public Map<String, Set<String>> getMatchingCells() {
            return matchingCellHashes;
        }

        public boolean contains(final ResultCell cell) {
            if (cell.getTimestamp() != latestTimestamp) {
                return false;
            }

            final byte[] row = Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength() + cell.getRowOffset());
            final String rowHash = new String(row, StandardCharsets.UTF_8);
            final Set<String> cellHashes = matchingCellHashes.get(rowHash);
            if (cellHashes == null) {
                return false;
            }

            final byte[] cellValue = Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength() + cell.getValueOffset());
            final String cellHash = new String(cellValue, StandardCharsets.UTF_8);
            return cellHashes.contains(cellHash);
        }

        public Map<String, String> toFlatMap() {
            final Map<String, String> map = new HashMap<>();
            map.put(StateKeys.TIMESTAMP, String.valueOf(latestTimestamp));

            int rowCounter = 0;
            for (final Map.Entry<String, Set<String>> entry : matchingCellHashes.entrySet()) {
                final String rowId = entry.getKey();

                final String rowIdKey = StateKeys.ROW_ID_PREFIX + rowCounter;
                final String cellKeyPrefix = rowIdKey + ".";
                map.put(rowIdKey, rowId);

                final Set<String> cellValues = entry.getValue();
                int cellCounter = 0;
                for (final String cellValue : cellValues) {
                    final String cellId = cellKeyPrefix + (cellCounter++);
                    map.put(cellId, cellValue);
                }

                rowCounter++;
            }

            return map;
        }

        public static ScanResult fromFlatMap(final Map<String, String> map) {
            if (map == null) {
                return null;
            }

            final String timestampValue = map.get(StateKeys.TIMESTAMP);
            if (timestampValue == null) {
                return null;
            }

            final long timestamp = Long.parseLong(timestampValue);
            final Map<String, Set<String>> rowIndexToMatchingCellHashes = new HashMap<>();
            final Map<String, String> rowIndexToId = new HashMap<>();

            for (final Map.Entry<String, String> entry : map.entrySet()) {
                final String key = entry.getKey();
                final Matcher matcher = CELL_ID_PATTERN.matcher(key);
                if (!matcher.matches()) {
                    // if it's not a valid key, move on.
                    continue;
                }

                final String rowIndex = matcher.group(1);
                final String cellIndex = matcher.group(3);

                Set<String> cellHashes = rowIndexToMatchingCellHashes.get(rowIndex);
                if (cellHashes == null) {
                    cellHashes = new HashSet<>();
                    rowIndexToMatchingCellHashes.put(rowIndex, cellHashes);
                }

                if (cellIndex == null) {
                    // this provides a Row ID.
                    rowIndexToId.put(rowIndex, entry.getValue());
                } else {
                    cellHashes.add(entry.getValue());
                }
            }

            final Map<String, Set<String>> matchingCellHashes = new HashMap<>(rowIndexToMatchingCellHashes.size());
            for (final Map.Entry<String, Set<String>> entry : rowIndexToMatchingCellHashes.entrySet()) {
                final String rowIndex = entry.getKey();
                final String rowId = rowIndexToId.get(rowIndex);
                final Set<String> cellValues = entry.getValue();
                matchingCellHashes.put(rowId, cellValues);
            }

            return new ScanResult(timestamp, matchingCellHashes);
        }
    }

}
