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
package com.hurence.logisland.service.cache;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Category;
import com.hurence.logisland.annotation.documentation.ComponentCategory;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.datastore.DatastoreClientService;
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import com.hurence.logisland.service.datastore.model.MultiGetQueryRecord;
import com.hurence.logisland.service.datastore.model.MultiGetResponseRecord;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Category(ComponentCategory.DATASTORE)
@Tags({"csv", "service", "cache"})
@CapabilityDescription("A cache that store csv lines as records loaded from a file")
public class CSVKeyValueCacheService extends LRUKeyValueCacheService<String, Record> implements DatastoreClientService {


    public static final PropertyDescriptor CSV_HEADER = new PropertyDescriptor.Builder()
            .name("csv.header")
            .displayName("csv headers")
            .description("comma separated header values")
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();
    public static final PropertyDescriptor FIRST_LINE_HEADER = new PropertyDescriptor.Builder()
            .name("first.line.header")
            .displayName("csv headers first line")
            .description("csv headers grabbed from first line")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor DATABASE_FILE_PATH = new PropertyDescriptor.Builder()
            .name("csv.file.path")
            .displayName("Local path to the CSV File")
            .description("Local Path to the CSV File.")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();
    public static final PropertyDescriptor DATABASE_FILE_URI = new PropertyDescriptor.Builder()
            .name("csv.file.uri")
            .displayName("URL to the CSV File")
            .description("Path to the CSV File.")
            .required(false)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .build();
    public static final PropertyDescriptor ROW_KEY = new PropertyDescriptor.Builder()
            .name("row.key")
            .displayName("Row key")
            .description("th primary key of this db")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ENCODING_CHARSET = new PropertyDescriptor.Builder()
            .name("encoding.charset")
            .displayName("Encoding charset")
            .description("charset")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("UTF-8")
            .build();
    public static AllowableValue CSV_DEFAULT = new AllowableValue("default", "default", "Standard comma separated format, as for RFC4180 but allowing empty lines. Settings are: withDelimiter(',') withQuote('\"') withRecordSeparator(\"\\r\\n\") withIgnoreEmptyLines(true)");
    public static AllowableValue CSV_EXCEL = new AllowableValue("excel", "excel", "Excel file format (using a comma as the value delimiter). Note that the actual value delimiter used by Excel is locale dependent, it might be necessary to customize this format to accommodate to your regional settings. withDelimiter(',')  withQuote('\"') withRecordSeparator(\"\\r\\n\") withIgnoreEmptyLines(false) withAllowMissingColumnNames(true)");
    public static AllowableValue CSV_EXCEL_FR = new AllowableValue("excel_fr", "excel french", "Excel file format (using a comma as the value delimiter). Note that the actual value delimiter used by Excel is locale dependent, it might be necessary to customize this format to accommodate to your regional settings. withDelimiter(';')  withQuote('\"') withRecordSeparator(\"\\r\\n\") withIgnoreEmptyLines(false) withAllowMissingColumnNames(true)");
    public static AllowableValue CSV_MYSQL = new AllowableValue("mysql", "mysql", "Default MySQL format used by the SELECT INTO OUTFILE and LOAD DATA INFILE operations." +
            "This is a tab-delimited format with a LF character as the line separator. Values are not quoted and special characters are escaped with '\\'. The default NULL string is \"\\\\N\". Settings are: withDelimiter('\\t') withQuote(null) withRecordSeparator('\\n') withIgnoreEmptyLines(false) withEscape('\\\\') withNullString(\"\\\\N\") withQuoteMode(QuoteMode.ALL_NON_NULL)");
    public static AllowableValue CSV_RFC4180 = new AllowableValue("rfc4180", "RFC4180", "Comma separated format as defined by RFC 4180. Settings are: withDelimiter(',') withQuote('\"') withRecordSeparator(\"\\r\\n\") withIgnoreEmptyLines(false)");
    public static AllowableValue CSV_TDF = new AllowableValue("tdf", "Tab delimited", "Tab-delimited format. Settings are: withDelimiter('\\t') withQuote('\"') withRecordSeparator(\"\\r\\n\") withIgnoreSurroundingSpaces(true)");
    public static final PropertyDescriptor CSV_FORMAT = new PropertyDescriptor.Builder()
            .name("csv.format")
            .displayName("csv format")
            .description("a configuration for loading csv")
            .required(true)
            .allowableValues(CSV_DEFAULT, CSV_EXCEL, CSV_EXCEL_FR, CSV_MYSQL, CSV_RFC4180, CSV_TDF)
            .defaultValue(CSV_DEFAULT.getValue())
            .build();
    private static Logger logger = LoggerFactory.getLogger(CSVKeyValueCacheService.class);
    final AtomicReference<Map<String, String>> headers = new AtomicReference<>(new HashMap<>());
    protected String dbUri = null;
    protected String dbPath = null;
    protected String rowKey = null;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(CSV_FORMAT);
        props.add(CSV_HEADER);
        props.add(DATABASE_FILE_URI);
        props.add(DATABASE_FILE_PATH);
        props.add(ROW_KEY);
        props.add(CACHE_SIZE);
        props.add(FIRST_LINE_HEADER);
        props.add(ENCODING_CHARSET);
        return Collections.unmodifiableList(props);
    }

    @Override
    // @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException {
        super.init(context);
        try {

            if (context.getPropertyValue(DATABASE_FILE_URI).isSet()) {
                dbUri = context.getPropertyValue(DATABASE_FILE_URI).asString();
            }


            if (context.getPropertyValue(DATABASE_FILE_PATH).isSet()) {
                dbPath = context.getPropertyValue(DATABASE_FILE_PATH).asString();
            }

            if ((dbUri == null) && (dbPath == null)) {
                throw new Exception("You must declare " + DATABASE_FILE_URI.getName() +
                        " or " + DATABASE_FILE_PATH.getName());
            }

            InputStream is = null;
            if (dbUri != null) {
                logger.info("opening csv database from hdfs : " + dbUri);
                is = initFromUri(dbUri);
            }

            if (dbPath != null) {
                logger.info("opening csv database from local fs : " + dbPath);
                is = initFromPath(context, dbPath);
            }


            if (is == null) {
                throw new InitializationException("Something went wrong while initializing csv db from " +
                        DATABASE_FILE_URI.getName() + " or " + DATABASE_FILE_PATH.getName());
            }

            // final Reader reader = new InputStreamReader(is);
            CSVFormat format = CSVFormat.DEFAULT;
            if (context.getPropertyValue(CSV_FORMAT).asString().equals(CSV_EXCEL.getValue())) {
                format = CSVFormat.EXCEL;
            } else if (context.getPropertyValue(CSV_FORMAT).asString().equals(CSV_EXCEL_FR.getValue())) {
                format = CSVFormat.EXCEL.withDelimiter(';');
            } else if (context.getPropertyValue(CSV_FORMAT).asString().equals(CSV_MYSQL.getValue())) {
                format = CSVFormat.MYSQL;
            } else if (context.getPropertyValue(CSV_FORMAT).asString().equals(CSV_RFC4180.getValue())) {
                format = CSVFormat.RFC4180;
            } else if (context.getPropertyValue(CSV_FORMAT).asString().equals(CSV_TDF.getValue())) {
                format = CSVFormat.TDF;
            }


            if (context.getPropertyValue(CSV_HEADER).isSet()) {
                String[] columnNames = context.getPropertyValue(CSV_HEADER).asString().split(",");
                for (String name : columnNames) {
                    headers.get().put(name, "string");
                }
                format = format.withHeader(columnNames);
            } else if (context.getPropertyValue(FIRST_LINE_HEADER).isSet()) {
                format = format.withFirstRecordAsHeader();
            } else {
                throw new InitializationException("unable to get headers from somewhere");
            }


            Charset charset = Charset.forName("UTF-8");
            if (context.getPropertyValue(ENCODING_CHARSET).isSet()) {
                String encoding = context.getPropertyValue(ENCODING_CHARSET).asString();
                charset = Charset.forName(encoding);
            }

            rowKey = context.getPropertyValue(ROW_KEY).asString();
            CSVParser parser = CSVParser.parse(is, charset, format); //new CSVParser(reader, format);

            /*
            *    CSVParser parser = null;

            if (context.getPropertyValue(ENCODING_CHARSET).isSet()) {
                String encoding = context.getPropertyValue(ENCODING_CHARSET).asString();
                parser = CSVParser.parse(reader, Charset.forName(encoding), format);
            } else {
                parser = CSVParser.parse(reader, format);
            }
            */
            long count = 0;
            try {
                final Set<String> columnNames = parser.getHeaderMap().keySet();
                for (final CSVRecord record : parser) {

                    Record logislandRecord = new StandardRecord();
                    for (final String column : columnNames) {
                        logislandRecord.setStringField(column, record.get(column));
                    }

                    set(logislandRecord.getField(rowKey).asString(), logislandRecord);
                    count++;
                }
            } finally {
                logger.info("successfully loaded " + count + " records from CSV file");

                parser.close();
                is.close();
            }


        } catch (Exception e) {
            getLogger().error("Could not load database file: {}", new Object[]{e.getMessage()});
            throw new InitializationException(e);
        }
    }


    private InputStream initFromPath(ControllerServiceInitializationContext context, String dbPath) {
        final File dbFile = new File(dbPath);
        try {
            return new FileInputStream(dbFile);
        } catch (FileNotFoundException e) {
            logger.info(e.toString());
            return null;
        }


    }

    private InputStream initFromUri(String dbUri) {
        Configuration conf = new Configuration();

        String hdfsUri = conf.get("fs.defaultFS");
        getLogger().info("Default HDFS URI: " + hdfsUri);

        // Set HADOOP user to same as current suer
        String hadoopUser = System.getProperty("user.name");
        System.setProperty("HADOOP_USER_NAME", hadoopUser);
        System.setProperty("hadoop.home.dir", "/");

        // Get the HDFS filesystem
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(hdfsUri), conf);


            // Create a path to config file and init input stream
            Path hdfsReadpath = new Path(dbUri);
            getLogger().info("Reading DB file from HDFS at: " + dbUri);
            return fs.open(hdfsReadpath);
        } catch (IOException e) {
            logger.info(e.toString());
            return null;
        }

    }


    @Override
    public void waitUntilCollectionReady(String name, long timeoutMilli) throws DatastoreClientServiceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createCollection(String name, int partitionsCount, int replicationFactor) throws DatastoreClientServiceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropCollection(String name) throws DatastoreClientServiceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long countCollection(String name) throws DatastoreClientServiceException {
        return 0;
    }

    @Override
    public boolean existsCollection(String name) throws DatastoreClientServiceException {
        return false;
    }

    @Override
    public void refreshCollection(String name) throws DatastoreClientServiceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyCollection(String reindexScrollTimeout, String src, String dst) throws DatastoreClientServiceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createAlias(String collection, String alias) throws DatastoreClientServiceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean putMapping(String indexName, String doctype, String mappingAsJsonString) throws DatastoreClientServiceException {
        return false;
    }

    @Override
    public void bulkFlush() throws DatastoreClientServiceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void bulkPut(String collectionName, Record record) throws DatastoreClientServiceException {
        set(record.getField(rowKey).asString(), record);
    }

    @Override
    public void put(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {

        set(record.getField(rowKey).asString(), record);
    }

    @Override
    public void remove(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords) throws DatastoreClientServiceException {

        List<MultiGetResponseRecord> results = new ArrayList<>();
        for (MultiGetQueryRecord mgqr : multiGetQueryRecords) {
            String collectionName = mgqr.getIndexName();
            String typeName = mgqr.getTypeName();

            for (String id : mgqr.getDocumentIds()) {
                Record record = get(collectionName, new StandardRecord().setStringField(rowKey, id));
                Map<String, String> retrievedFields = new HashMap<>();
                if (record != null) {

                    if (mgqr.getFieldsToInclude()[0].equals("*")) {
                        for (Field field : record.getAllFieldsSorted()) {
                            if (!field.getName().equals(FieldDictionary.RECORD_TIME) &&
                                    !field.getName().equals(FieldDictionary.RECORD_TYPE) &&
                                    !field.getName().equals(FieldDictionary.RECORD_ID))
                                retrievedFields.put(field.getName(), field.asString());
                        }
                    } else {
                        for (String prop : mgqr.getFieldsToInclude()) {
                            retrievedFields.put(prop, record.getField(prop).asString());
                        }
                    }
                } else {
                    logger.debug("unable to retrieve record (id=" + id + ") from collection " + collectionName);
                }
                results.add(new MultiGetResponseRecord(collectionName, typeName, id, retrievedFields));
            }
        }

        return results;
    }

    @Override
    public Record get(String collectionName, Record record) throws DatastoreClientServiceException {
        if (record.hasField(rowKey)) {
            return get(record.getField(rowKey).asString());
        } else {
            logger.error("field " + rowKey + " not found in record " + record.toString());
            return null;
        }

    }

    @Override
    public Collection<Record> query(String query) {
        return null;
    }

    @Override
    public long queryCount(String query) {
        return 0;
    }
}
