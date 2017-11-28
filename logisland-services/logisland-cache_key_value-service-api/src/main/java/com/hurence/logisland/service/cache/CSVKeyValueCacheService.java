/**
 * Copyright (C) 2017 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.service.cache;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"csv", "service", "cache"})
@CapabilityDescription("A cache that store csv lines as records loaded from a file")
public class CSVKeyValueCacheService extends LRUKeyValueCacheService<String, Record> {


    public static AllowableValue CSV_DEFAULT = new AllowableValue("default", "default", "Standard comma separated format, as for RFC4180 but allowing empty lines. Settings are: withDelimiter(',') withQuote('\"') withRecordSeparator(\"\\r\\n\") withIgnoreEmptyLines(true)");

    public static AllowableValue CSV_EXCEL = new AllowableValue("excel", "excel", "Excel file format (using a comma as the value delimiter). Note that the actual value delimiter used by Excel is locale dependent, it might be necessary to customize this format to accommodate to your regional settings. withDelimiter(',')  withQuote('\"') withRecordSeparator(\"\\r\\n\") withIgnoreEmptyLines(false) withAllowMissingColumnNames(true)");

    public static AllowableValue CSV_EXCEL_FR = new AllowableValue("excel_fr", "excel french", "Excel file format (using a comma as the value delimiter). Note that the actual value delimiter used by Excel is locale dependent, it might be necessary to customize this format to accommodate to your regional settings. withDelimiter(';')  withQuote('\"') withRecordSeparator(\"\\r\\n\") withIgnoreEmptyLines(false) withAllowMissingColumnNames(true)");

    public static AllowableValue CSV_MYSQL = new AllowableValue("mysql", "mysql", "Default MySQL format used by the SELECT INTO OUTFILE and LOAD DATA INFILE operations.\n" +
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


    protected String dbUri = null;
    protected String dbPath = null;
    final AtomicReference<Map<String, String>> headers = new AtomicReference<>(new HashMap<>());

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
                is = initFromUri(dbUri);
            }

            if (dbPath != null) {
                is = initFromPath(context, dbPath);
            }


            final Reader reader = new InputStreamReader(is);
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


            String rowKey = context.getPropertyValue(ROW_KEY).asString();
            CSVParser parser = new CSVParser(reader, format);
            try {
                final Set<String> columnNames = parser.getHeaderMap().keySet();
                for (final CSVRecord record : parser) {

                    Record logislandRecord = new StandardRecord();
                    for (final String column : columnNames) {
                        logislandRecord.setStringField(column, record.get(column));
                    }

                    set(logislandRecord.getField(rowKey).asString(),logislandRecord );
                }
            } finally {
                parser.close();
                reader.close();
            }


        } catch (Exception e) {
            getLogger().error("Could not load database file: {}", new Object[]{e.getMessage()});
            throw new InitializationException(e);
        }
    }


    private InputStream initFromPath(ControllerServiceInitializationContext context, String dbPath) throws Exception {
        final File dbFile = new File(dbPath);

        return new FileInputStream(dbFile);
    }

    private InputStream initFromUri(String dbUri) throws Exception {
        Configuration conf = new Configuration();

        String hdfsUri = conf.get("fs.defaultFS");
        getLogger().info("Default HDFS URI: " + hdfsUri);

        // Set HADOOP user to same as current suer
        String hadoopUser = System.getProperty("user.name");
        System.setProperty("HADOOP_USER_NAME", hadoopUser);
        System.setProperty("hadoop.home.dir", "/");

        // Get the HDFS filesystem
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);

        // Create a path to config file and init input stream
        Path hdfsReadpath = new Path(dbUri);
        getLogger().info("Reading DB file from HDFS at: " + dbUri);
        FSDataInputStream inputStream = fs.open(hdfsReadpath);

        return inputStream;
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(CSV_FORMAT);
        props.add(CSV_HEADER);
        props.add(DATABASE_FILE_URI);
        props.add(DATABASE_FILE_PATH);
        props.add(ROW_KEY);
        props.add(CACHE_SIZE);
        return Collections.unmodifiableList(props);
    }


}
