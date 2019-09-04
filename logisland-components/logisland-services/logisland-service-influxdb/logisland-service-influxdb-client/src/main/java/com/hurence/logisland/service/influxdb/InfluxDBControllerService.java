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
package com.hurence.logisland.service.influxdb;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnStopped;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import com.hurence.logisland.service.datastore.MultiGetQueryRecord;
import com.hurence.logisland.service.datastore.MultiGetResponseRecord;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.apache.commons.lang3.NotImplementedException;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Pong;

import java.util.*;
import java.util.concurrent.*;

@Tags({"influxdb", "service", "time series"})
@CapabilityDescription(
        "Provides a controller service that for the moment only allows to bulkput records into influxdb."
)
public class InfluxDBControllerService extends AbstractControllerService implements InfluxDBClientService {

    private InfluxDB influxDB; // Client object
    private String database; // Destination database
    private Map<String, Set<String>> tags = new HashMap<String, Set<String>>(); // Tags for each measurement: measurement -> set of tags
    private Map<String, Set<String>> fields = new HashMap<String, Set<String>>(); // Fields for each measurement: measurement -> set of fields
    private Map<String, List<Object>> timeFields = new HashMap<String, List<Object>>(); // Time fields for each measurement: measurement -> [timeField,TimeUnit]
    private CONFIG_MODE mode;
    private ConsistencyLevel consistencyLevel;
    private String retentionPolicy;
    private InfluxDBUpdater updater;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private long flushInterval;
    final BlockingQueue<RecordToIndex> queue = new ArrayBlockingQueue<>(100000);
    volatile boolean stillSomeRecords = false; // tests only code

    /**
     * Holds a record to index and its meta data
     */
    static class RecordToIndex
    {
        // Destination keyspace.table
        private String measurement = null;
        // Record to index
        private Record record;

        public RecordToIndex(String measurement, Record record)
        {
            this.measurement = measurement;
            this.record = record;
        }

        public String getMeasurement() {
            return measurement;
        }

        public Record getRecord() {
            return record;
        }
    }

    protected static final PropertyDescriptor URL = new PropertyDescriptor.Builder()
            .name("influxdb.url")
            .displayName("InfluxDB url")
            .description("InfluxDB connection url")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    protected static final PropertyDescriptor USER = new PropertyDescriptor.Builder()
            .name("influxdb.user")
            .displayName("InfluxDB user name.")
            .description("The user name to use for authentication.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .build();

    protected static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("influxdb.password")
            .displayName("InfluxDB user password.")
            .description("The user password to use for authentication.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .build();

    protected static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .name("influxdb.database")
            .displayName("InfluxDB database")
            .description("InfluxDB database name")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    protected static final PropertyDescriptor TAGS = new PropertyDescriptor.Builder()
            .name("influxdb.tags")
            .displayName("InfluxDB explicit tags")
            .description("List of tags for each supported measurement. " +
                    " Syntax: <measurement>:<tag>[,<tag>]...[;<measurement>:<tag>,[<tag>]]..." +
                    " Example: cpu:core1,core2;mem:used : in this example, the cpu measurement" +
                    " has 2 tags: core1 and core2 and the mem measurement has 1 tag: used." +
                    " This must only be set if configuration mode is " + CONFIG_MODE.EXPLICIT_TAGS_AND_FIELDS + " or " +
                    CONFIG_MODE.ALL_AS_FIELDS_BUT_EXPLICIT_TAGS + ".")
            .addValidator(Validation.TAGS_VALIDATOR)
            .required(false)
            .build();

    protected static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("influxdb.fields")
            .displayName("InfluxDB explicit fields")
            .description("List of fields for each supported measurement. " +
                    " Syntax: <measurement>:<field>[,<field>]...[;<measurement>:<field>,[<field>]]..." +
                    " Example: cpu:core1,core2;mem:used : in this example, the cpu measurement" +
                    " has 2 fields: core1 and core2 and the mem measurement has 1 field: used." +
                    " This must only be set if configuration mode is " + CONFIG_MODE.EXPLICIT_TAGS_AND_FIELDS + " or " +
                    CONFIG_MODE.ALL_AS_TAGS_BUT_EXPLICIT_FIELDS + ".")
            .addValidator(Validation.FIELDS_VALIDATOR)
            .required(false)
            .build();

    protected static final PropertyDescriptor TIME_FIELD = new PropertyDescriptor.Builder()
            .name("influxdb.timefield")
            .displayName("InfluxDB time field")
            .description("Time field for each supported measurement. " +
                    " Syntax: <measurement>:<field>,<format>...[;<measurement>:<field>,<format>]..." +
                    " With format being any constant defined in " +
                    " java.util.concurrent.TimeUnit enum: DAYS, HOURS, MICROSECONDS, MILLISECONDS, MINUTES, NANOSECONDS or SECONDS." +
                    " Example: cpu:time,NANOSECONDS;mem:timeStamp,MILLISECONDS" +
                    " In this example: for the cpu measurement, the time for the influx DB point matching the record will" +
                    " be the value of the time field that represents nanoseconds. For the mem measurement, the time for" +
                    " the influx DB point matching the record will be the value of the timeStamp field that represents milliseconds. " +
                    " Any measurement for which the time field is not defined will use the content of the " + FieldDictionary.RECORD_TIME +
                    " technical field as the time (which is a number of milliseconds since epoch).")
            .addValidator(Validation.TIME_FIELD_VALIDATOR)
            .required(false)
            .build();

    /** Available configuration mode */
    static enum CONFIG_MODE {
        EXPLICIT_TAGS_AND_FIELDS("explicit_tags_and_fields"),
        ALL_AS_FIELDS("all_as_fields"),
        ALL_AS_TAGS_BUT_EXPLICIT_FIELDS("all_as_tags_but_explicit_fields"),
        ALL_AS_FIELDS_BUT_EXPLICIT_TAGS("all_as_fields_but_explicit_tags");

        private String value;
        CONFIG_MODE(String value)
        {
            this.value = value;
        }

        public String toString()
        {
            return value;
        }

        public static CONFIG_MODE fromValue(String value)
        {
            switch(value)
            {
                case "explicit_tags_and_fields":
                    return EXPLICIT_TAGS_AND_FIELDS;
                case "all_as_fields":
                    return ALL_AS_FIELDS;
                case "all_as_tags_but_explicit_fields":
                    return ALL_AS_TAGS_BUT_EXPLICIT_FIELDS;
                case "all_as_fields_but_explicit_tags":
                    return ALL_AS_FIELDS_BUT_EXPLICIT_TAGS;
                default:
                    throw new RuntimeException("Unknown configuration mode: " + value);
            }
        }
    }

    protected static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("influxdb.configuration_mode")
            .displayName("InfluxDB configuration mode that determines how tags and fields are chosen.")
            .description("Determines the way fields and tags are chosen from the logisland record. " +
                    "Possible values and meaning: " +
                    CONFIG_MODE.EXPLICIT_TAGS_AND_FIELDS.toString() + ": only logisland record fields listed in " +
                    TAGS.getName() + " and " + FIELDS.getName() + " will be inserted into InfluxDB with the explicit type. " +
                    CONFIG_MODE.ALL_AS_FIELDS.toString() + ": all available logisland record fields will be inserted into " +
                    " InfluxDB as fields. " +
                    CONFIG_MODE.ALL_AS_TAGS_BUT_EXPLICIT_FIELDS.toString() + ": all available logisland record fields will be inserted into " +
                            " InfluxDB as tags except those listed in " + FIELDS.getName() + " that will be inserted into InfluxDB as fields. " +
                    CONFIG_MODE.ALL_AS_FIELDS_BUT_EXPLICIT_TAGS.toString() + ": all available logisland record fields will be inserted into " +
                    " InfluxDB as fields except those listed in " + TAGS.getName() + " that will be inserted into InfluxDB as tags"
             )
            .allowableValues(
                    CONFIG_MODE.EXPLICIT_TAGS_AND_FIELDS.toString(),
                    CONFIG_MODE.ALL_AS_FIELDS.toString(),
                    CONFIG_MODE.ALL_AS_FIELDS_BUT_EXPLICIT_TAGS.toString(),
                    CONFIG_MODE.ALL_AS_TAGS_BUT_EXPLICIT_FIELDS.toString())
            .required(true)
            .build();

    protected static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .name("influxdb.consistency_level")
            .displayName("InfluxDB consistency level.")
            .description("Determines the consistency level used to write points into InfluxDB. Possible values are: " +
                    ConsistencyLevel.ANY + ", " + ConsistencyLevel.ONE + ", " + ConsistencyLevel.QUORUM + "and " +
                    ConsistencyLevel.ALL + ". Default value is " + ConsistencyLevel.ANY + ". This is only useful when " +
                    " using a clustered InfluxDB infrastructure.")
            .allowableValues(
                    ConsistencyLevel.ANY.toString(),
                    ConsistencyLevel.ONE.toString(),
                    ConsistencyLevel.QUORUM.toString(),
                    ConsistencyLevel.ALL.toString())
            .required(false)
            .defaultValue(ConsistencyLevel.ANY.toString())
            .build();

    protected static final PropertyDescriptor RETENTION_POLICY = new PropertyDescriptor.Builder()
            .name("influxdb.retention_policy")
            .displayName("InfluxDB retention policy.")
            .description("Determines the name of the retention policy to use. Defaults to autogen. The defined retention" +
                    " policy must already be defined in the InfluxDB server.")
            .defaultValue("autogen")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(URL);
        descriptors.add(USER);
        descriptors.add(DATABASE);
        descriptors.add(PASSWORD);
        descriptors.add(TAGS);
        descriptors.add(FIELDS);
        descriptors.add(MODE);
        descriptors.add(CONSISTENCY_LEVEL);
        descriptors.add(RETENTION_POLICY);
        descriptors.add(TIME_FIELD);
        descriptors.add(BATCH_SIZE);
        descriptors.add(FLUSH_INTERVAL);
        return descriptors;
    }

    Map<String, Set<String>> getTags()
    {
        return tags;
    }

    Map<String, Set<String>> getFields()
    {
        return fields;
    }

    Map<String, List<Object>> getTimeFields()
    {
        return timeFields;
    }

    CONFIG_MODE getMode()
    {
        return mode;
    }

    String getDatabase()
    {
        return database;
    }

    ConsistencyLevel getConsistencyLevel()
    {
        return consistencyLevel;
    }

    String getRetentionPolicy()
    {
        return retentionPolicy;
    }

    @Override
    public void init(ControllerServiceInitializationContext context) throws InitializationException {

        /**
         * Get config and establish connection to influxdb
         */
        super.init(context);

        // Database
        database = context.getPropertyValue(DATABASE).asString();

        // Configuration mode
        mode = CONFIG_MODE.fromValue(context.getPropertyValue(MODE).asString());

        // Parse tags and fields according to defined configuration mode
        switch (mode)
        {
            case EXPLICIT_TAGS_AND_FIELDS:
                parseTags(context.getPropertyValue(TAGS).asString());
                parseFields(context.getPropertyValue(FIELDS).asString());
                checkTagsAndFieldsForDuplicates();
                break;
            case ALL_AS_TAGS_BUT_EXPLICIT_FIELDS:
                parseFields(context.getPropertyValue(FIELDS).asString());
                break;
            case ALL_AS_FIELDS_BUT_EXPLICIT_TAGS:
                parseTags(context.getPropertyValue(TAGS).asString());
                break;
        }

        // Parse time fields if any
        if (context.getPropertyValue(TIME_FIELD).isSet())
        {
            parseTimeFields(context.getPropertyValue(TIME_FIELD).asString());
            checkTagsOrFieldsTimeFieldCollision();
        }

        // Consistency level
        if (context.getPropertyValue(CONSISTENCY_LEVEL).isSet())
        {
            consistencyLevel = ConsistencyLevel.valueOf(context.getPropertyValue(CONSISTENCY_LEVEL).asString());
        }

        // Retention policy
        if (context.getPropertyValue(RETENTION_POLICY).isSet())
        {
            retentionPolicy = context.getPropertyValue(RETENTION_POLICY).asString();
        }

        // Url
        String influxUrl = context.getPropertyValue(URL).asString();

        String influxUserName = null;
        if (context.getPropertyValue(USER).isSet())
        {
            influxUserName = context.getPropertyValue(USER).asString();
        }

        String influxUserPassword = null;
        if (context.getPropertyValue(PASSWORD).isSet())
        {
            influxUserPassword = context.getPropertyValue(PASSWORD).asString();
        }
        influxUserPassword = context.getPropertyValue(PASSWORD).asString();

        boolean withCredentials = false;
        String credDetails = "none";
        if ( (influxUserName != null) && (influxUserName.length() > 0) &&
                (influxUserPassword != null) && (influxUserPassword.length() > 0) )
        {
            withCredentials = true;
            credDetails = "username=" + influxUserName;
        }

        getLogger().info("Establishing InfluxDB connection to url " + influxUrl + ". Credentials: " + credDetails);

        // Connect
        if (withCredentials)
        {
            influxDB = InfluxDBFactory.connect(influxUrl, influxUserName, influxUserPassword);
        } else
        {
            influxDB = InfluxDBFactory.connect(influxUrl);
        }

        Pong pong = influxDB.ping();

        if (pong == null)
        {
            throw new InitializationException("Could not connect to InfluxDB");
        }
        if (!pong.isGood())
        {
            throw new InitializationException("Could not connect to InfluxDB (bad pong)");
        }

        getLogger().info("Connected to InfluxDB: " + pong);

        startUpdater(context);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {

        final List<ValidationResult> problems = new ArrayList<>();

        // Sanity checking
        if (context.getPropertyValue(PASSWORD).isSet() && !context.getPropertyValue(USER).isSet())
        {
            problems.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(this.getClass().getSimpleName())
                    .explanation("Password provided without user name.")
                    .build());
        }
        if (!context.getPropertyValue(PASSWORD).isSet() && context.getPropertyValue(USER).isSet())
        {
            problems.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(this.getClass().getSimpleName())
                    .explanation("User name provided without password.")
                    .build());
        }
            mode = CONFIG_MODE.fromValue(context.getPropertyValue(MODE).asString());

        // Parse tags and fields according to defined configuration mode
        switch (mode)
        {
            case EXPLICIT_TAGS_AND_FIELDS:
                if (!context.getPropertyValue(TAGS).isSet())
                {
                    problems.add(new ValidationResult.Builder()
                            .valid(false)
                            .subject(this.getClass().getSimpleName())
                            .explanation("Configuration mode " + mode + " requires " + TAGS.getName() + " to be set.")
                            .build());
                }
                if (!context.getPropertyValue(FIELDS).isSet())
                {
                    problems.add(new ValidationResult.Builder()
                            .valid(false)
                            .subject(this.getClass().getSimpleName())
                            .explanation("Configuration mode " + mode + " requires " + FIELDS.getName() + " to be set.")
                            .build());
                }
                break;
            case ALL_AS_FIELDS:
                if (context.getPropertyValue(TAGS).isSet())
                {
                    problems.add(new ValidationResult.Builder()
                            .valid(false)
                            .subject(this.getClass().getSimpleName())
                            .explanation("Configuration mode " + mode + " requires " + TAGS.getName() + " to not be set.")
                            .build());
                }
                if (context.getPropertyValue(FIELDS).isSet())
                {
                    problems.add(new ValidationResult.Builder()
                            .valid(false)
                            .subject(this.getClass().getSimpleName())
                            .explanation("Configuration mode " + mode + " requires " + FIELDS.getName() + " to not be set.")
                            .build());
                }
                break;
            case ALL_AS_TAGS_BUT_EXPLICIT_FIELDS:
                if (context.getPropertyValue(TAGS).isSet())
                {
                    problems.add(new ValidationResult.Builder()
                            .valid(false)
                            .subject(this.getClass().getSimpleName())
                            .explanation("Configuration mode " + mode + " requires " + TAGS.getName() + " to not be set.")
                            .build());
                }
                if (!context.getPropertyValue(FIELDS).isSet())
                {
                    problems.add(new ValidationResult.Builder()
                            .valid(false)
                            .subject(this.getClass().getSimpleName())
                            .explanation("Configuration mode " + mode + " requires " + FIELDS.getName() + " to be set.")
                            .build());
                }
                break;
            case ALL_AS_FIELDS_BUT_EXPLICIT_TAGS:
                if (!context.getPropertyValue(TAGS).isSet())
                {
                    problems.add(new ValidationResult.Builder()
                            .valid(false)
                            .subject(this.getClass().getSimpleName())
                            .explanation("Configuration mode " + mode + " requires " + TAGS.getName() + " to be set.")
                            .build());
                }
                if (context.getPropertyValue(FIELDS).isSet())
                {
                    problems.add(new ValidationResult.Builder()
                            .valid(false)
                            .subject(this.getClass().getSimpleName())
                            .explanation("Configuration mode " + mode + " requires " + FIELDS.getName() + " to not be set.")
                            .build());
                }
                break;
        }
        return problems;
    }

    /**
     * Checks no duplicate field is present in both tag and field lists
     * @throws InitializationException
     */
    private void checkTagsAndFieldsForDuplicates() throws InitializationException
    {

        for (Map.Entry<String, Set<String>> entry : tags.entrySet())
        {
            String measurement = entry.getKey();
            Set<String> fieldSet = fields.get(measurement);
            if (fieldSet != null)
            {
                // Measurement is present in fields also, check for duplicates
                Set<String> tagSet = entry.getValue();
                for (String tag : tagSet)
                {
                    if (fieldSet.contains(tag))
                    {
                        throw new InitializationException("Measurement " + measurement + " has a duplicate value in tags" +
                                " list and fields list: " + tag);
                    }
                }
            }
        }
    }

    /**
     * Checks that not time field is defined either in tags or fields
     * @throws InitializationException
     */
    private void checkTagsOrFieldsTimeFieldCollision() throws InitializationException
    {
        // Check tags
        for (Map.Entry<String, Set<String>> entry : tags.entrySet())
        {
            String measurement = entry.getKey();
            List<Object> timeFieldAndFormat = timeFields.get(measurement);
            if (timeFieldAndFormat == null)
            {
                // No specified time field for this measurement: go to next
                continue;
            }
            String timeField = (String)timeFieldAndFormat.get(0);
            for (String tag : entry.getValue())
            {
                if (tag.equals(timeField))
                {
                    throw new InitializationException("In measurement " + measurement + ", " + timeField + " cannot be" +
                            " both a tag and a time field.");
                }
            }
        }

        // Check fields
        for (Map.Entry<String, Set<String>> entry : fields.entrySet())
        {
            String measurement = entry.getKey();
            List<Object> timeFieldAndFormat = timeFields.get(measurement);
            if (timeFieldAndFormat == null)
            {
                // No specified time field for this measurement: go to next
                continue;
            }
            String timeField = (String)timeFieldAndFormat.get(0);
            for (String field : entry.getValue())
            {
                if (field.equals(timeField))
                {
                    throw new InitializationException("In measurement " + measurement + ", " + timeField + " cannot be" +
                            " both a field and a time field.");
                }
            }
        }
    }

    /**
     * Parses configured list of tags for each measurement
     * @param input Configuration value
     * @throws InitializationException
     */
    private void parseTags(String input) throws InitializationException
    {
        String trimmedValue = input.trim();
        if (trimmedValue.length() == 0)
        {
            throw new InitializationException("Empty tags");
        }
        String[] measurementEntries = trimmedValue.split(";");
        if (measurementEntries.length == 0)
        {
            throw new InitializationException("No measurement entry found in tags");
        }
        // Go through measurement entries
        for (String measurementEntry : measurementEntries) {
            String trimmedMeasurementEntry = measurementEntry.trim();
            String[] measurementAndTags = trimmedMeasurementEntry.split(":");
            if (measurementAndTags.length != 2)
            {
                throw new InitializationException("Measurement entry not conform: " + measurementEntry);
            }
            String measurement = measurementAndTags[0];
            String trimmedMeasurement = measurement.trim();
            if (trimmedMeasurement.length() == 0)
            {
                throw new InitializationException("Measurement entry not conform: " + measurementEntry);
            }
            String tagList = measurementAndTags[1];
            String trimmedTags = tagList.trim();
            if (trimmedTags.length() == 0)
            {
                throw new InitializationException("Measurement entry not conform (tags part): " + measurementEntry);
            }
            String[] tagValues = trimmedTags.split(",");
            if (tagValues.length == 0)
            {
                throw new InitializationException("Measurement entry not conform (tags part): " + measurementEntry);
            }
            Set<String> tagSet = new HashSet<String>();
            for (String tag : tagValues)
            {
                String trimmedTag = tag.trim();
                if (trimmedTag.length() == 0)
                {
                    throw new InitializationException("Measurement entry not conform (tags part): " + measurementEntry);
                }
                tagSet.add(trimmedTag);
            }
            tags.put(trimmedMeasurement, tagSet);
        }
    }

    /**
     * Parses configured list of fields for each measurement
     * @param input Configuration value
     * @throws InitializationException
     */
    private void parseFields(String input) throws InitializationException
    {
        String trimmedValue = input.trim();
        if (trimmedValue.length() == 0)
        {
            throw new InitializationException("Empty fields");
        }
        String[] measurementEntries = trimmedValue.split(";");
        if (measurementEntries.length == 0)
        {
            throw new InitializationException("No measurement entry found in fields");
        }
        // Go through measurement entries
        for (String measurementEntry : measurementEntries) {
            String trimmedMeasurementEntry = measurementEntry.trim();
            String[] measurementAndFields = trimmedMeasurementEntry.split(":");
            if (measurementAndFields.length != 2)
            {
                throw new InitializationException("Measurement entry not conform: " + measurementEntry);
            }
            String measurement = measurementAndFields[0];
            String trimmedMeasurement = measurement.trim();
            if (trimmedMeasurement.length() == 0)
            {
                throw new InitializationException("Measurement entry not conform: " + measurementEntry);
            }
            String fieldList = measurementAndFields[1];
            String trimmedFields = fieldList.trim();
            if (trimmedFields.length() == 0)
            {
                throw new InitializationException("Measurement entry not conform (fields part): " + measurementEntry);
            }
            String[] fieldValues = trimmedFields.split(",");
            if (fieldValues.length == 0)
            {
                throw new InitializationException("Measurement entry not conform (fields part): " + measurementEntry);
            }
            Set<String> fieldSet = new HashSet<String>();
            for (String field : fieldValues)
            {
                String trimmedField = field.trim();
                if (trimmedField.length() == 0)
                {
                    throw new InitializationException("Measurement entry not conform (fields part): " + measurementEntry);
                }
                fieldSet.add(trimmedField);
            }
            fields.put(trimmedMeasurement, fieldSet);
        }
    }

    /**
     * Parses configured list of time fields for each measurement
     * @param input Configuration value
     * @throws InitializationException
     */
    private void parseTimeFields(String input)  throws InitializationException {
        String trimmedValue = input.trim();
        if (trimmedValue.length() == 0)
        {
            throw new InitializationException("Empty timefield");
        }
        String[] measurementEntries = trimmedValue.split(";");
        if (measurementEntries.length == 0)
        {
            throw new InitializationException("No measurement entry found in timefield");
        }
        // Go through measurement entries
        for (String measurementEntry : measurementEntries) {
            String trimmedMeasurementEntry = measurementEntry.trim();
            String[] measurementAndField = trimmedMeasurementEntry.split(":");
            if (measurementAndField.length != 2)
            {
                throw new InitializationException("Measurement entry not conform: " + measurementEntry);
            }
            String measurement = measurementAndField[0];
            String trimmedMeasurement = measurement.trim();
            if (trimmedMeasurement.length() == 0)
            {
                throw new InitializationException("Measurement entry not conform: " + measurementEntry);
            }
            String field = measurementAndField[1];
            String trimmedField = field.trim();
            if (trimmedField.length() == 0)
            {
                throw new InitializationException("Measurement entry not conform (timefield part): " + measurementEntry);
            }
            String[] fieldAndFormat = trimmedField.split(",");
            if (fieldAndFormat.length != 2)
            {
                throw new InitializationException("Measurement entry not conform (timefield part): " + measurementEntry);
            }
            String timeField = fieldAndFormat[0].trim();
            if (timeField.isEmpty())
            {
                throw new InitializationException("Measurement entry not conform (missing time field): " + measurementEntry);
            }
            String formatString = fieldAndFormat[1].trim();
            if (formatString.isEmpty())
            {
                throw new InitializationException("Measurement entry not conform (missing format): " + measurementEntry);
            }
            // Check format
            TimeUnit format = null;
            try {
                format = TimeUnit.valueOf(formatString);
            } catch(IllegalArgumentException e)
            {
                throw new InitializationException("Measurement entry not conform (unsupported format " + format + "): " + measurementEntry);
            }
            List<Object> timeFieldAndFormat = Arrays.asList(timeField, format);
            timeFields.put(measurement, timeFieldAndFormat);
        }
    }

    // Note: we use the @OnDisabled facility here so that test can call proper disconnection with
    // runner.disableControllerService(service); runner has no stopControllerService(service)
    // This service does not however currently supports disable/enable out of test
    @OnDisabled
    @OnStopped
    public final void stop() {

        stopUpdater();

        if (influxDB != null) {
            influxDB.close();
        }
    }

    /**
     * Starts the updaters
     */
    private void startUpdater(ControllerServiceInitializationContext context)
    {

        /**
         * Prepare the update
         */

        // setup a thread pool of influxdb updaters
        int batchSize = context.getPropertyValue(BATCH_SIZE).asInteger();
        flushInterval = context.getPropertyValue(FLUSH_INTERVAL).asLong();
        updater = new InfluxDBUpdater(influxDB, queue , batchSize, this, flushInterval);

        executorService.execute(updater);
    }

    /**
     * Stops the updater
     */
    private void stopUpdater()
    {
        updater.stop();

        executorService.shutdown();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            getLogger().error("Timeout waiting for influxdb updater to terminate");
        }

        updater = null;
    }

    @Override
    public void createCollection(String name, int partitionsCount, int replicationFactor) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for InfluxDB");
    }

    @Override
    public void dropCollection(String name) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for InfluxDB");
    }

    @Override
    public long countCollection(String name) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for InfluxDB");
    }

    @Override
    public boolean existsCollection(String name) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for InfluxDB");
    }

    @Override
    public void refreshCollection(String name) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for InfluxDB");
    }

    @Override
    public void copyCollection(String reindexScrollTimeout, String src, String dst) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for InfluxDB");
    }

    @Override
    public void createAlias(String collection, String alias) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for InfluxDB");
    }

    @Override
    public boolean putMapping(String indexName, String doctype, String mappingAsJsonString) throws DatastoreClientServiceException {
        getLogger().warn("putMapping not implemented for InfluxDB");
        if (true) {
            throw new NotImplementedException("Not yet supported for InfluxDB");
        }
        return false;
    }

    // Special collection name used in test to know when the last record of the test is treated
    public static final String END_OF_TEST = "endoftest";

    /**
     * Unit tests only code
     */
    public void waitForFlush()
    {
        // Then wait for all records sent to influxdb
        while (this.stillSomeRecords)
        {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                getLogger().error("Interrupted while waiting for influxdb updater flush [step 2]");
            }
        }
    }

    @Override
    public void bulkFlush() throws DatastoreClientServiceException {

        // TODO: current bulkPut processor implementation systematically calls bulkFlush. I think it should not otherwise
        // what's the point in having a dedicated thread for insertion (updater thread)?
        // If we put some mechanism to wait for flush here, the perf will be impacted. So for test purpose only,
        // I set the flush mechanism in waitForFlush
    }

    @Override
    public void bulkPut(String collectionName, Record record) throws DatastoreClientServiceException {
        if ( (collectionName != null) && (record != null) ) {
            stillSomeRecords = true;
            queue.add(new RecordToIndex(collectionName, record));
        } else {
            if ( (record != null) && (collectionName == null) )
            {
                record.addError(ProcessError.UNKNOWN_ERROR.toString(),
                        "Trying to bulkput to InfluxDB with a null collection name");
            }

            if (record == null)
                getLogger().error("Trying to add a null record in the queue");
        }
    }

    @Override
    public void put(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        // For time being, support it through bulkPut
        bulkPut(collectionName, record);
    }

    @Override
    public void remove(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for InfluxDB");
    }

    @Override
    public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for InfluxDB");
    }

    @Override
    public Record get(String collectionName, Record record) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for InfluxDB");
    }

    @Override
    public Collection<Record> query(String query) {
        throw new NotImplementedException("Not yet supported for InfluxDB");
    }

    @Override
    public long queryCount(String query) {
        throw new NotImplementedException("Not yet supported for InfluxDB");
    }
}