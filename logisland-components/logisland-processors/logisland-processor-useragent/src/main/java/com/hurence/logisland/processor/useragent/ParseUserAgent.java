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
package com.hurence.logisland.processor.useragent;

import com.hurence.logisland.annotation.documentation.*;
import com.hurence.logisland.classloading.PluginLoader;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.*;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;

import com.hurence.logisland.validator.ValidationResult;
import com.hurence.logisland.validator.Validator;
import nl.basjes.parse.useragent.UserAgent;
import nl.basjes.parse.useragent.UserAgentAnalyzer;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * HTTP user-agent processor
 */
@Category(ComponentCategory.ENRICHMENT)
@Tags({"User-Agent", "clickstream", "DMP"})
@CapabilityDescription(
        "The user-agent processor allows to decompose User-Agent value from an HTTP header into several attributes of interest."
                + " There is no standard format for User-Agent strings, hence it is not easily possible to use regexp to handle them."
                + " This processor rely on the `YAUAA library <https://github.com/nielsbasjes/yauaa>`_ to do the heavy work.")
@ExtraDetailFile("./details/ParseUserAgent-Detail.rst")
public class ParseUserAgent extends AbstractProcessor {

    private static final Object sync = new Object();

    private boolean debug;
    private String userAgentField;
    private boolean useCache;
    private boolean userAgentKeep;
    private int cacheSize;
    private List<String> selectedFields;
    private boolean confidenceEnabled;
    private boolean ambiguityEnabled;

    private static final String KEY_DEBUG = "debug";
    private static final String KEY_CACHE_ENABLED = "cache.enabled";
    private static final String KEY_CACHE_SIZE = "cache.size";
    private static final String KEY_USERAGENT_FIELD = "useragent.field";
    private static final String KEY_USERAGENT_KEEP = "useragent.keep";
    private static final String KEY_FIELDS_TO_RETURN = "fields";
    private static final String KEY_CONFIDENCE_ENABLED = "confidence.enabled";
    private static final String KEY_AMBIGUITY_ENABLED = "ambiguity.enabled";

    //private static GenericObjectPool<UserAgentAnalyzer> pool;

//    private static final List<String> defaultFields = Arrays.asList(
//            "DeviceClass",
//            "DeviceName",
//            "DeviceBrand",
//            "DeviceFirmwareVersion",
//            "DeviceVersion",
//            "OperatingSystemClass",
//            "OperatingSystemName",
//            "OperatingSystemVersion",
//            "OperatingSystemNameVersion",
//            "LayoutEngineClass",
//            "LayoutEngineName",
//            "LayoutEngineVersion",
//            "LayoutEngineVersionMajor",
//            "LayoutEngineNameVersion",
//            "LayoutEngineNameVersionMajor",
//            "AgentClass",
//            "AgentName",
//            "AgentVersion",
//            "AgentVersionMajor",
//            "AgentNameVersion",
//            "AgentNameVersionMajor"
//            );

    private static final List<String> defaultFields;

    static {
        //is there a better way?
        ClassLoader cl = PluginLoader.getRegistry().get(ParseUserAgent.class.getCanonicalName());
        ClassLoader current = Thread.currentThread().getContextClassLoader();
        //will never be null
        try {
            Thread.currentThread().setContextClassLoader(cl);
            defaultFields = new UserAgentAnalyzer().getAllPossibleFieldNamesSorted();
        } finally {
            Thread.currentThread().setContextClassLoader(current);
        }
    }


    public static final PropertyDescriptor DEBUG = new PropertyDescriptor.Builder()
            .name(KEY_DEBUG)
            .description("Enable debug.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .defaultValue("false")
            .build();

    // Use cache
    public static final PropertyDescriptor CACHE_ENABLED = new PropertyDescriptor.Builder()
            .name(KEY_CACHE_ENABLED)
            .description("Enable caching. Caching to avoid to redo the same computation for many identical User-Agent strings.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .defaultValue("true")
            .build();

    // Cache size
    public static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name(KEY_CACHE_SIZE)
            .description("Set the size of the cache.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(false)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor USERAGENT_FIELD = new PropertyDescriptor.Builder()
            .name(KEY_USERAGENT_FIELD)
            .description("Must contain the name of the field that contains the User-Agent value in the incoming record.")
            //.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            //.defaultValue("useragent") // TODO : define what should be the default value
            .build();

    // Removes original User-Agent
    public static final PropertyDescriptor USERAGENT_KEEP = new PropertyDescriptor.Builder()
            .name(KEY_USERAGENT_KEEP)
            .description("Defines if the field that contained the User-Agent must be kept or not in the resulting records.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .defaultValue("true")
            .build();

    // Report per field confidence
    public static final PropertyDescriptor CONFIDENCE_ENABLED = new PropertyDescriptor.Builder()
            .name(KEY_CONFIDENCE_ENABLED)
            .description("Enable confidence reporting. Each field will report a confidence attribute with a value comprised between 0 and 10000.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .defaultValue("false")
            .build();

    // Report ambiguity count
    public static final PropertyDescriptor AMBIGUITY_ENABLED = new PropertyDescriptor.Builder()
            .name(KEY_AMBIGUITY_ENABLED)
            .description("Enable ambiguity reporting. Reports a count of ambiguities.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .defaultValue("false")
            .build();

    // List of attributes to return
    public static final PropertyDescriptor FIELDS_TO_RETURN = new PropertyDescriptor.Builder()
            .name(KEY_FIELDS_TO_RETURN)
            .description("Defines the fields to be returned.")
            .addValidator(new Validator() {
                @Override
                public ValidationResult validate(final String subject, final String value) {

                    String reason = null;
                    try {
                        String[] fields = value.split(",");
                        for (String field : fields) {
                            String f = field.trim();
                            if (!defaultFields.contains(f)) {
                                reason += "The field " + f + " is not valid. ";
                            }
                        }
                    } catch (final Exception e) {
                        reason = "not a comma separated list";
                    }

                    return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
                }
            })
            .required(false)
            .defaultValue(String.join(", ", defaultFields))
            .build();

    // TODO :  add the following params
    // Resource file with regex ???

    // error if useragent field is missing true/false

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DEBUG);
        descriptors.add(CACHE_ENABLED);
        descriptors.add(CACHE_SIZE);
        descriptors.add(USERAGENT_FIELD);
        descriptors.add(USERAGENT_KEEP);
        descriptors.add(CONFIDENCE_ENABLED);
        descriptors.add(AMBIGUITY_ENABLED);
        descriptors.add(FIELDS_TO_RETURN);
        return Collections.unmodifiableList(descriptors);
    }


    @Override
    public void init(final ProcessContext context) throws InitializationException {
        super.init(context);
        getLogger().debug("Initializing User-Agent Processor");

        debug = context.getPropertyValue(DEBUG).asBoolean();
        userAgentField = context.getPropertyValue(USERAGENT_FIELD).asString();
        userAgentKeep = context.getPropertyValue(USERAGENT_KEEP).asBoolean();
        useCache = context.getPropertyValue(CACHE_ENABLED).asBoolean();
        cacheSize = context.getPropertyValue(CACHE_SIZE).asInteger();
        String tmp = context.getPropertyValue(FIELDS_TO_RETURN).asString();
        selectedFields = Arrays.asList(tmp.split(",")).stream().map(String::trim).collect(Collectors.toList());
        confidenceEnabled = context.getPropertyValue(CONFIDENCE_ENABLED).asBoolean();
        ambiguityEnabled = context.getPropertyValue(AMBIGUITY_ENABLED).asBoolean();

        if (debug) {
            getLogger().info(KEY_USERAGENT_FIELD + "\t: " + userAgentField);
            getLogger().info(KEY_USERAGENT_KEEP + "\t: " + userAgentKeep);
            getLogger().info(KEY_DEBUG + "\t: " + debug);
            getLogger().info(KEY_CACHE_ENABLED + "\t: " + useCache);
            getLogger().info(KEY_CACHE_SIZE + "\t: " + cacheSize);
            getLogger().info(KEY_FIELDS_TO_RETURN + "\t: " + selectedFields);
            getLogger().info(KEY_CONFIDENCE_ENABLED + "\t: " + confidenceEnabled);
            getLogger().info(KEY_AMBIGUITY_ENABLED + "\t: " + ambiguityEnabled);
        }

        if (Singleton.get() == null) {
            getLogger().debug("Initializing UserAgentAnalyzerPool");
            synchronized (sync) {
                if (Singleton.get() == null) {

                    GenericObjectPoolConfig config = new GenericObjectPoolConfig();

                    //config.setMaxIdle(1);
                    config.setMaxTotal(10);

                    //TestOnBorrow=true --> To ensure that we get a valid object from pool
                    //config.setTestOnBorrow(true);

                    //TestOnReturn=true --> To ensure that valid object is returned to pool
                    //config.setTestOnReturn(true);

                    PooledUserAgentAnalyzerFactory factory = new PooledUserAgentAnalyzerFactory(selectedFields, cacheSize);

                    UserAgentAnalyzerPool pool = new UserAgentAnalyzerPool(factory, config);
                    Singleton.set(pool);
                }
            }
        }
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        if (debug) {
            getLogger().debug("User-Agent Processor records input: " + records);
        }
        UserAgentAnalyzerPool pool = null;
        UserAgentAnalyzer uaa = null;
        try {
            pool = (UserAgentAnalyzerPool) Singleton.get();
            getLogger().debug("borrow UserAgentAnalyzer from pool");
            uaa = pool.borrowObject();
            processRecords(records, uaa);
        } catch (Throwable t) {
            getLogger().error("Error retrieving user-agent-analyser from pool", t);
            for (Record record : records) {
                record.setStringField(FieldDictionary.RECORD_ERRORS, "Failure retrieving user-agent-analyser from pool");
            }
        } finally {
            if (pool != null && uaa != null) {
                pool.returnObject(uaa);
            }
        }

        if (debug) {
            getLogger().debug("User-Agent Processor records output: " + records);
        }
        return records;
    }

    private void processRecords(Collection<Record> records, UserAgentAnalyzer uaa) {
        for (Record record : records) {

            Field uaField = record.getField(userAgentField);
            if (uaField == null) {
                getLogger().debug("Skipping record. Field '" + userAgentField + "' does not exists in record");
                continue;
            }

            String recordValue = (String) uaField.getRawValue();


            UserAgent agent;
            try {
                agent = uaa.parse(recordValue);
            } catch (Throwable t) {
                record.setStringField(FieldDictionary.RECORD_ERRORS, "Failure in User-agent decoding");
                getLogger().error("Cannot parse User-Agent content: " + record, t);
                continue;
            }

            for (String field : selectedFields) {
                String value = agent.getValue(field);
                if (value != null && !value.isEmpty()) {
                    record.setStringField(field, value);
                }
                if (confidenceEnabled) {
                    record.setField(new Field(field + ".confidence", FieldType.LONG, agent.getConfidence(field)));
                }
            }

            if (ambiguityEnabled) {
                record.setField(new Field("ambiguity", FieldType.INT, agent.getAmbiguityCount()));
            }


            if (!userAgentKeep) {
                record.removeField(userAgentField);
            }
        }
    }

    // TODO :
//    @Override
//    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
//
//        logger.debug("property {} value changed from {} to {}", descriptor.getName(), oldValue, newValue);
//
//        /**
//         * Handle the debug property
//         */
//        if (descriptor.getName().equals(KEY_DEBUG))
//        {
//          if (newValue != null)
//          {
//              if (newValue.equalsIgnoreCase("true"))
//              {
//                  debug = true;
//              }
//          } else
//          {
//              debug = false;
//          }
//        }
//    }

}
