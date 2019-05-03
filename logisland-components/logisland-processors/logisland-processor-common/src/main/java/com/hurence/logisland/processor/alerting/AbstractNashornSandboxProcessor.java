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
package com.hurence.logisland.processor.alerting;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.*;
import com.hurence.logisland.service.cache.CacheService;
import com.hurence.logisland.service.datastore.DatastoreClientService;
import com.hurence.logisland.validator.StandardValidators;
import delight.nashornsandbox.NashornSandbox;
import delight.nashornsandbox.NashornSandboxes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;

@DynamicProperty(name = "field to add",
        supportsExpressionLanguage = false,
        value = "a default value",
        description = "Add a field to the record with the default value")
public abstract class AbstractNashornSandboxProcessor extends AbstractProcessor {

    private static final Logger logger = LoggerFactory.getLogger(AbstractNashornSandboxProcessor.class);

    /**
     * Default storage for Nashorn js sandbox.
     */
    private static final Map<String, String> DEFAULT_JS_STORAGE = Collections.synchronizedMap(new HashMap<>());

    public static final PropertyDescriptor MAX_CPU_TIME = new PropertyDescriptor.Builder()
            .name("max.cpu.time")
            .description("maximum CPU time in milliseconds allowed for script execution.")
            .required(false)
            .defaultValue("100")
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_MEMORY = new PropertyDescriptor.Builder()
            .name("max.memory")
            .description("maximum memory in Bytes which JS executor thread can allocate")
            .required(false)
            .defaultValue("51200")
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    public static final PropertyDescriptor ALLOW_NO_BRACE = new PropertyDescriptor.Builder()
            .name("allow.no.brace")
            .description("Force, to check if all blocks are enclosed with curly braces \"\"{}\"\".\n" +
                    ".. raw:: html\n" +
                    "  <p>\n" +
                    "  Explanation: all loops (for, do-while, while, and if-else, and functions\n" +
                    "  should use braces, because poison_pill() function will be inserted after\n" +
                    "  each open brace \"\"{\"\", to ensure interruption checking. Otherwise simple\n" +
                    "  code like:\n" +
                    "  <pre>\n" +
                    "    while(true) while(true) {\n" +
                    "      // do nothing\n" +
                    "    }\n" +
                    "  </pre>\n" +
                    "  or even:\n" +
                    "  <pre>\n" +
                    "    while(true)\n" +
                    "  </pre>\n" +
                    "  cause unbreakable loop, which force this sandbox to use {@link Thread#stop()}\n" +
                    "  which make JVM unstable.\n" +
                    "  </p>\n" +
                    "  <p>\n" +
                    "  Properly writen code (even in bad intention) like:\n" +
                    "  <pre>\n" +
                    "    while(true) { while(true) {\n" +
                    "      // do nothing\n" +
                    "    }}\n" +
                    "  </pre>\n" +
                    "  will be changed into:\n" +
                    "  <pre>\n" +
                    "    while(true) {poison_pill(); \n" +
                    "      while(true) {poison_pill();\n" +
                    "        // do nothing\n" +
                    "      }\n" +
                    "    }\n" +
                    "  </pre>\n" +
                    "  which finish nicely when interrupted.\n" +
                    "  <p>\n" +
                    "  For legacy code, this check can be turned off, but with no guarantee, the\n" +
                    "  JS thread will gracefully finish when interrupted.\n" +
                    "  </p>")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_PREPARED_STATEMENTS = new PropertyDescriptor.Builder()
            .name("max.prepared.statements")
            .description("The size of prepared statements LRU cache. If 0, this is disabled.\n" +
                    ".. raw:: html\n" +
                    "  <p>\n" +
                    "  Each statements when setMaxCPUTime(long) is set is prepared to\n" +
                    "  quit itself when time exceeded. To execute only once this procedure per\n" +
                    "  statement set this value.\n" +
                    "  </p>\n" +
                    "  <p>\n" +
                    "  When setMaxCPUTime(long) is set 0, this value is ignored.\n" +
                    "  </p>\n")
            .required(false)
            .defaultValue("30")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATASTORE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("datastore.client.service")
            .description("The instance of the Controller Service to use for accessing datastore.")
            .required(true)
            .identifiesControllerService(DatastoreClientService.class)
            .build();


    public static final PropertyDescriptor DATASTORE_CACHE_COLLECTION = new PropertyDescriptor.Builder()
            .name("datastore.cache.collection")
            .description("The collection where to find cached objects")
            .required(false)
            .defaultValue("test")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor JS_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("js.cache.service")
            .description("The cache service to be used to store already sanitized JS expressions. " +
                    "If not specified a in-memory unlimited hash map will be used.")
            .required(false)
            .identifiesControllerService(CacheService.class)
            .build();


    public static final PropertyDescriptor OUTPUT_RECORD_TYPE = new PropertyDescriptor.Builder()
            .name("output.record.type")
            .description("the type of the output record")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(RecordDictionary.EVENT)
            .build();

    protected DatastoreClientService datastoreClientService;
    protected NashornSandbox sandbox;
    protected Map<String, String> dynamicTagValuesMap;
    protected String outputRecordType;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MAX_CPU_TIME);
        properties.add(MAX_MEMORY);
        properties.add(ALLOW_NO_BRACE);
        properties.add(MAX_PREPARED_STATEMENTS);
        properties.add(DATASTORE_CLIENT_SERVICE);
        properties.add(DATASTORE_CACHE_COLLECTION);
        properties.add(JS_CACHE_SERVICE);
        properties.add(OUTPUT_RECORD_TYPE);

        return properties;
    }


    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)
                .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
                .required(false)
                .dynamic(true)
                .build();
    }


    @Override
    public boolean hasControllerService() {
        return true;
    }


    abstract protected void setupDynamicProperties(ProcessContext context);

    @Override
    public void init(ProcessContext context) {

        super.init(context);
        sandbox = NashornSandboxes.create();

        CacheService<String, String> cacheService = PluginProxy.rewrap(context.getPropertyValue(JS_CACHE_SERVICE).asControllerService());

        //inject the right cache service (or the default one).
        if (cacheService != null) {
            sandbox.setScriptCache(((js, allowNoBraces, producer) -> {
                String ret = cacheService.get(js);
                if (ret == null) {
                    ret = producer.get();
                    cacheService.set(js, ret);
                }
                return ret;
            }));
        } else {
            sandbox.setScriptCache((js, allowNoBraces, producer) ->
                    DEFAULT_JS_STORAGE.computeIfAbsent(js, s -> producer.get()));
        }

        Long maxCpuTime = context.getPropertyValue(MAX_CPU_TIME).asLong();
        Long maxMemory = context.getPropertyValue(MAX_MEMORY).asLong();
        Boolean allowNoBrace = context.getPropertyValue(ALLOW_NO_BRACE).asBoolean();
        Integer maxPreparedStatements = context.getPropertyValue(MAX_PREPARED_STATEMENTS).asInteger();


        sandbox.setMaxCPUTime(maxCpuTime);
        sandbox.setMaxMemory(maxMemory);
        sandbox.allowNoBraces(allowNoBrace);
        sandbox.setMaxPreparedStatements(maxPreparedStatements); // because preparing scripts for execution is expensive
        sandbox.setExecutor(Executors.newSingleThreadExecutor());
        //TODO properly shutdown Executor service in stop method when framework will support it
        datastoreClientService = PluginProxy.rewrap(context.getPropertyValue(DATASTORE_CLIENT_SERVICE).asControllerService());
        if (datastoreClientService == null) {
            getLogger().error("Datastore client service is not initialized!");
        }

        sandbox.inject("cache", datastoreClientService);
        sandbox.allow(DatastoreClientService.class);
        sandbox.allow(Record.class);
        sandbox.allow(StandardRecord.class);
        sandbox.allow(FieldType.class);
        sandbox.allow(FieldDictionary.class);


        dynamicTagValuesMap = new HashMap<>();
        outputRecordType = context.getPropertyValue(OUTPUT_RECORD_TYPE).asString();

        this.setupDynamicProperties(context);


    }

}