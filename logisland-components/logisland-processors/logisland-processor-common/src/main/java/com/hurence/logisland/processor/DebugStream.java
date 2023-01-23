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
package com.hurence.logisland.processor;

import com.hurence.logisland.annotation.documentation.*;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.serializer.*;
import com.hurence.logisland.validator.StandardValidators;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Category(ComponentCategory.UTILS)
@Tags({"record", "debug"})
@CapabilityDescription("This is a processor that logs incoming records." +
                       " All records are printed out by default unless a filter is set (see debug.filter)." +
                       " Also all fields are printed out by default unless a selection is set (see debug.selection).")
@ExtraDetailFile("./details/common-processors/DebugStream-Detail.rst")
public class DebugStream extends AbstractProcessor {

    public static final AllowableValue NO_DESERIALIZER =
        new AllowableValue("none", "no deserialization", "get body as raw string");

    public static final AllowableValue AVRO_DESERIALIZER =
            new AllowableValue(AvroSerializer.class.getName(), "avro deserialization", "deserialize body as avro blocs");

    public static final AllowableValue JSON_DESERIALIZER =
            new AllowableValue(JsonSerializer.class.getName(), "json deserialization", "deserialize body as json blocs");

    public static final AllowableValue EXTENDED_JSON_DESERIALIZER =
            new AllowableValue(ExtendedJsonSerializer.class.getName(), "extended json deserialization", "deserialize body as json blocs");

    public static final AllowableValue KRYO_DESERIALIZER =
            new AllowableValue(KryoSerializer.class.getName(), "kryo deserialization", "deserialize body with kryo");

    public static final PropertyDescriptor SERIALIZER = new PropertyDescriptor.Builder()
            .name("event.serializer")
            .description("the serializer needed for loading the payload and handling it as a record set.")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(KRYO_DESERIALIZER, JSON_DESERIALIZER, AVRO_DESERIALIZER, NO_DESERIALIZER, EXTENDED_JSON_DESERIALIZER)
            .defaultValue(EXTENDED_JSON_DESERIALIZER.getValue())
            .build();

    static final PropertyDescriptor RECORD_SCHEMA = new PropertyDescriptor.Builder()
            .name("event.serializer.schema")
            .description("the schema definition for the deserializer (for response payload). You can limit data to retrieve this way")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor DEBUG_FILTER = new PropertyDescriptor.Builder()
            .name("debug.filter")
            .description("A filter that will look for the provided key with the specified value: <key>:<regexp>. Eg")
            .required(false)
            .addValidator(StandardValidators.FILTER_REGEXP_VALIDATOR)
            .build();

    static final PropertyDescriptor DEBUG_FILTER_SELECTION = new PropertyDescriptor.Builder()
            .name("debug.selection")
            .description("A comma separated fieldnames to narrow the number of fields printed out. Note that fieldnames are trimmed.")
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SERIALIZER);
        descriptors.add(RECORD_SCHEMA);
        descriptors.add(DEBUG_FILTER);
        descriptors.add(DEBUG_FILTER_SELECTION);
        return Collections.unmodifiableList(descriptors);
    }

    private volatile MemoryMXBean memBean;
    private volatile RecordSerializer serializer;
    private volatile String key;
    private volatile Pattern filter;
    private volatile Collection<String> selection;

    @Override
    public void init(ProcessContext context)  throws InitializationException {
        super.init(context);
        if (memBean == null) {
            memBean = ManagementFactory.getMemoryMXBean();
        }
        if (context.getPropertyValue(RECORD_SCHEMA).isSet()) {
            serializer = SerializerProvider.getSerializer(
                    context.getPropertyValue(SERIALIZER).asString(),
                    context.getPropertyValue(RECORD_SCHEMA).asString());
        } else {
            String serializerCanonicName = context.getPropertyValue(SERIALIZER).asString();
            if (!serializerCanonicName.equals(NO_DESERIALIZER.getValue())) {
                serializer = SerializerProvider.getSerializer(context.getPropertyValue(SERIALIZER).asString(), null);
            } else {
                serializer = new StringSerializer();
            }
        }

        /*
         * Supports filtering to avoid all records to be fully dumped.
         * For eg.
         *     debug.filter: Userid:8822736715780
         *     debug.selection: Class,codeProduct
         * will dump only records with field 'Userid=8822736715780' and will only print out fields 'Class,codeProduct'.
         */
        if (context.getPropertyValue(DEBUG_FILTER).isSet()) {
            final String filter = context.getPropertyValue(DEBUG_FILTER).asString();
            try {
                final int index = filter.indexOf(':');
                this.key = filter.substring(0, index);
                final String regexp = filter.substring(index+1);
                this.filter = Pattern.compile(regexp);

                if ( getLogger().isTraceEnabled() ) {
                    getLogger().trace("Successfully setup filter for DebugStream with => key='" + this.key + "' and regexp='" + regexp + "'");
                }
            }
            catch (final Exception e) {
                throw new RuntimeException("Invalid filter for '"+filter+"'. Was expecting 'key:<regexp>'", e);
            }
        }
        if (context.getPropertyValue(DEBUG_FILTER_SELECTION).isSet()) {
            final String selection = context.getPropertyValue(DEBUG_FILTER_SELECTION).asString();
            try {
                this.selection = Arrays.stream(selection.split(","))
                                       .map(String::trim)
                                       .collect(Collectors.toList());

                if ( getLogger().isTraceEnabled() ) {
                    getLogger().trace("Successfully setup filter for DebugStream with => selection=" + this.selection);
                }
            }
            catch (final Exception e) {
                throw new RuntimeException("Invalid selection for '"+selection+"'. Was expecting '<field>[,<field>]'", e);
            }
        }
    }

    @Override
    public Collection<Record> process(final ProcessContext context, final Collection<Record> collection) {

        getLogger().info("processing {} records", new Object[]{collection.size()});

        if (collection.size() != 0) {
            //Do not use serialization ! It is pointless as at this point the object is already deserialized into a Record !
            //Moreover trying to serialize the record may fail for a lot of reason (if record contains some objects without appropriate bean pattern.
            for(Record record: collection) {
                boolean log = true; // log by default if no filter. If a filter is defined then log if the key matches the regular expression.
                if ( this.filter!=null ) {
                    // If a filter is defined check if the field to check is present in the record.
                    final Field field = record.getField(this.key);
                    if ( field != null ) {
                        final String value = field.asString();
                        if ( value!=null ) {
                            // Check if the fields matches the regexp.
                            log = this.filter.matcher(value).find();
                        }
                    }
                }
                if ( log ) {
                    if ( this.selection!=null ) {
                        // Prints out only select fields.
                        final Map<String, Field> fields = record.getAllFields().stream()
                                                                .filter(f -> this.selection.contains(f.getName()))
                                                                .collect(Collectors.toMap(Field::getName,Function.identity()));
                        record = new StandardRecord();
                        record.addFields(fields);
                    }
                    getLogger().info(record.toString(1));
                }
            }
        }

        return collection;
    }


}
