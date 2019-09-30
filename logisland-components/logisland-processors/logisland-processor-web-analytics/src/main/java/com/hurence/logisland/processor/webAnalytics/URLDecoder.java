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
package com.hurence.logisland.processor.webAnalytics;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.*;

@Tags({"record", "fields", "Decode"})
@CapabilityDescription("Decode one or more field containing an URL with possibly special chars encoded\n" +
        "...")
@DynamicProperty(name = "fields to decode",
        supportsExpressionLanguage = false,
        value = "a default value",
        description = "Decode one or more fields from the record ")
@ExtraDetailFile("./details/URLDecoder-Detail.rst")
public class URLDecoder extends AbstractProcessor {

    private static final Logger logger = LoggerFactory.getLogger(URLDecoder.class);
    private static final String UTF8_CHARSET = "UTF-8";
    private final HashSet<String> fieldsToDecode = new HashSet();
    private final static String UTF8_PERCENT_ENCODED_CHAR = "%25";
    private String percentEncodedChar = UTF8_PERCENT_ENCODED_CHAR;

    private static final PropertyDescriptor FIELDS_TO_DECODE_PROP = new PropertyDescriptor.Builder()
            .name("decode.fields")
            .description("List of fields (URL) to decode")
            .required(true)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();

    private static final PropertyDescriptor CHARSET_PROP = new PropertyDescriptor.Builder()
            .name("charset")
            .description("Charset to use to decode the URL")
            .required(false)
            .defaultValue(UTF8_CHARSET)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(FIELDS_TO_DECODE_PROP);
        descriptors.add(CHARSET_PROP);
        return Collections.unmodifiableList(descriptors);
    }


    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(true)
                .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
                .required(false)
                .dynamic(true)
                .build();
    }

    public void init(ProcessContext context) throws InitializationException {
        super.init(context);
        String commaSeparatedFields = context.getPropertyValue(FIELDS_TO_DECODE_PROP).asString();
        String charset = context.getPropertyValue(CHARSET_PROP).asString();

        String[] fieldsArr = commaSeparatedFields.split("\\s*,\\s*");
        for (String field:fieldsArr
             ) {
            fieldsToDecode.add(field);
        }
        try {
            percentEncodedChar = java.net.URLEncoder.encode("%", charset);
        } catch (UnsupportedEncodingException e1) {
            percentEncodedChar=UTF8_PERCENT_ENCODED_CHAR; // Default to UTF-8 encoded char
            logger.warn(e1.toString());
        }
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        for (Record record : records) {
            updateRecord(context, record, fieldsToDecode);
        }
        return records;
    }


    private void updateRecord(ProcessContext context, Record record, HashSet<String> fields) {

        String charset = context.getPropertyValue(CHARSET_PROP).asString();
        if ((fields == null) || fields.isEmpty()) {
            return;
        }
        fields.forEach(fieldName -> {
            // field is already here
            if (record.hasField(fieldName)) {
                String value = record.getField(fieldName).asString();
                decode(value,charset,record,fieldName,true);
            }
        });
    }

    private void decode (String value, String charset, Record record, String fieldName, boolean tryTrick)
    {
        String decodedValue = null;
        try {
            decodedValue = java.net.URLDecoder.decode(value, charset);
            if (!decodedValue.equals(value)) {
                final FieldType fieldType = record.getField(fieldName).getType();
                record.removeField(fieldName);
                record.setField(fieldName, fieldType, decodedValue);
            }
        } catch (IllegalArgumentException e) {
            if (tryTrick) {
                value = value.replaceAll("%(?![0-9a-fA-F]{2})", percentEncodedChar);
                decode(value, charset, record, fieldName, false);
            }
            else {
                logger.warn(e.toString());
            }
        } catch (Exception e){
            logger.warn(e.toString());
        }
    }


}
