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

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import com.sun.jndi.toolkit.url.Uri;
import org.apache.http.client.utils.URIBuilder;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

@Tags({"record", "fields", "Decode"})
@CapabilityDescription("Decode one or more field containing an URI with possibly special chars encoded\n" +
        "...")
@ExtraDetailFile("./details/URLDecoder-Detail.rst")
public class URIDecoder extends AbstractProcessor {

    private final static String UTF8_CHARSET = "UTF-8";

    private static final PropertyDescriptor FIELDS_TO_DECODE_PROP = new PropertyDescriptor.Builder()
            .name("decode.fields")
            .description("List of fields (URL) to decode")
            .required(true)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor CHARSET_PROP = new PropertyDescriptor.Builder()
            .name("charset")
            .description("Charset to use to decode the URL")
            .required(true)
            .defaultValue(UTF8_CHARSET)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private Set<String> fieldsToDecode;
    private String charset;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(FIELDS_TO_DECODE_PROP);
        descriptors.add(CHARSET_PROP);
        return Collections.unmodifiableList(descriptors);
    }


    public void init(ProcessContext context) throws InitializationException {
        super.init(context);
        charset = context.getPropertyValue(CHARSET_PROP).asString();
        initFieldsToDecode(context);
    }

    public void initFieldsToDecode(ProcessContext context) {
        String commaSeparatedFields = context.getPropertyValue(FIELDS_TO_DECODE_PROP).asString();
        String[] fieldsArr = commaSeparatedFields.split("\\s*,\\s*");
        fieldsToDecode = new HashSet();
        Collections.addAll(fieldsToDecode, fieldsArr);
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        for (Record record : records) {
            updateRecord(record);
        }
        return records;
    }


    private void updateRecord(Record record) {
        fieldsToDecode.forEach(fieldName -> {
            if (record.hasField(fieldName)) {
                String uriStr = record.getField(fieldName).asString();
                if (uriStr != null && !uriStr.isEmpty()) {
                    decode(uriStr, record, fieldName);
                }
            }
        });
    }

    private void decode(String uriStr, Record record, String fieldNameToSetDecodedUri)
    {
        try {
            String decodedURI = uriToDecodedString(new URI(uriStr));
            if (!decodedURI.equals(uriStr)) {
                final FieldType fieldType = record.getField(fieldNameToSetDecodedUri).getType();
                record.removeField(fieldNameToSetDecodedUri);
                record.setField(fieldNameToSetDecodedUri, fieldType, decodedURI);
            }
        } catch (Exception e){
            getLogger().error("Error while trying to decode uri {}, for record {}.", new Object[]{uriStr, record.getId()}, e);
            String msg = "Could not process uri : '" + uriStr + "'.\n Cause: " + e.getMessage();
            record.addError(ProcessError.STRING_FORMAT_ERROR.toString(), getLogger(), msg);
        }
    }


    /**A URI is like
    [<scheme>:]<scheme-specific-part>[#<fragment>]
     @see URI
     */
    private String uriToDecodedString(URI uri) {
        String uriStr = "";
        if (uri.getScheme() != null && !uri.getScheme().isEmpty()) {
            uriStr += uri.getScheme() + ":";
        }
        if (uri.getSchemeSpecificPart() != null && !uri.getSchemeSpecificPart().isEmpty()) {
            uriStr += uri.getSchemeSpecificPart();
        }
        if (uri.getFragment() != null && !uri.getFragment().isEmpty()) {
            uriStr += "#" + uri.getFragment();
        }
        return uriStr;
    }
}
