/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.processor;

import com.google.common.collect.Lists;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

@Tags({"record", "id", "idempotent", "generate", "modify"})
@CapabilityDescription("modify id of records or generate it following defined rules")
public class ModifyId extends AbstractProcessor {

    private static final long serialVersionUID = -270933070438408174L;

    private static final Logger logger = LoggerFactory.getLogger(ModifyId.class);

    public static final AllowableValue GENERATE_RANDOM_UUID = new AllowableValue("randomUuid", "generate a random uid",
            "generate a randomUid using java library");

    public static final AllowableValue GENERATE_HASH = new AllowableValue("hashFields", "generate a hash from fields",
            "generate a hash from fields");


    public static final PropertyDescriptor STRATEGY = new PropertyDescriptor.Builder()
            .name("strategy.to.use")
            .description("the strategy to generate new Id")
            .required(true)
            .allowableValues(GENERATE_RANDOM_UUID, GENERATE_HASH)
            .defaultValue(GENERATE_RANDOM_UUID.getValue())
            .build();

    /**
     * properties sued only in case of Hash strategy
     */
    public static final PropertyDescriptor FIELDS_TO_USE_FOR_HASH = new PropertyDescriptor.Builder()
            .name("fields.to.use.for.hash")
            .description("the comma separated list of field names (e.g. \"policyid,date_raw\"")
            .required(true)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHARSET_TO_USE_FOR_HASH = new PropertyDescriptor.Builder()
            .name("charset.to.use.for.hash")
            .description("the charset to use to hash id string (e.g. \"UTF-8\"")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    public static final PropertyDescriptor HASH_ALGORITHM = new PropertyDescriptor.Builder()
            .name("algorithm.to.use.for.hash")
            .description("the algorithme to use to hash id string (e.g. \"SHA-256\"")
            .required(true)
            .addValidator(StandardValidators.HASH_ALGORITHM_VALIDATOR)
            .defaultValue("SHA-256")
            .build();


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        /**
         * set up strategy to build id
         */
        IdBuilder idBuilder = null;
        if (context.getPropertyValue(STRATEGY).isSet()) {
            if (context.getPropertyValue(STRATEGY).getRawValue().equals(GENERATE_RANDOM_UUID.getValue())) {
                idBuilder = new IdBuilder() {
                    @Override
                    public String buildId(Record record) {
                        return UUID.randomUUID().toString();
                    }
                };
            } else if (context.getPropertyValue(STRATEGY).getRawValue().equals(GENERATE_HASH.getValue())) {
                List<String> fieldsForHash = Lists.newArrayList(
                        context.getPropertyValue(FIELDS_TO_USE_FOR_HASH).asString().split(","));

                try {
                    final MessageDigest digest = MessageDigest.getInstance(context.getPropertyValue(HASH_ALGORITHM).asString());
                    final Charset charset = Charset.forName(context.getPropertyValue(CHARSET_TO_USE_FOR_HASH).asString());
                    idBuilder = new IdBuilder() {
                        @Override
                        public String buildId(Record record) {
                            StringBuilder stb = new StringBuilder();
                            for (String fieldName: fieldsForHash) {
                                stb.append(record.getField(fieldName));
                            }
                            digest.update(stb.toString().getBytes(charset));
                            byte[] digested = digest.digest();
                            return new String(digested, charset);
                        }
                    };
                } catch (NoSuchAlgorithmException e) {
                    throw new Error("This error should not happen because the validator should ensure the algorythme exist", e);
                }

            }
        }
        /**
        * build new id for all records
        */
        for (Record record : records) {
            String newId = idBuilder.buildId(record);
            record.setId(newId);
        }

        return records;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(STRATEGY);
        descriptors.add(FIELDS_TO_USE_FOR_HASH);
        descriptors.add(CHARSET_TO_USE_FOR_HASH);
        descriptors.add(HASH_ALGORITHM);

        return Collections.unmodifiableList(descriptors);
    }

    interface IdBuilder {
        String buildId(Record record);
    }
}
