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
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

@Tags({"record", "id", "idempotent", "generate", "modify"})
@CapabilityDescription("modify id of records or generate it following defined rules")
//TODO add others tags see others processor
public class ModifyId extends AbstractProcessor {

    private static final long serialVersionUID = -270933070438408174L;

    private static final Logger logger = LoggerFactory.getLogger(ModifyId.class);

    public static final AllowableValue RANDOM_UUID_STRATEGY = new AllowableValue("randomUuid", "generate a random uid",
            "generate a randomUid using java library");

    public static final AllowableValue HASH_FIELDS_STRATEGY = new AllowableValue("hashFields", "generate a hash from fields",
            "generate a hash from fields");

    public static final AllowableValue JAVA_FORMAT_STRING_WITH_FIELDS_STRATEGY = new AllowableValue("fromFields", "generate a string from java pattern and fields",
            "generate a string from java pattern and fields");

    public static final AllowableValue TYPE_TIME_HASH_STRATEGY = new AllowableValue("typetimehash", "generate a concatenation of type, time and a hash from fields",
            "generate a concatenation of type, time and a hash from fields (as for generate_hash strategy)");




    public static final PropertyDescriptor STRATEGY = new PropertyDescriptor.Builder()
            .name("id.generation.strategy")
            .description("the strategy to generate new Id")
            .required(true)
            .allowableValues(RANDOM_UUID_STRATEGY, HASH_FIELDS_STRATEGY, JAVA_FORMAT_STRING_WITH_FIELDS_STRATEGY, TYPE_TIME_HASH_STRATEGY)
            .defaultValue(RANDOM_UUID_STRATEGY.getValue())
            .build();



    /**
     * properties sued only in case of Hash strategy
     */
    public static final PropertyDescriptor CHARSET_TO_USE_FOR_HASH = new PropertyDescriptor.Builder()
            .name("hash.charset")
            .description("the charset to use to hash id string (e.g. 'UTF-8')")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    /**
     * properties sued only in case of Format strategy
     */
    public static final PropertyDescriptor JAVA_FORMAT_STRING = new PropertyDescriptor.Builder()
            .name("java.formatter.string")
            .description("the format to use to build id string (e.g. '%4$2s %3$2s %2$2s %1$2s' (see java Formatter)")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor LANGUAGE_TAG = new PropertyDescriptor.Builder()
            .name("language.tag")
            .description("the language to use to format numbers in string")
            .required(true)
            .addValidator(StandardValidators.LANGUAGE_TAG_VALIDATOR)
            .allowableValues(Locale.getISOLanguages())
            .defaultValue(Locale.ENGLISH.toLanguageTag())
            .build();

    /**
     * properties sued only in case of Hash strategy or Format strategy
     */
    public static final PropertyDescriptor FIELDS_TO_USE = new PropertyDescriptor.Builder()
            .name("fields.to.hash")
            .description("the comma separated list of field names (e.g. : 'policyid,date_raw'")
            .required(true)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(FieldDictionary.RECORD_RAW_VALUE)
            .build();

    //TODO determines those values dynamically, used this code to determine those
    //    Provider[] providers = Security.getProviders();
    //    for (Provider p : providers) {
    //        String providerStr = String.format("%s/%s/%f\n", p.getName(),
    //                p.getInfo(), p.getVersion());
    //        System.out.println("provider: " + p.getName());
    //        Set<Provider.Service> services = p.getServices();
    //        for (Provider.Service s : services) {
    //            if ("MessageDigest".equals(s.getType())) {
    //                System.out.printf("\t%s//%s//%s\n", s.getType(),
    //                        s.getAlgorithm(), s.getClassName());
    //            }
    //        }
    //    }
    public static final Set<String> HASH_ALGORITHMS = new HashSet<>(Arrays.asList(
            "MD2", "MD5", "SHA", "SHA-224", "SHA-256", "SHA-384", "SHA-512"
    ));

    public static final PropertyDescriptor HASH_ALGORITHM = new PropertyDescriptor.Builder()
            .name("hash.algorithm")
            .description("the algorithme to use to hash id string (e.g. 'SHA-256'")
            .required(true)
            .allowableValues(HASH_ALGORITHMS)
            .addValidator(StandardValidators.HASH_ALGORITHM_VALIDATOR)
            .defaultValue("SHA-256")
            .build();


    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));
        if (context.getPropertyValue(STRATEGY).isSet()) {
            if (context.getPropertyValue(STRATEGY).getRawValue().equals(JAVA_FORMAT_STRING_WITH_FIELDS_STRATEGY.getValue())) {
                if (!context.getPropertyValue(JAVA_FORMAT_STRING).isSet()) {
                    validationResults.add(
                            new ValidationResult.Builder()
                                    .input(JAVA_FORMAT_STRING.getName())
                                    .explanation(String.format("%s must be set when strategy is %s",
                                            JAVA_FORMAT_STRING.getName(),
                                            context.getPropertyValue(STRATEGY).getRawValue()))
                                    .valid(false)
                                    .build());
                }
            }
        }
        return validationResults;
    }

    private IdBuilder idBuilder = null;

    @Override
    public void init(ProcessContext context) {
        if (context.getPropertyValue(STRATEGY).isSet()) {
            if (context.getPropertyValue(STRATEGY).getRawValue().equals(RANDOM_UUID_STRATEGY.getValue())) {
                idBuilder = new IdBuilder() {
                    @Override
                    public void buildId(Record record) {
                        record.setId(UUID.randomUUID().toString());
                    }
                };
            } else if (context.getPropertyValue(STRATEGY).getRawValue().equals(HASH_FIELDS_STRATEGY.getValue())) {
                final List<String> fieldsForHash = Lists.newArrayList(
                        context.getPropertyValue(FIELDS_TO_USE).asString().split(","));

                try {
                    final MessageDigest digest = MessageDigest.getInstance(context.getPropertyValue(HASH_ALGORITHM).asString());
                    final Charset charset = Charset.forName(context.getPropertyValue(CHARSET_TO_USE_FOR_HASH).asString());
                    idBuilder = new IdBuilder() {
                        @Override
                        public void buildId(Record record) {
                            StringBuilder stb = new StringBuilder();
                            for (String fieldName: fieldsForHash) {
                                stb.append(record.getField(fieldName).asString());
                            }
                            digest.update(stb.toString().getBytes(charset));
                            byte[] digested = digest.digest();
                            record.setId(new String(digested, charset));
                        }
                    };
                } catch (NoSuchAlgorithmException e) {
                    throw new Error("This error should not happen because the validator should ensure the algorythme exist", e);
                }
            } else if (context.getPropertyValue(STRATEGY).getRawValue().equals(JAVA_FORMAT_STRING_WITH_FIELDS_STRATEGY.getValue())) {
                final String[] fieldsForFormat = context.getPropertyValue(FIELDS_TO_USE).asString().split(",");
                final String format = context.getPropertyValue(JAVA_FORMAT_STRING).asString();
                final Locale local = Locale.forLanguageTag(context.getPropertyValue(LANGUAGE_TAG).asString());
                idBuilder = new IdBuilder() {
                    @Override
                    public void buildId(Record record) {
                        final Object[] valuesForFormat = new Object[fieldsForFormat.length];
                        for (int i=0; i < valuesForFormat.length; i++) {
                            if (!record.hasField(fieldsForFormat[i])) {
                                List<String> fieldsName =  Lists.newArrayList(fieldsForFormat);
                                record.addError(ProcessError.CONFIG_SETTING_ERROR.getName(),
                                        String.format("could not build id with format : '%s' \nfields: '%s' \n because "+
                                                "field: '%s' does not exist", format, fieldsName, fieldsForFormat[i]));
                                return;
                            }
                            valuesForFormat[i] = record.getField(fieldsForFormat[i]).getRawValue();
                        }
                        try {
                            record.setId(String.format(local, format, valuesForFormat));
                        } catch (IllegalFormatException e) {
                            // If a format string contains an illegal syntax, a format specifier that is incompatible with the given arguments,
                            // insufficient arguments given the format string, or other illegal conditions.
                            // For specification of all possible formatting errors, see the Details section of the formatter class specification.
                            record.addError(ProcessError.STRING_FORMAT_ERROR.getName(), e.getMessage());
                        } catch (NullPointerException  e) {//should not happen
                            record.addError(ProcessError.CONFIG_SETTING_ERROR.getName(), e.getMessage());
                        }
                    }
                };
            } else if (context.getPropertyValue(STRATEGY).getRawValue().equals(TYPE_TIME_HASH_STRATEGY.getValue())) {
                final List<String> fieldsForHash = Lists.newArrayList(
                        context.getPropertyValue(FIELDS_TO_USE).asString().split(","));
                try {
                    final MessageDigest digest = MessageDigest.getInstance(context.getPropertyValue(HASH_ALGORITHM).asString());
                    final Charset charset = Charset.forName(context.getPropertyValue(CHARSET_TO_USE_FOR_HASH).asString());
                    idBuilder = new IdBuilder() {
                        @Override
                        public void buildId(Record record) {
                            StringBuilder stb = new StringBuilder();
                            for (String fieldName: fieldsForHash) {
                                stb.append(record.getField(fieldName).asString());
                            }
                            digest.update(stb.toString().getBytes(charset));
                            byte[] digested = digest.digest();
                            final String hashString = new String(digested, charset);
                            final String recordType = record.getField(FieldDictionary.RECORD_TYPE).asString();
                            final String recordTime = record.getField(FieldDictionary.RECORD_TIME).asString();
                            final String newId = String.format("%s-%s-%s", recordType, recordTime, hashString);
                            record.setId(newId);
                        }
                    };
                } catch (NoSuchAlgorithmException e) {
                    throw new Error("This error should not happen because the validator should ensure the algorythme exist", e);
                }
            }
        }
    }


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        /**
         * set up strategy to build id
         */
        try {
            init(context);
        } catch (Throwable t) {
            logger.error("error while initializing idBuilder", t);
        }

        /**
         * build new id for all records
         */
        try {
            for (Record record : records) {
                idBuilder.buildId(record);
            }
        } catch (Throwable t) {
            logger.error("error while setting id for records", t);
        }
        return records;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(STRATEGY);
        descriptors.add(FIELDS_TO_USE);
        descriptors.add(CHARSET_TO_USE_FOR_HASH);
        descriptors.add(HASH_ALGORITHM);
        descriptors.add(JAVA_FORMAT_STRING);
        descriptors.add(LANGUAGE_TAG);

        return Collections.unmodifiableList(descriptors);
    }

    interface IdBuilder {
        void buildId(Record record);
    }
}
