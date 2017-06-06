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

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.SeeAlso;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.time.DateUtil;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@Tags({"parser", "regex", "log", "record"})
@CapabilityDescription("This is a processor that is used to split a String into fields according to a given Record mapping")
@SeeAlso(value = {SplitTextMultiline.class}, classNames = {"com.hurence.logisland.processor.SplitTextMultiline"})
@DynamicProperty(name = "alternative regex & mapping",
        supportsExpressionLanguage = true,
        value = "another regex that could match",
        description = "this regex will be tried if the main one has not matched. " +
                "It must be in the form alt.value.regex.1 and alt.value.fields.1")
public class SplitText extends AbstractProcessor {

    static final long serialVersionUID = 1413578915552852739L;

    private static Logger logger = LoggerFactory.getLogger(SplitText.class);


    public static final PropertyDescriptor VALUE_REGEX = new PropertyDescriptor.Builder()
            .name("value.regex")
            .description("the regex to match for the message value")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor VALUE_FIELDS = new PropertyDescriptor.Builder()
            .name("value.fields")
            .description("a comma separated list of fields corresponding to matching groups for the message value")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_REGEX = new PropertyDescriptor.Builder()
            .name("key.regex")
            .description("the regex to match for the message key")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(".*")
            .build();

    public static final PropertyDescriptor KEY_FIELDS = new PropertyDescriptor.Builder()
            .name("key.fields")
            .description("a comma separated list of fields corresponding to matching groups for the message key")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .defaultValue(FieldDictionary.RECORD_RAW_KEY)
            .build();

    public static final PropertyDescriptor RECORD_TYPE = new PropertyDescriptor.Builder()
            .name("record.type")
            .description("default type of record")
            .required(false)
            .defaultValue("record")
            .build();

    public static final PropertyDescriptor KEEP_RAW_CONTENT = new PropertyDescriptor.Builder()
            .name("keep.raw.content")
            .description("do we add the initial raw content ?")
            .required(false)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor TIME_ZONE_RECORD_TIME = new PropertyDescriptor.Builder()
            .name("timezone.record.time")
            .description("what is the time zone of the string formatted date for 'record_time' field.")
            .required(false)
            .defaultValue("UTC")
            .addValidator(StandardValidators.TIMEZONE_VALIDATOR)
            .build();

    private class AlternativeMappingPattern{

        private String[] mapping;
        private Pattern pattern;

        AlternativeMappingPattern(String mapping,Pattern pattern) {
            this.mapping = mapping.split(",");
            this.pattern = pattern;
        }

        public String[] getMapping() {
            return mapping;
        }

        public Pattern getPattern() {
            return pattern;
        }
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(VALUE_REGEX);
        descriptors.add(VALUE_FIELDS);
        descriptors.add(KEY_REGEX);
        descriptors.add(KEY_FIELDS);
        descriptors.add(RECORD_TYPE);
        descriptors.add(KEEP_RAW_CONTENT);
        descriptors.add(TIME_ZONE_RECORD_TIME);

        return Collections.unmodifiableList(descriptors);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));

        // key regex and fields must be set together
        if (context.getPropertyValue(KEY_REGEX).isSet() ^ context.getPropertyValue(KEY_FIELDS).isSet()) {
            validationResults.add(
                    new ValidationResult.Builder()
                            .input(KEY_REGEX.getName())
                            .explanation("key regex and fields must be set together")
                            .valid(false)
                            .build());
        }
      /*  final String methodValue = context.getProperty(ENCRYPTION_ALGORITHM).getValue();
        final EncryptionMethod encryptionMethod = EncryptionMethod.valueOf(methodValue);
        final String algorithm = encryptionMethod.getAlgorithm();
        final String password = context.getProperty(PASSWORD).getValue();
        final KeyDerivationFunction kdf = KeyDerivationFunction.valueOf(context.getProperty(KEY_DERIVATION_FUNCTION).getValue());
        final String keyHex = context.getProperty(RAW_KEY_HEX).getValue();
        if (isPGPAlgorithm(algorithm)) {
            final boolean encrypt = context.getProperty(MODE).getValue().equalsIgnoreCase(ENCRYPT_MODE);
            final String publicKeyring = context.getProperty(PUBLIC_KEYRING).getValue();
            final String publicUserId = context.getProperty(PUBLIC_KEY_USERID).getValue();
            final String privateKeyring = context.getProperty(PRIVATE_KEYRING).getValue();
            final String privateKeyringPassphrase = context.getProperty(PRIVATE_KEYRING_PASSPHRASE).getValue();
            validationResults.addAll(validatePGP(encryptionMethod, password, encrypt, publicKeyring, publicUserId, privateKeyring, privateKeyringPassphrase));
        } else { // Not PGP
            if (encryptionMethod.isKeyedCipher()) { // Raw key
                validationResults.addAll(validateKeyed(encryptionMethod, kdf, keyHex));
            } else { // PBE
                boolean allowWeakCrypto = context.getProperty(ALLOW_WEAK_CRYPTO).getValue().equalsIgnoreCase(WEAK_CRYPTO_ALLOWED_NAME);
                validationResults.addAll(validatePBE(encryptionMethod, kdf, password, allowWeakCrypto));
            }
        }*/
        return validationResults;
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        final String[] keyFields = context.getPropertyValue(KEY_FIELDS).asString().split(",");
        final String keyRegexString = context.getPropertyValue(KEY_REGEX).asString();
        final Pattern keyRegex = Pattern.compile(keyRegexString);
        final String[] valueFields = context.getPropertyValue(VALUE_FIELDS).asString().split(",");
        final String valueRegexString = context.getPropertyValue(VALUE_REGEX).asString();
        final String eventType = context.getPropertyValue(RECORD_TYPE).asString();
        final boolean keepRawContent = context.getPropertyValue(KEEP_RAW_CONTENT).asBoolean();
        final Pattern valueRegex = Pattern.compile(valueRegexString);



        /**
         * try to match the regexp to create an event
         */
        List<Record> outputRecords = new ArrayList<>();
        records.forEach(record -> {
            try {
                final String key = record.getField(FieldDictionary.RECORD_KEY).asString();
                final String value = record.getField(FieldDictionary.RECORD_VALUE).asString();

                StandardRecord outputRecord = new StandardRecord(eventType);

                // match the key
                if (key != null && !key.isEmpty()) {
                    try {
                        Matcher keyMatcher = keyRegex.matcher(key);
                        if (keyMatcher.matches()) {

                            if (keepRawContent) {
                                outputRecord.setField(FieldDictionary.RECORD_RAW_KEY, FieldType.STRING, keyMatcher.group(0));
                            }
                            for (int i = 0; i < keyMatcher.groupCount() + 1 && i < keyFields.length; i++) {
                                String content = keyMatcher.group(i);
                                if (content != null) {
                                    outputRecord.setField(keyFields[i], FieldType.STRING, keyMatcher.group(i + 1).replaceAll("\"", ""));
                                }
                            }
                        } else {
                            outputRecord.setField(FieldDictionary.RECORD_RAW_KEY, FieldType.STRING, key);
                        }
                    } catch (Exception e) {
                        String errorMessage = "error while matching key " + key +
                                " with regex " + keyRegexString +
                                " : " + e.getMessage();
                        logger.warn(errorMessage);
                        outputRecord.addError(ProcessError.REGEX_MATCHING_ERROR.getName(), errorMessage);
                        outputRecord.setField(FieldDictionary.RECORD_RAW_KEY, FieldType.STRING, value);
                    }
                }
                /**
                 * initializing timezone
                 */
                TimeZone timezone = null;
                if (context.getPropertyValue(TIME_ZONE_RECORD_TIME).isSet()) {
                    timezone = TimeZone.getTimeZone(context.getPropertyValue(TIME_ZONE_RECORD_TIME).asString());
                } else {
                    timezone = TimeZone.getTimeZone("UTC");
                }

                // match the value
                if (value != null && !value.isEmpty()) {
                    try {
                        Matcher valueMatcher = valueRegex.matcher(value);
                        if (valueMatcher.lookingAt()) {
                            extractValueFields(valueFields, keepRawContent, outputRecord, valueMatcher, timezone);
                        } else {
                            // try the other Regex
                            List<AlternativeMappingPattern> alternativeRegexList =
                                    getAlternativePatterns(context, valueRegexString);
                            boolean hasMatched = false;
                            for (AlternativeMappingPattern alternativeMatchingRegex : alternativeRegexList) {
                                Matcher alternativeValueMatcher = alternativeMatchingRegex.getPattern().matcher(value);
                                if (alternativeValueMatcher.lookingAt()) {
                                    extractValueFields(
                                            alternativeMatchingRegex.getMapping(),
                                            keepRawContent,
                                            outputRecord,
                                            alternativeValueMatcher, timezone);
                                    hasMatched = true;
                                    break;
                                }
                            }
                            // if we don't have any matches output an error
                            if (!hasMatched) {
                                outputRecord.addError(ProcessError.REGEX_MATCHING_ERROR.getName(), "check your conf");
                                outputRecord.setField(FieldDictionary.RECORD_RAW_VALUE, FieldType.STRING, value);
                            }

                        }

                    } catch (Exception e) {
                        String errorMessage = "error while matching value " + value +
                                " with regex " + valueRegexString +
                                " : " + e.getMessage();
                        logger.warn(errorMessage);
                        outputRecord.addError(ProcessError.REGEX_MATCHING_ERROR.getName(), errorMessage);
                        outputRecord.setField(FieldDictionary.RECORD_RAW_VALUE, FieldType.STRING, value);
                    } finally {
                        outputRecords.add(outputRecord);
                    }
                }
            } catch (Exception e) {
                // nothing to do here

                logger.warn("issue while matching getting K/V on record {}, exception {}", record, e.getMessage());
            }
        });

        return outputRecords;
    }

    private List<AlternativeMappingPattern> getAlternativePatterns(ProcessContext context, String valueRegexString) {
        /**
         * list alternative regex
         */
        List<AlternativeMappingPattern> alternativeMappingRegex = new ArrayList<>();
        // loop over dynamic properties to add alternative regex
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            if(!entry.getKey().getName().toLowerCase().contains("alt.value.fields")){
                continue;
            }
            final String patternPropertyKey = entry.getKey().getName().toLowerCase().replace("fields", "regex");

            if(context.getPropertyValue(patternPropertyKey) != null){
                final String alternativeRegexString = context.getPropertyValue(patternPropertyKey).asString();
                final String mapping = entry.getValue();
                final Pattern pattern = Pattern.compile(alternativeRegexString);
                alternativeMappingRegex.add(new AlternativeMappingPattern(mapping, pattern));
            }

        }
        return alternativeMappingRegex;
    }

    private void extractValueFields(String[] valueFields, boolean keepRawContent, StandardRecord outputRecord, Matcher valueMatcher,
                                    TimeZone timezone) {
        if (keepRawContent) {
            outputRecord.setField(FieldDictionary.RECORD_RAW_VALUE, FieldType.STRING, valueMatcher.group(0));
        }
        for (int i = 0; i < Math.min(valueMatcher.groupCount() + 1, valueFields.length); i++) {
            String content = valueMatcher.group(i + 1);
            String fieldName = valueFields[i];
            if (content != null) {
                outputRecord.setStringField(fieldName, content.replaceAll("\"", ""));
            }
        }


        // TODO removeField this ugly stuff with EL
        if (outputRecord.getField(FieldDictionary.RECORD_TIME) != null) {

            try {
                long eventTime = Long.parseLong(outputRecord.getField(FieldDictionary.RECORD_TIME).getRawValue().toString());
            } catch (Exception ex) {
                Date eventDate = null;
                try {
                    eventDate = DateUtil.parse(outputRecord.getField(FieldDictionary.RECORD_TIME).getRawValue().toString(), timezone);
                } catch (ParseException e) {
                    logger.info("issue while parsing date : {} ", e.toString());
                }
                if (eventDate != null) {
                    outputRecord.setField(FieldDictionary.RECORD_TIME, FieldType.LONG, eventDate.getTime());
                }
            }
        }
    }


}
