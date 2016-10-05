package com.hurence.logisland.processor;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.ValidationContext;
import com.hurence.logisland.component.ValidationResult;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.time.DateUtil;
import com.hurence.logisland.util.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


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
            .defaultValue("")
            .build();

    public static final PropertyDescriptor EVENT_TYPE = new PropertyDescriptor.Builder()
            .name("event.type")
            .description("default type of event")
            .required(false)
            .defaultValue("event")
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(VALUE_REGEX);
        descriptors.add(VALUE_FIELDS);
        descriptors.add(KEY_REGEX);
        descriptors.add(KEY_FIELDS);
        descriptors.add(EVENT_TYPE);

        return Collections.unmodifiableList(descriptors);
    }


    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));

        // key regex and fields must be set together
        if (context.getProperty(KEY_REGEX).isSet() ^ context.getProperty(KEY_FIELDS).isSet()) {
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

        final String[] keyFields = context.getProperty(KEY_FIELDS).asString().split(",");
        final String keyRegexString = context.getProperty(KEY_REGEX).asString();
        final Pattern keyRegex = Pattern.compile(keyRegexString);
        final String[] valueFields = context.getProperty(VALUE_FIELDS).asString().split(",");
        final String valueRegexString = context.getProperty(VALUE_REGEX).asString();
        final String eventType = context.getProperty(EVENT_TYPE).asString();
        final Pattern valueRegex = Pattern.compile(valueRegexString);

        List<Record> outputRecords = new ArrayList<>();

        /**
         * try to match the regexp to create an event
         */
        records.forEach(record -> {
            try {
                final String key = record.getField(FieldDictionary.RECORD_KEY).asString();
                final String value = record.getField(FieldDictionary.RECORD_VALUE).asString();

                StandardRecord outputRecord = new StandardRecord(eventType);

                // match the key
                if (key != null) {
                    try {
                        Matcher keyMatcher = keyRegex.matcher(key);
                        if (keyMatcher.matches()) {
                            for (int i = 0; i < keyMatcher.groupCount() + 1 && i < keyFields.length; i++) {
                                String content = keyMatcher.group(i);
                                if (content != null) {
                                    outputRecord.setField(keyFields[i], FieldType.STRING, keyMatcher.group(i + 1).replaceAll("\"", ""));

                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.info("error while matching key {} with regex {}", key, keyRegexString);
                    }
                }


                // match the value
                if (value != null) {
                    try {
                        Matcher valueMatcher = valueRegex.matcher(value);
                        if (valueMatcher.lookingAt()) {
                            for (int i = 0; i < valueMatcher.groupCount() + 1 && i < valueFields.length; i++) {
                                String content = valueMatcher.group(i);
                                if (content != null) {
                                    outputRecord.setField(valueFields[i], FieldType.STRING, valueMatcher.group(i).replaceAll("\"", ""));
                                }
                            }


                            // TODO removeField this ugly stuff with EL
                            if (outputRecord.getField("date") != null && outputRecord.getField("time") != null) {
                                String eventTimeString = outputRecord.getField("date").asString() +
                                        " " +
                                        outputRecord.getField("time").asString();

                                try {
                                    Date eventDate = DateUtil.parse(eventTimeString);

                                    if (eventDate != null) {
                                        outputRecord.setField(FieldDictionary.RECORD_TIME, FieldType.LONG, eventDate.getTime());
                                    }
                                } catch (Exception e) {
                                    logger.warn("unable to parse date {}", eventTimeString);
                                }

                            }


                            // TODO removeField this ugly stuff with EL
                            if (outputRecord.getField(FieldDictionary.RECORD_TIME) != null) {

                                try {
                                    long eventTime = Long.parseLong(outputRecord.getField(FieldDictionary.RECORD_TIME).getRawValue().toString());
                                } catch (Exception ex) {

                                    Date eventDate = DateUtil.parse(outputRecord.getField(FieldDictionary.RECORD_TIME).getRawValue().toString());
                                    if (eventDate != null) {
                                        outputRecord.setField(FieldDictionary.RECORD_TIME, FieldType.LONG, eventDate.getTime());
                                    }
                                }
                            }


                            outputRecords.add(outputRecord);
                        }
                    } catch (Exception e) {
                        logger.warn("issue while matching regex {} on string {} exception {}", valueRegexString, value, e.getMessage());
                    }
                }
            } catch (Exception e) {
                // nothing to do here

                logger.warn("issue while matching getting K/V on record {}, exception {}", record, e.getMessage());
            }
        });


        return outputRecords;
    }


}
