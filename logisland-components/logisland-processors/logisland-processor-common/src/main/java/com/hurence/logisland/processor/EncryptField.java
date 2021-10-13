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
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.encryption.*;

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.Configuration;
import com.hurence.logisland.validator.ValidationResult;


import javax.crypto.NoSuchPaddingException;
import java.io.*;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.*;
@ExtraDetailFile("./details/common-processors/EncryptField-Detail.rst")
@CapabilityDescription("This is a processor that is used to encrypt or decrypt one or many fields of any type of a given Record mapping")
@DynamicProperty(name = "field to encrypt",
        supportsExpressionLanguage = true,
        value = "a default value",
        description = "encrypt the field value")
public class EncryptField extends AbstractProcessor {


    private static final long serialVersionUID = -270933070438408174L;

    public static final String ENCRYPT_MODE = "Encrypt";
    public static final String DECRYPT_MODE = "Decrypt";
    public static final String AES = "AES";
    public static final String AES_CBC_PKCS5 = "AES/CBC/PKCS5Padding";
    public static final String AES_CBC_NoPAD = "AES/CBC/NoPadding";
    public static final String AES_ECB_NoPAD = "AES/ECB/NoPadding";
    public static final String AES_ECB_PKCS5 = "AES/ECB/PKCS5Padding";
    public static final String DES = "DES";
    public static final String DES_CBC_NoPAD = "DES/CBC/NoPadding";
    public static final String DES_CBC_PKCS5 = "DES/CBC/PKCS5Padding";
    public static final String DES_ECB_NoPAD = "DES/ECB/NoPadding";
    public static final String DES_ECB_PKCS5 = "DES/ECB/PKCS5Padding";
    public static final String DESede = "DESede";
    public static final String DESede_CBC_NoPAD = "DESede/CBC/NoPadding";
    public static final String DESede_CBC_PKCS5 = "DESede/CBC/PKCS5Padding";
    public static final String DESede_ECB_NoPAD = "DESede/ECB/NoPadding";
    public static final String DESede_ECB_PKCS5 = "DESede/ECB/PKCS5Padding";
    public static final String RSA_ECB_PKCS5 = "RSA/ECB/PKCS1Padding";
    public static final String RSA_ECB_OAE1 = "RSA/ECB/OAEPWithSHA-1AndMGF1Padding";
    public static final String RSA_ECB_OAE256 = "RSA/ECB/OAEPWithSHA-256AndMGF1Padding";
    public static final String RSA = "RSA";


    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("mode")
            .description("Specifies whether the content should be encrypted or decrypted")
            .required(true)
            .allowableValues(ENCRYPT_MODE, DECRYPT_MODE)
            .defaultValue(ENCRYPT_MODE)
            .build();

    public static final PropertyDescriptor ALGO = new PropertyDescriptor.Builder()
            .name("algorithm")
            .description("Specifies the algorithm that the cipher will use to encrypt/decrypt")
            .required(true)
            .allowableValues(AES, AES_CBC_PKCS5, AES_ECB_PKCS5, AES_ECB_NoPAD, AES_CBC_NoPAD, DES, DES_CBC_PKCS5, DES_ECB_PKCS5, DES_CBC_NoPAD, DES_ECB_NoPAD, DESede, DESede_CBC_PKCS5, DESede_ECB_PKCS5,DESede_CBC_NoPAD,DESede_ECB_NoPAD, RSA_ECB_PKCS5, RSA_ECB_OAE1, RSA_ECB_OAE256, RSA)
            .defaultValue(AES)
            .build();

    public static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
            .name("key")
            .description("Specifies the key to use (getByte on string will be used) by the Cipher (If the algorithm use one). See javadoc for more info")
            .required(false)
            .defaultValue("azerty1234567890")
            .build();

    public static final PropertyDescriptor IV = new PropertyDescriptor.Builder()
            .name("iv")
            .description("Specifies the byte array to use as iv (getByte on string will be used) by the Cipher (If the algorithm use one). See javadoc for more info")
            .required(false)
            .build();

    public static final PropertyDescriptor KEYFILE = new PropertyDescriptor.Builder()
            .name("key.file")
            .description("Specifies the key file to use as public or private key by the Cipher (If the algorithm use one). See javadoc for more info\"")
            .required(false)
            .build();

    public static final PropertyDescriptor CHARSET_TO_USE = new PropertyDescriptor.Builder()
            .name("charset")
            .description("the charset encoding to use (e.g. 'UTF-8'). It is used for encryption/decryption on type String to convert to or from bytes.")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();



    private Encryptor encryptor = null;

    @Override
    public void init(final ProcessContext context) throws InitializationException {
        super.init(context);
        try {
            String transformation = context.getProperty(ALGO);
            String keyFile;
            byte[] iv;
            byte[] key;
            if (context.getPropertyValue(IV).isSet()){
                iv = context.getPropertyValue(IV).asString().getBytes();
            } else {
                iv = null;
            }
            if (context.getPropertyValue(KEY).isSet()){
                key = context.getPropertyValue(KEY).asString().getBytes();
            } else {
                key = null;
            }
            if (context.getPropertyValue(KEYFILE).isSet()){
                keyFile = context.getPropertyValue(KEYFILE).asString();
            } else {
                keyFile = null;
            }
            this.encryptor = initEncryptor(transformation , iv , key ,keyFile);
        } catch (Exception ex) {
            throw new InitializationException(ex);
        }
    }

    private Encryptor initEncryptor(String transformation, byte[] iv, byte[] key, String keyFile) throws NoSuchAlgorithmException, NoSuchPaddingException, IllegalArgumentException, InvalidAlgorithmParameterException, InvalidKeyException, InvalidKeySpecException {
        final String algo;
        final Optional<String> mode;
        final Optional<String> padding;
        final String[] splittedAlgo = transformation.split("/");
        if (splittedAlgo.length > 3) {
            throw new NoSuchAlgorithmException();
        } else if (splittedAlgo.length == 3) {
            algo = splittedAlgo[0];
            mode = Optional.of(splittedAlgo[1]);
            padding = Optional.of(splittedAlgo[2]);
        } else if (splittedAlgo.length == 2) {
            throw new NoSuchAlgorithmException(String.format("Invalid transformation format:%s",transformation));
        } else if (splittedAlgo.length == 1) {
            algo = splittedAlgo[0];
            mode = Optional.empty();
            padding = Optional.empty();
        } else {
            throw new  RuntimeException("should not happend");
        }

        if (EncryptorAES.ALGO_AES.equalsIgnoreCase(algo)) {
            if (mode.isPresent()){
                switch (mode.get()) {
                    case "ECB":
                    case "CBC":
                        return EncryptorAES.getInstance(mode.get(), padding.get(), key, iv);
                    default:
                        getLogger().warn("this mode: {} may not be supported yet!", new Object[]{mode.get()});
                        return EncryptorAES.getInstance(mode.get(), padding.get(), key, iv);
                }
            } else {
                return EncryptorAES.getInstance(null, null, key, iv);
            }
        } else if (EncryptorDES.ALGO_DES.equalsIgnoreCase(algo)) {
            if (mode.isPresent()){
                switch (mode.get()) {
                    case "ECB":
                    case "CBC":
                        return EncryptorDES.getInstance(mode.get(), padding.get(), key, iv);
                    default:
                        getLogger().warn("this mode: {} may not be supported yet!", new Object[]{mode.get()});
                        return EncryptorDES.getInstance(mode.get(), padding.get(), key, iv);
                }
            } else {
                return EncryptorDES.getInstance(null, null, key, iv);
            }
        } else if (EncryptorDESede.ALGO_DESede.equalsIgnoreCase(algo)) {
            if (mode.isPresent()){
                switch (mode.get()) {
                    case "ECB":
                    case "CBC":
                        return EncryptorDESede.getInstance(mode.get(), padding.get(), key, iv);
                    default:
                        getLogger().warn("this mode: {} may not be supported yet!", new Object[]{mode.get()});
                        return EncryptorDESede.getInstance(mode.get(), padding.get(), key, iv);
                }
            } else {
                return EncryptorDESede.getInstance(null, null, key, iv);
            }
        }else if (EncryptorRSA.ALGO_RSA.equalsIgnoreCase(algo)) {
            if (null != key) {
                getLogger().warn("the RSA algo will not use this key {} , RSA use public and private keys !",new Object[]{key});
            }
            if (mode.isPresent()){
                switch (mode.get()) {
                    case "ECB":
                        return EncryptorRSA.getInstance(mode.get(), padding.get(), keyFile);
                    default:
                        getLogger().warn("this mode: {} may not be supported yet!", new Object[]{mode.get()});
                        return EncryptorDESede.getInstance(mode.get(), padding.get(), key, iv);
                }
            } else {
                return EncryptorRSA.getInstance(null, null, keyFile);
            }
        }else {
            throw new IllegalArgumentException(String.format("this algo: %s is not being supported!",algo));
        }
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MODE);
        properties.add(ALGO);
        properties.add(KEY);
        properties.add(IV);
        properties.add(KEYFILE);
        properties.add(CHARSET_TO_USE);

        return Collections.unmodifiableList(properties);

    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)
                .required(false)
                .dynamic(true)
                .build();
    }


    @Override
    protected Collection<ValidationResult> customValidate(final Configuration context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));
        try {
            String keyFile;
            byte[] iv;
            byte[] key;
            String transformation = context.getPropertyValue(ALGO).asString();
            if (context.getPropertyValue(IV).isSet()){
                iv = context.getPropertyValue(IV).asString().getBytes();
            } else {
                iv = null;
            }
            if (context.getPropertyValue(KEYFILE).isSet()){
                keyFile = context.getPropertyValue(KEYFILE).asString();
            } else {
                keyFile = null;
            }
            if (context.getPropertyValue(KEY).isSet()){
                key = context.getPropertyValue(KEY).asString().getBytes();
            } else {
                key = null;
            }
            initEncryptor(transformation, iv, key, keyFile);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | IllegalArgumentException | InvalidAlgorithmParameterException | InvalidKeyException | InvalidKeySpecException ex) {
            validationResults.add(
                    new ValidationResult.Builder()
                            .input(ex.getMessage())
                            .valid(false)
                            .build());
        }
        return validationResults;
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        final boolean encrypt = context.getPropertyValue(MODE).asString().equalsIgnoreCase(ENCRYPT_MODE);
        Collection<String> allfieldsToEncrypt_InString = getFieldsNameMapping(context);
        Map<String, String> fieldsNameMappings = getFieldsNameTypeMapping(context);

        try {
            init(context);
        } catch (Throwable t) {
            getLogger().error("error while initializing", t);
        }

        try {
            for (Record record : records) {
                if (encrypt) {
                    for (String fieldName : allfieldsToEncrypt_InString) {
                        if (!record.hasField(fieldName)) continue;
                        Field field = record.getField(fieldName);
                        try {
                            if (field.getType() == FieldType.STRING) {
                                record.setCheckedField(fieldName, FieldType.BYTES, encryptor.encrypt(((String) field.getRawValue()).getBytes(context.getPropertyValue(CHARSET_TO_USE).asString())));
                            } else {
                                record.setCheckedField(fieldName, FieldType.BYTES, encryptor.encrypt(toByteArray(field.getRawValue())));
                            }
                        } catch (Exception ex) {
                            getLogger().error("error while processing record field " + fieldName, ex);
                        }

                    }

                } else {
                    for (final Map.Entry<String, String> entry : fieldsNameMappings.entrySet()) {
                        String fieldName = entry.getKey();
                        String fieldType = entry.getValue();
                        Field field = record.getField(fieldName);
                        if (!record.hasField(fieldName)) continue;
                        try {
                            if (!field.getType().equals(FieldType.BYTES)) {
                                record.addError("Wrong input ", getLogger(), " type was instead of");
                                continue;
                            }
                            FieldType type;
                            try{
                                type = getFieldType(fieldType);
                            } catch (IllegalArgumentException | NullPointerException ex) {
                                getLogger().error("error while processing record field " + fieldName + " ; ", ex);
                                continue;
                            }
                            try {
                                if (type == FieldType.STRING) {
                                    record.setCheckedField(fieldName, type, new String(encryptor.decrypt((byte[]) field.getRawValue()), context.getPropertyValue(CHARSET_TO_USE).asString()) );
                                } else {
                                    record.setCheckedField(fieldName, type,toObject(encryptor.decrypt((byte[]) field.getRawValue())));
                                }
                            } catch (Exception ex) {
                                getLogger().error("error while setting casting value to byte array ", ex);
                            }
                        } catch (Exception ex) {
                            getLogger().error("error while processing record field " + fieldName, ex);
                        }
                    }
                }

            }
        } catch (Throwable t) {
            getLogger().error("error while processing records ", t);
        }
        return records;
    }
    // convert objects to byte array
    public static byte[] toByteArray(Object obj) throws IOException {
        byte[] bytes ;
        ByteArrayOutputStream bos = null;
        ObjectOutputStream oos = null;
        try {
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray();
        } finally {
            if (oos != null) {
                oos.close();
            }
            if (bos != null) {
                bos.close();
            }
        }
        return bytes;
    }
    // convert byte array to object
    public static Object toObject(byte[] bytes) throws IOException, ClassNotFoundException {
        Object obj = null;
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try {
            bis = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bis);
            obj = ois.readObject();
        } finally {
            if (bis != null) {
                bis.close();
            }
            if (ois != null) {
                ois.close();
            }
        }
        return obj;
    }

    // get the specific field or fields to encrypt form the dynamic property
    private Collection<String> getFieldsNameMapping(ProcessContext context) {
        Collection<String> fieldsNameMappings = new ArrayList<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            final String fieldName = entry.getKey().getName();
            fieldsNameMappings.add(fieldName);
        }
        return fieldsNameMappings;
    }

    private Map<String, String> getFieldsNameTypeMapping(ProcessContext context) {

        Map<String, String> fieldsNameMappings = new HashMap<>();
        // loop over dynamic properties to add alternative regex
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }

            final String fieldName = entry.getKey().getName();
            final String fieldType = entry.getValue();

            fieldsNameMappings.put(fieldName, fieldType);
        }
        return fieldsNameMappings;
    }
    private FieldType getFieldType (String fieldType) throws IllegalArgumentException, NullPointerException {
        return FieldType.valueOf(fieldType.toUpperCase());
    }

}
