package com.hurence.logisland.processor;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.encryption.Encryptor;
import com.hurence.logisland.processor.encryption.EncryptorAES;
import com.hurence.logisland.processor.encryption.EncryptorDES;
import com.hurence.logisland.processor.encryption.EncryptorDESede;
import com.hurence.logisland.processor.encryption.EncryptorGetInstance;

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;


import javax.crypto.NoSuchPaddingException;
import java.io.*;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.*;


@DynamicProperty(name = "field to encrypt",
        supportsExpressionLanguage = true,
        value = "a default value",
        description = "encrypt the field value")
public class EncryptField extends AbstractProcessor {


    private static final long serialVersionUID = -270933070438408174L;

//    private static final Logger logger = LoggerFactory.getLogger(ModifyId.class);


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
    public static final byte[] Iv = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};


    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("Mode")
            .description("Specifies whether the content should be encrypted or decrypted")
            .required(true)
            .allowableValues(ENCRYPT_MODE, DECRYPT_MODE)
            .defaultValue(ENCRYPT_MODE)
            .build();

    public static final PropertyDescriptor ALGO = new PropertyDescriptor.Builder()
            .name("Algo")
            .description("Specifies the algorithm that the cipher will use")
            .required(true)
            .allowableValues(AES, AES_CBC_PKCS5, AES_ECB_PKCS5, AES_ECB_NoPAD, AES_CBC_NoPAD, DES, DES_CBC_PKCS5, DES_ECB_PKCS5, DES_CBC_NoPAD, DES_ECB_NoPAD, DESede, DESede_CBC_PKCS5, DESede_ECB_PKCS5,DESede_CBC_NoPAD,DESede_ECB_NoPAD, RSA_ECB_PKCS5)
            .defaultValue(AES)
            .build();

    public static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
            .name("Key")
            .description("Specifies the key to use")
            .required(true)
            .defaultValue("azerty1234567890")
            .build();

    public static final PropertyDescriptor IV = new PropertyDescriptor.Builder()
            .name("Iv")
            .description("Specifies the Iv[] to use")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            /*.defaultValue(Iv)*/
            .build();


    private Collection<String> fieldTypes = null;
    private Encryptor encryptor = null;

    @Override
    public void init(final ProcessContext context) throws InitializationException {
        super.init(context);
        fieldTypes = getFieldsNameMapping(context);
        try {
            this.encryptor = initEncryptor(new StandardValidationContext(context.getProperties()));
        } catch (Exception ex) {
            throw new InitializationException(ex);
        }
    }

    private Encryptor initEncryptor(ValidationContext context) throws NoSuchAlgorithmException, NoSuchPaddingException, IllegalArgumentException, InvalidAlgorithmParameterException, InvalidKeyException, InvalidKeySpecException {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));
        final String algo;
        final Optional<String> mode;
        final Optional<String> padding;
        final String[] splittedAlgo = context.getPropertyValue(ALGO).asString().split("/");
        if (splittedAlgo.length > 3) {
            //TODO add error
            throw new NoSuchAlgorithmException();
        } else if (splittedAlgo.length == 3) {
            //TODO
            algo = splittedAlgo[0];
            mode = Optional.of(splittedAlgo[1]);
            padding = Optional.of(splittedAlgo[2]);
        } else if (splittedAlgo.length == 2) {
            algo = splittedAlgo[0];
            mode = Optional.of(splittedAlgo[1]);
            padding = Optional.empty();
        } else if (splittedAlgo.length == 1) {
            algo = splittedAlgo[0];
            mode = Optional.empty();
            padding = Optional.empty();
        } else {
            throw new  RuntimeException("should not happend");
        }

        if (EncryptorAES.ALGO_AES.equalsIgnoreCase(algo)) {
            try {
                switch (mode.get()) {
                    case "ECB":
                        return EncryptorAES.getInstance(mode.get(), padding.get(), context.getPropertyValue(KEY).asString().getBytes(), null);
                    case "CBC":
                        return EncryptorAES.getInstance(mode.get(), padding.get(), context.getPropertyValue(KEY).asString().getBytes(), (byte[]) context.getPropertyValue(IV).getRawValue());
                    default:
                        return EncryptorAES.getInstance(mode.orElse(null), padding.orElse(null), context.getPropertyValue(KEY).asString().getBytes(), null);

                }
            } catch (NoSuchAlgorithmException | NoSuchPaddingException | IllegalArgumentException | InvalidAlgorithmParameterException | InvalidKeyException ex) {
                validationResults.add(
                        new ValidationResult.Builder()
                                .input(ex.getMessage())
                                .valid(false)
                                .build());
            }
        } else if (EncryptorDES.ALGO_DES.equalsIgnoreCase(algo)) {
            try {
                switch (mode.get()) {
                    case "ECB":
                        return EncryptorDES.getInstance(mode.get(), padding.get(), context.getPropertyValue(KEY).asString().getBytes(), null);
                    case "CBC":
                        return EncryptorDES.getInstance(mode.get(), padding.get(), context.getPropertyValue(KEY).asString().getBytes(), (byte[]) context.getPropertyValue(IV).getRawValue());
                    default:
                        return EncryptorDES.getInstance(mode.orElse(null), padding.orElse(null), context.getPropertyValue(KEY).asString().getBytes(), null);

                }
            } catch (NoSuchAlgorithmException | NoSuchPaddingException | IllegalArgumentException | InvalidAlgorithmParameterException | InvalidKeyException | InvalidKeySpecException  ex) {
                validationResults.add(
                        new ValidationResult.Builder()
                                .input(ex.getMessage())
                                .valid(false)
                                .build());
            }
        } else if (EncryptorDESede.ALGO_DESede.equalsIgnoreCase(algo)) {
            try {
                switch (mode.get()) {
                    case "ECB":
                        return EncryptorDESede.getInstance(mode.get(), padding.get(), context.getPropertyValue(KEY).asString().getBytes(), null);
                    case "CBC":
                        return EncryptorDESede.getInstance(mode.get(), padding.get(), context.getPropertyValue(KEY).asString().getBytes(), (byte[]) context.getPropertyValue(IV).getRawValue());
                    default:
                        return EncryptorDESede.getInstance(mode.orElse(null), padding.orElse(null), context.getPropertyValue(KEY).asString().getBytes(), null);

                }
            } catch (NoSuchAlgorithmException | NoSuchPaddingException | IllegalArgumentException | InvalidAlgorithmParameterException | InvalidKeyException | InvalidKeySpecException  ex) {
                validationResults.add(
                        new ValidationResult.Builder()
                                .input(ex.getMessage())
                                .valid(false)
                                .build());
            }
        }
        /*switch (algo) {
            case EncryptorAES.ALGO_AES:
                return EncryptorGetInstance.GetInstAES(algo, mode.orElse(null), padding.orElse(null), context.getPropertyValue(KEY).asString().getBytes(), (byte[]) context.getPropertyValue(IV).getRawValue());
            case EncryptorDES.ALGO_DES:
                 return EncryptorGetInstance.GetInstDES(algo, mode.orElse(null), padding.orElse(null), context.getPropertyValue(KEY).asString().getBytes(), (byte[]) context.getPropertyValue(IV).getRawValue());
            case EncryptorDESede.ALGO_DESede:
                return EncryptorGetInstance.GetInstDESede(algo, mode.orElse(null), padding.orElse(null), context.getPropertyValue(KEY).asString().getBytes(), (byte[]) context.getPropertyValue(IV).getRawValue());
            default:
                throw new  RuntimeException("should not happend");
        }*/
        return null;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MODE);
        properties.add(ALGO);
        properties.add(KEY);
        properties.add(IV);

        return Collections.unmodifiableList(properties);

    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)  // TODO understand what expressionLanguage is !!!
                .required(false)
                .dynamic(true)
                .build();
    }


    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));
        try {
            initEncryptor(context);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | IllegalArgumentException | InvalidAlgorithmParameterException | InvalidKeyException | InvalidKeySpecException ex) {
            validationResults.add(null);
        }
        return validationResults;
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        final boolean encrypt = context.getPropertyValue(MODE).toString().equalsIgnoreCase(ENCRYPT_MODE);
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
                            record.setCheckedField(fieldName, FieldType.BYTES, encryptor.encrypt(field.getRawValue()));
                        } catch (Exception ex) {
                            getLogger().error("error while processing record field" + fieldName, ex);
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
                                record.addError("Wrong input", getLogger(), "type was instead of");
                                continue;
                            }
                            FieldType type;
                            try{
                                type = getFieldType(fieldType);
                            } catch (IllegalArgumentException | NullPointerException ex) {
                                getLogger().error("error while processing record field" + fieldName + " ; ", ex);
                                continue;
                            }
                            try {
                                record.setCheckedField(fieldName, type, encryptor.decrypt((byte[]) field.getRawValue()));
                            } catch (Exception ex) {
                                getLogger().error("error while setting casting value to byte array", ex);
                            }
                        } catch (Exception ex) {
                            getLogger().error("error while processing record field" + fieldName, ex);
                        }
                    }
                }

            }
        } catch (Throwable t) {
            getLogger().error("error while processing records", t);
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
