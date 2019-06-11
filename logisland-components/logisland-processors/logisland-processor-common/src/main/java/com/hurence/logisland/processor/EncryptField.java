package com.hurence.logisland.processor;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ModifyId;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import javax.crypto.*;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.DESedeKeySpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.spec.KeySpec;
import java.text.ParseException;
import java.util.*;
import java.util.logging.Level;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


@DynamicProperty(name = "field to encrypt",
        supportsExpressionLanguage = true,
        value = "a default value",
        description = "encrypt the field value")


public class EncryptField extends AbstractProcessor {

    private static final long serialVersionUID = -270933070438408174L;

    private static final Logger logger = LoggerFactory.getLogger(ModifyId.class);


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
            .allowableValues(AES, AES_CBC_PKCS5, AES_CBC_NoPAD, AES_ECB_NoPAD, AES_ECB_PKCS5, DES, DES_CBC_NoPAD, DES_CBC_PKCS5, DES_ECB_NoPAD, DES_ECB_PKCS5, DESede, DESede_CBC_NoPAD, DESede_CBC_PKCS5, DESede_ECB_NoPAD, DESede_ECB_PKCS5)
            .defaultValue(AES)
            .build();

    public static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
            .name("Key")
            .description("Specifies the key to use")
            .required(true)
            .defaultValue("azerty1234567890")
            .build();



    private Collection<String> fieldTypes = null;

    @Override
    public void init(final ProcessContext context) {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MODE);
        properties.add(ALGO);
        properties.add(KEY);

        properties = Collections.unmodifiableList(properties);
        fieldTypes = getFieldsNameMapping(context);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MODE);
        properties.add(ALGO);
        properties.add(KEY);

        return Collections.unmodifiableList(properties);

    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)  // TODO understand what expressionLanguage is !!!
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .required(false)
                .dynamic(true)
                .build();
    }

    // check if the algorithm chosen is AES, otherwaie it is DES or DESede
    public static boolean isAESAlgorithm(final String algorithm) {
        return algorithm.startsWith("A");
    }

    // encrpyt or decript data with AES algo (with different transformation available)
    //if encrypt: input object / output byte[]
    //if decrypt: input field (the field will be in type FieldType.BYTES) / output object
    public class ExempleAES {

        private final String ALGO_AES;
        private byte[] keyValue;
        private Cipher cipher;

        public ExempleAES(String ALGO, String key) throws Exception {

            ALGO_AES = ALGO;
            keyValue = key.getBytes();
            cipher = Cipher.getInstance(ALGO_AES);
        }

        public byte[] encrypt (Object Data) throws Exception{
            Key key = generateKey();
            /*Cipher c = Cipher.getInstance(ALGO_AES);*/
            cipher.init(Cipher.ENCRYPT_MODE, key);
            byte[] encVal = cipher.doFinal(toByteArray(Data));
            return  encVal;
        }

        public Object decrypt (Field encryptedData) throws  Exception {
            Key key = generateKey();
            if (ALGO_AES.contains("AES/CBC")) {
                byte[] iV = cipher.getIV();
                IvParameterSpec spec = new IvParameterSpec(iV);
                cipher.init(Cipher.DECRYPT_MODE, key, spec);
            } else {
                cipher.init(Cipher.DECRYPT_MODE, key);
            }
            /*Cipher c = Cipher.getInstance(ALGO_AES);
            cipher.init(Cipher.DECRYPT_MODE, key);*/
            byte[] encryptedDataBytes = toByteArray(encryptedData);
            byte[] decValue = cipher.doFinal(encryptedDataBytes);
            Object decryptedValue = toObject(decValue);
            return decryptedValue;
        }

        private Key generateKey() throws Exception {
            Key key = new SecretKeySpec(keyValue, "AES");
            return key;
        }

    }

    // encrpyt or decript data with DES or DESede algo (with different transformation available)
    //if encrypt: input object / output byte[]
    //if decrypt: input field (the field will be in type FieldType.BYTES) / output object
    public class ExempleDES {

        private static final String UNICODE_FORMAT = "UTF8";
        public final String DES_ENCRYPTION_SHEME;
        private KeySpec myKeySpec;
        private SecretKeyFactory mySecretKeyFactory;
        private Cipher cipher;
        byte[] keyAsBytes;
        private String myEncryptionKey;
        private String myEncryptionScheme;
        SecretKey key;

        public ExempleDES(String algo, String myEncKey) throws Exception {
            DES_ENCRYPTION_SHEME = algo;
            myEncryptionKey = myEncKey;
            myEncryptionScheme = DES_ENCRYPTION_SHEME;
            keyAsBytes = myEncryptionKey.getBytes(UNICODE_FORMAT);
            if (DES_ENCRYPTION_SHEME.startsWith("DESede")) {
                myKeySpec = new DESedeKeySpec(keyAsBytes);
                mySecretKeyFactory = SecretKeyFactory.getInstance("DESede");
                key = mySecretKeyFactory.generateSecret(myKeySpec);
            } else {
                myKeySpec = new DESKeySpec(keyAsBytes);
                mySecretKeyFactory = SecretKeyFactory.getInstance("DES");
                key = mySecretKeyFactory.generateSecret(myKeySpec);
            }
            cipher = Cipher.getInstance(myEncryptionScheme);
        }

        public byte[] encrypt (Object unencryptedString) {
            byte[] encryptedText = null;
            try {
                cipher.init(Cipher.ENCRYPT_MODE, key);
                byte[] plainText = toByteArray(unencryptedString);
                encryptedText = cipher.doFinal(plainText);
            } catch (InvalidKeyException | IllegalBlockSizeException | BadPaddingException | IOException  e) {
            }
            return encryptedText;
        }

        public Object decrypt (Field encryptedString) {
            Object decryptedText = null;
            try{
                if (myEncryptionScheme.contains("CBC")) {
                    byte[] iV = cipher.getIV();
                    IvParameterSpec spec = new IvParameterSpec(iV);
                    cipher.init(Cipher.DECRYPT_MODE, key, spec);
                } else {
                    cipher.init(Cipher.DECRYPT_MODE, key);
                }
                byte[] encryptedStringBytes = toByteArray(encryptedString);
                byte[] plainText = cipher.doFinal(encryptedStringBytes);
                decryptedText = toObject(plainText);

            } catch (InvalidKeyException | IOException | IllegalBlockSizeException | BadPaddingException | InvalidAlgorithmParameterException | ClassNotFoundException e) {
            }
            return decryptedText;
        }

    }




    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        final boolean encrypt = context.getPropertyValue(MODE).toString().equalsIgnoreCase(ENCRYPT_MODE);
        try {
            init(context);
        } catch (Throwable t) {
            logger.error("error while initializing", t);
        }

        try {
            Collection<Field> allfieldsToEncrypt = null;
            Collection<String> allfieldsToEncrypt_InString ;

            for (Record record : records) {
                // check if user choose some specific field or fields to encrypt, if don't we'll encrypt all fields.
                if (getFieldsNameMapping(context) == null) {
                    allfieldsToEncrypt = record.getAllFields();
                } else {
                    allfieldsToEncrypt_InString = getFieldsNameMapping(context);
                    for (String name : allfieldsToEncrypt_InString) {
                        allfieldsToEncrypt.add(record.getField(name));
                        /*final Object inputDateValue = context.getPropertyValue(name).evaluate(record);*/ // idea ! : we can take objects as input not fields!
                    }

                }

                for (Field field : allfieldsToEncrypt) {
                    if (isAESAlgorithm(context.getProperty(ALGO))) {
                        ExempleAES encryptAES = new ExempleAES(context.getProperty(ALGO), context.getProperty(KEY));
                        if (encrypt) {
                            record.setField(field.getName(), FieldType.BYTES, encryptAES.encrypt(field)); // is field an Object ??!!
                        } else {
                            record.setField(field.getName(), field.getType(), encryptAES.decrypt(field)); // !!!!!!!!!!! how to know the original type of the field before encrypting
                        }

                    } else {
                        ExempleDES encryptDES = new ExempleDES(context.getProperty(ALGO), context.getProperty(KEY));
                        if (encrypt) {
                            record.setField(field.getName(), FieldType.BYTES, encryptDES.encrypt(field));
                        } else {
                            record.setField(field.getName(), FieldType.STRING, encryptDES.decrypt(field));
                        }
                    }
                }

            }
        } catch (Throwable t) {
            logger.error("error while processing records", t);
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
        Collection<String> fieldsNameMappings = null;
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            final String fieldName = entry.getKey().getName();
            fieldsNameMappings.add(fieldName);
        }
        return fieldsNameMappings;
    }
}
