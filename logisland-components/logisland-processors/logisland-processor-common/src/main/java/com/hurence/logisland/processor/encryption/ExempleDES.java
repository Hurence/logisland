package com.hurence.logisland.processor.encryption;

import com.hurence.logisland.processor.EncryptField;

import javax.crypto.*;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.DESedeKeySpec;
import javax.crypto.spec.IvParameterSpec;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.spec.KeySpec;

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
            byte[] plainText = EncryptField.toByteArray(unencryptedString);
            encryptedText = cipher.doFinal(plainText);
        } catch (InvalidKeyException | IllegalBlockSizeException | BadPaddingException | IOException e) {
        }
        return encryptedText;
    }

    public Object decrypt(byte[] encrypted) {
        Object decryptedText = null;
        try{
            if (myEncryptionScheme.contains("CBC")) {
                byte[] iV = cipher.getIV();
                IvParameterSpec spec = new IvParameterSpec(iV);
                cipher.init(Cipher.DECRYPT_MODE, key, spec);
            } else {
                cipher.init(Cipher.DECRYPT_MODE, key);
            }
            byte[] plainText = cipher.doFinal(encrypted);
            decryptedText = EncryptField.toObject(plainText);

        } catch (InvalidKeyException | IOException | IllegalBlockSizeException | BadPaddingException | InvalidAlgorithmParameterException | ClassNotFoundException e) {
        }
        return decryptedText;
    }

}
