package com.hurence.logisland.processor.encryption;

import com.hurence.logisland.processor.EncryptField;
import com.hurence.logisland.record.Field;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.AlgorithmParameters;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.spec.AlgorithmParameterSpec;

// encrpyt or decript data with AES algo (with different transformation available)
//if encrypt: input object / output byte[]
//if decrypt: input field (the field will be in type FieldType.BYTES) / output object
public class EncryptorAES implements Encryptor {

    public final static String ALGO_AES = "AES";
    private String mode;
    private String padding;
    private byte[] key;
    private byte[] iv;
    private Cipher cipher;

    public static EncryptorAES getInstance(String mode, String padding, byte[] key, byte[] iv)
            throws NoSuchAlgorithmException, NoSuchPaddingException, IllegalArgumentException {
        //TODO validate that parameters are correct depending on mode padding etc
        if (mode == null) {
            if (iv == null) throw new IllegalArgumentException("iv is required");
            return new EncryptorAES(null, null, key, iv);
        }
        switch (mode) {
            case "CBC":
                //TODO
                break;
            case "ECB":
                //TODO
                break;
        }
        return new EncryptorAES(mode, padding, key, iv);
    }

    private EncryptorAES(String mode, String padding, byte[] key, byte[] iv) throws NoSuchAlgorithmException, NoSuchPaddingException {
        this.mode = mode;
        this.padding = padding;
        this.key = key;
        this.iv = iv;
        cipher = Cipher.getInstance(ALGO_AES);
    }

    public byte[] encrypt (Object Data) throws Exception{
        Key key = generateKey();
        if (mode.equalsIgnoreCase("CBC")) {
            IvParameterSpec spec = new IvParameterSpec(iv);
            cipher.init(Cipher.ENCRYPT_MODE, key, spec);
        } else {
            cipher.init(Cipher.ENCRYPT_MODE, key);
        }
        //TODO handle case of strings, not using ObjectStream just getBytes for example or 64BaseEcncoding
        byte[] x = EncryptField.toByteArray(Data);
        byte[] encVal = cipher.doFinal(x);
        return  encVal;
    }

    public Object decrypt (byte[] encryptedData) throws  Exception {
        Key key = generateKey();
        if (mode.equalsIgnoreCase("CBC")) {
            IvParameterSpec spec = new IvParameterSpec(iv);
            cipher.init(Cipher.DECRYPT_MODE, key, spec);
        } else {
            cipher.init(Cipher.DECRYPT_MODE, key);
        }
        byte[] decValue = cipher.doFinal(encryptedData);
        //TODO handle case of strings, not using ObjectStream just getBytes for example or 64BaseEcncoding
        Object decryptedValue = EncryptField.toObject(decValue);
        return decryptedValue;
    }

    private Key generateKey() throws Exception {
        return new SecretKeySpec(this.key, ALGO_AES);
    }

}