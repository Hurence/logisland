package com.hurence.logisland.processor.encryption;

import com.hurence.logisland.processor.EncryptField;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.Key;

public class ExempleAEScbc {
    private final String ALGO_AES;
    private byte[] keyValue;
    private Cipher cipher;

    public ExempleAEScbc(String ALGO, String key, Cipher c) throws Exception {

        ALGO_AES = ALGO;
        keyValue = key.getBytes();
        cipher = c;
    }

    public ExempleAEScbc(String ALGO, String key) throws Exception {

        ALGO_AES = ALGO;
        keyValue = key.getBytes();
        cipher = Cipher.getInstance(ALGO_AES);
    }

    public byte[] encrypt (Object Data) throws Exception{
        Key key = generateKey();
        /*Cipher c = Cipher.getInstance(ALGO_AES);*/
        cipher.init(Cipher.ENCRYPT_MODE, key);
        byte[] encVal = cipher.doFinal(EncryptField.toByteArray(Data));
        return  encVal;
    }

    public byte[] getbyteCipher () throws  Exception{
        return (EncryptField.toByteArray(cipher));
    }

    public Object decrypt (byte[] encryptedData, Cipher c) throws  Exception {
        Key key = generateKey();
        if (ALGO_AES.contains("AES/CBC")) {
            cipher = c;
            byte[] iV = cipher.getIV();
            IvParameterSpec spec = new IvParameterSpec(iV);
            cipher.init(Cipher.DECRYPT_MODE, key, spec);
        } else {
            cipher.init(Cipher.DECRYPT_MODE, key);
        }
        /*Cipher c = Cipher.getInstance(ALGO_AES);
        cipher.init(Cipher.DECRYPT_MODE, key);*/
        /*byte[] encryptedDataBytes = EncryptField.toByteArray(encryptedData);*/
        byte[] decValue = cipher.doFinal(encryptedData);
        Object decryptedValue = EncryptField.toObject(decValue);
        return decryptedValue;
    }

    private Key generateKey() throws Exception {
        Key key = new SecretKeySpec(keyValue, "AES");
        return key;
    }
}
