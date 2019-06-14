package com.hurence.logisland.processor.encryption;

import com.hurence.logisland.processor.EncryptField;
import com.hurence.logisland.record.Field;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.Key;

// encrpyt or decript data with AES algo (with different transformation available)
//if encrypt: input object / output byte[]
//if decrypt: input field (the field will be in type FieldType.BYTES) / output object
public class ExempleAES {

    private final String ALGO_AES;
    private byte[] keyValue;
    public Cipher cipher;
    public byte[] Iv;

    public ExempleAES(String ALGO, String key) throws Exception {

        ALGO_AES = ALGO;
        keyValue = key.getBytes();
        cipher = Cipher.getInstance(ALGO_AES);
    }
    public ExempleAES(String ALGO, String key, byte[] iv) throws Exception {

        ALGO_AES = ALGO;
        keyValue = key.getBytes();
        cipher = Cipher.getInstance(ALGO_AES);
        Iv = iv;
    }

    public byte[] encrypt (Object Data) throws Exception{
        Key key = generateKey();
        /*Cipher c = Cipher.getInstance(ALGO_AES);*/
        cipher.init(Cipher.ENCRYPT_MODE, key);
        byte[] x = EncryptField.toByteArray(Data);
        byte[] encVal = cipher.doFinal(x);
        Iv = cipher.getIV();
        return  encVal;
    }

    public Object decrypt (byte[] encryptedData) throws  Exception {
        Key key = generateKey();
        if (ALGO_AES.contains("AES/CBC")) {
            /*byte[] iV = cipher.getIV();*/
            IvParameterSpec spec = new IvParameterSpec(Iv);
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

    public byte[] getiv() {
        return Iv;
    }

}
  // TlHvP12Rk8lW44cxGTcd4g==   Tm91cmk=