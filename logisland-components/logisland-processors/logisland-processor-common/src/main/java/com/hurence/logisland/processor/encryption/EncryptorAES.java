package com.hurence.logisland.processor.encryption;

import com.hurence.logisland.processor.EncryptField;
import com.hurence.logisland.record.Field;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.*;
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
            throws NoSuchAlgorithmException, NoSuchPaddingException, IllegalArgumentException, InvalidKeyException, InvalidAlgorithmParameterException {
        if (key.length%16 != 0) throw new InvalidKeyException("Invalid AES key length :" +key.length+"bytes");
        if (mode == null) {
            return new EncryptorAES(null, null, key, null);
        }
        switch (mode) {
            case "CBC":
                if (iv == null) throw new IllegalArgumentException("iv is required");
                if (padding == null) throw new NoSuchAlgorithmException("Invalid transformation format:"+ALGO_AES+"/"+mode);
                break;
            case "ECB":
                if (iv != null) throw new InvalidAlgorithmParameterException("ECB mode cannot use IV");
                if (padding == null) throw new NoSuchAlgorithmException("Invalid transformation format:"+ALGO_AES+"/"+mode);
                break;
        }
        return new EncryptorAES(mode, padding, key, iv);
    }

    private EncryptorAES(String mode, String padding, byte[] key, byte[] iv) throws NoSuchAlgorithmException, NoSuchPaddingException {
        if (mode != null) {this.mode = mode;} else {
            this.mode = "";
        }

        this.padding = padding;
        this.key = key;
        if (iv.length != 16) {
            this.iv = "azerty1234567890".getBytes();
        } else {
            this.iv = iv;
        }
        if (mode == null) {
            cipher = Cipher.getInstance(ALGO_AES);
        } else {cipher = Cipher.getInstance(ALGO_AES+"/"+mode+"/"+padding);}
    }

    public byte[] encrypt (Object Data) throws Exception{
        Key key = generateKey();
        if (mode.equalsIgnoreCase("CBC")) {
            IvParameterSpec spec = new IvParameterSpec(iv);
            cipher.init(Cipher.ENCRYPT_MODE, key, spec);
        } else {
            cipher.init(Cipher.ENCRYPT_MODE, key);
        }
        if ("NoPadding".equalsIgnoreCase(padding)) {
            try {
                String DataString = (String) Data;
                byte[] x = DataString.getBytes();
                byte[] encVal = cipher.doFinal(x);
                return encVal;
            } catch (ClassCastException e) {
                //ToDo how to handel this try!
            }
        }
        byte[] x = EncryptField.toByteArray(Data);
        byte[] encVal = cipher.doFinal(x);
        return  encVal;


        //TODO handle case of strings, not using ObjectStream just getBytes for example or 64BaseEcncoding

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
        if ("NoPadding".equalsIgnoreCase(padding)){
            try {
                String decryptedData = new String(decValue);
                return decryptedData;
            } catch (ClassCastException e) {
                //ToDo how to handel this try!
            }
        }
        Object decryptedValue = EncryptField.toObject(decValue);
        return decryptedValue;

        //TODO handle case of strings, not using ObjectStream just getBytes for example or 64BaseEcncoding
    }

    private Key generateKey() throws Exception {
        return new SecretKeySpec(this.key, ALGO_AES);
    }

}