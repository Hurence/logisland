package com.hurence.logisland.processor.encryption;

import com.hurence.logisland.processor.EncryptField;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.IvParameterSpec;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;

public class EncryptorDES implements Encryptor {

    public final static String ALGO_DES = "DES";
    private String mode;
    private String padding;
    private byte[] key;
    private byte[] iv;
    private Cipher cipher;
    private KeySpec myKeySpec;
    private SecretKeyFactory mySecretKeyFactory;
    SecretKey secretKey;

    public static EncryptorDES getInstance(String mode, String padding, byte[] key, byte[] iv)
            throws NoSuchAlgorithmException, NoSuchPaddingException, IllegalArgumentException, InvalidKeyException, InvalidAlgorithmParameterException, InvalidKeySpecException {
        //TODO validate that parameters are correct depending on mode padding etc
        if (key.length%8 != 0) throw new InvalidKeyException("Invalid DES key length"+key.length+"bytes");
        if (mode == null) {
            return new EncryptorDES(null, null, key, null);
        }
        switch (mode) {
            case "CBC":
                if (iv != null) throw new IllegalArgumentException("iv is required");
                if (padding == null) throw new NoSuchAlgorithmException("Invalid transformation format:"+ALGO_DES+"/"+mode);
                break;
            case "ECB":
                if (iv != null) throw new InvalidAlgorithmParameterException("ECB mode cannot use IV");
                if (padding == null) throw new NoSuchAlgorithmException("Invalid transformation format:"+ALGO_DES+"/"+mode);
                break;
        }
        return new EncryptorDES(mode, padding, key, iv);
    }

    private EncryptorDES(String mode, String padding, byte[] key, byte[] iv) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, InvalidKeySpecException {
        this.mode = mode;
        this.padding = padding;
        this.key = key;
        this.iv = iv;
        myKeySpec = new DESKeySpec(key);
        mySecretKeyFactory = SecretKeyFactory.getInstance("DES");
        secretKey = mySecretKeyFactory.generateSecret(myKeySpec);
        if (mode == null) {
            cipher = Cipher.getInstance(ALGO_DES);
        } else {cipher = Cipher.getInstance(ALGO_DES+"/"+mode+"/"+padding);}
    }

    public byte[] encrypt (Object Data) throws Exception{
        /*Key key = generateKey();*/
        if (mode.equalsIgnoreCase("CBC")) {
            IvParameterSpec spec = new IvParameterSpec(iv);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, spec);
        } else {
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
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
        } else {
            byte[] x = EncryptField.toByteArray(Data);
            byte[] encVal = cipher.doFinal(x);
            return  encVal;
        }

        //TODO handle case of strings, not using ObjectStream just getBytes for example or 64BaseEcncoding
        return null;
    }

    public Object decrypt (byte[] encryptedData) throws  Exception {
        /*Key key = generateKey();*/
        if (mode.equalsIgnoreCase("CBC")) {
            IvParameterSpec spec = new IvParameterSpec(iv);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, spec);
        } else {
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
        }
        byte[] decValue = cipher.doFinal(encryptedData);
        if ("NoPadding".equalsIgnoreCase(padding)){
            try {
                String decryptedData = new String(decValue);
                return decryptedData;
            } catch (ClassCastException e) {
                //ToDo how to handel this try!
            }
        } else {
            Object decryptedValue = EncryptField.toObject(decValue);
            return decryptedValue;
        }
        //TODO handle case of strings, not using ObjectStream just getBytes for example or 64BaseEcncoding
        return decValue;
    }

    /*private Key generateKey() throws Exception {
        return new SecretKeySpec(this.key, ALGO_DESede);
    }*/
}
