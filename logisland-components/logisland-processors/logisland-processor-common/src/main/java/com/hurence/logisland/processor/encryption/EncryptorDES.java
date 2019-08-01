package com.hurence.logisland.processor.encryption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger logger = LoggerFactory.getLogger(EncryptorDES.class);

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
        if (null == key || key.length%8 != 0) throw new InvalidKeyException("Invalid DES key length ");
        if (mode == null) {
            return new EncryptorDES(null, null, key, null);
        }
        switch (mode) {
            case "CBC":
                if (iv == null || iv.length != 8) {
                    logger.warn("Invalid IV! default IV will be used ");
                    iv = "12345678".getBytes();
                }
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

    public byte[] encrypt (byte[] Data) throws Exception{
        if (null != mode && mode.equalsIgnoreCase("CBC")) {
            IvParameterSpec spec = new IvParameterSpec(iv);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, spec);
        } else {
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
        }
        return  cipher.doFinal(Data);
    }

    public byte[] decrypt (byte[] encryptedData) throws  Exception {
        if (null != mode && mode.equalsIgnoreCase("CBC")) {
            IvParameterSpec spec = new IvParameterSpec(iv);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, spec);
        } else {
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
        }
        return cipher.doFinal(encryptedData);
    }
}
