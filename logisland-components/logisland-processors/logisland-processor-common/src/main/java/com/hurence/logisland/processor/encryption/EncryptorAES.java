package com.hurence.logisland.processor.encryption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.*;

// encrpyt or decript data with AES algo (with different transformation available)
public class EncryptorAES implements Encryptor {

    private static final Logger logger = LoggerFactory.getLogger(EncryptorAES.class);

    public final static String ALGO_AES = "AES";
    private String mode;
    private String padding;
    private byte[] key;
    private byte[] iv;
    private Cipher cipher;

    public static EncryptorAES getInstance(String mode, String padding, byte[] key, byte[] iv)
            throws NoSuchAlgorithmException, NoSuchPaddingException, IllegalArgumentException, InvalidKeyException, InvalidAlgorithmParameterException {
        if (null == key || key.length%16 != 0) throw new InvalidKeyException("Invalid AES key length ");
        if (mode == null) {
            return new EncryptorAES(null, null, key, null);
        }
        switch (mode) {
            case "CBC":
                if (null == iv || iv.length != 16) {
                    logger.warn("Invalid IV! default IV will be used ");
                    iv = "azerty1234567890".getBytes();
                }
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
        this.mode = mode;
        this.padding = padding;
        this.key = key;
        this.iv = iv;
        if (mode == null) {
            cipher = Cipher.getInstance(ALGO_AES);
        } else {cipher = Cipher.getInstance(ALGO_AES+"/"+mode+"/"+padding);}
    }

    public byte[] encrypt (byte[] Data) throws Exception{
        Key key = generateKey();
        if (null != mode && mode.equalsIgnoreCase("CBC")) {
            IvParameterSpec spec = new IvParameterSpec(iv);
            cipher.init(Cipher.ENCRYPT_MODE, key, spec);
        } else {
            cipher.init(Cipher.ENCRYPT_MODE, key);
        }
        return  cipher.doFinal(Data);
    }

    public byte[] decrypt (byte[] encryptedData) throws  Exception {
        Key key = generateKey();
        if (null != mode && mode.equalsIgnoreCase("CBC")) {
            IvParameterSpec spec = new IvParameterSpec(iv);
            cipher.init(Cipher.DECRYPT_MODE, key, spec);
        } else {
            cipher.init(Cipher.DECRYPT_MODE, key);
        }
        return cipher.doFinal(encryptedData);
    }

    private Key generateKey() {
        return new SecretKeySpec(this.key, ALGO_AES);
    }

}