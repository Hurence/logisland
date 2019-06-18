package com.hurence.logisland.processor.encryption;

import javax.crypto.NoSuchPaddingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;



public class EncryptorGetInstance {

    private String algo;
    private String mode;
    private String padding;
    private byte[] key;
    private byte[] iv;


    public EncryptorGetInstance(String algo, String mode, String padding, byte[] key, byte[] iv) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException, InvalidKeySpecException {
        this.algo = algo;
        this.mode = mode;
        this.padding = padding;
        this.key = key;
        this.iv = iv;
        /*switch (algo) {
            case EncryptorAES.ALGO_AES:
                GetInstAES(algo, mode, padding, key, iv);
                break;
            case EncryptorDES.ALGO_DES:
                GetInstDES(algo, mode, padding, key, iv);
                break;
            case EncryptorDESede.ALGO_DESede:
                GetInstDESede(algo, mode, padding, key, iv);
        }*/

    }

    public static Encryptor GetInstAES(String algo, String mode, String padding, byte[] key, byte[] iv) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException {
        switch (mode) {
            case "ECB":
                return EncryptorAES.getInstance(mode, padding, key, null);
            case "CBC":
                return EncryptorAES.getInstance(mode, padding, key, iv);
            default:
                throw new  RuntimeException("should not happend");

        }
    }
    public static Encryptor GetInstDES(String algo, String mode, String padding, byte[] key, byte[] iv) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException, InvalidKeySpecException {
        switch (mode) {
            case "ECB":
                return EncryptorDES.getInstance(mode, padding, key, null);
            case "CBC":
                return EncryptorDES.getInstance(mode, padding, key, iv);
            default:
                throw new  RuntimeException("should not happend");

        }
    }
    public static Encryptor GetInstDESede(String algo, String mode, String padding, byte[] key, byte[] iv) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException, InvalidKeySpecException {
        switch (mode) {
            case "ECB":
                return EncryptorDESede.getInstance(mode, padding, key, null);
            case "CBC":
                return EncryptorDESede.getInstance(mode, padding, key, iv);
            default:
                throw new  RuntimeException("should not happend");

        }
    }
}
