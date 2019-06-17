package com.hurence.logisland.processor.encryption;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.*;
import com.hurence.logisland.processor.EncryptField;
import com.sun.crypto.provider.SunJCE;

import javax.crypto.SecretKey;

public class ExempleRSA {
    private final String ALGO_RSA;
    private Cipher cipher;
    public byte[] Iv;
    public byte[] wrapedKey;
    public  String algoName;

    public ExempleRSA(String ALGO) throws Exception {
        ALGO_RSA = ALGO;
        cipher = Cipher.getInstance(ALGO_RSA);
    }
    public ExempleRSA(String ALGO, byte[] iv) throws Exception {
        ALGO_RSA = ALGO;
        cipher = Cipher.getInstance(ALGO_RSA);
        Iv = iv;
    }

    public  byte[] encrypt(Object Data, String keyString) throws Exception {
        Security.addProvider(new SunJCE()); // OR BELOW
        Security.insertProviderAt(new SunJCE(), 1);
        Key keywrap = generateKey(keyString);
        KeyPair keyPair = genKey();
        PublicKey key = keyPair.getPublic();
        /*Cipher cipher = Cipher.getInstance(ALGO_RSA);*/
        cipher.init(Cipher.ENCRYPT_MODE, key);
        byte[] inpBytes = EncryptField.toByteArray(Data);
        PrivateKey prKey = keyPair.getPrivate();
        algoName = prKey.getAlgorithm();
        Cipher cipher1 = Cipher.getInstance(ALGO_RSA,"SunJCE");
        cipher1.init(Cipher.WRAP_MODE,keywrap);
        wrapedKey = cipher1.wrap(prKey);
        Iv = cipher.getIV();
        byte[] encVal =cipher.doFinal(inpBytes);
        return encVal;
    }
    public   Object decrypt(byte[] inpBytes, Key key) throws Exception{

        /*Cipher cipher1 = Cipher.getInstance(ALGO_RSA);
        cipher1.init(Cipher.WRAP_MODE,keywrap);*/

        if (ALGO_RSA.contains("RES/CBC")) {
            byte[] iV = cipher.getIV();
            IvParameterSpec spec = new IvParameterSpec(Iv);
            cipher.init(Cipher.DECRYPT_MODE, key, spec);
        } else {
            cipher.init(Cipher.DECRYPT_MODE, key);
        }
        byte[] decValue = cipher.doFinal(inpBytes);
        Object decryptedValue = EncryptField.toObject(decValue);
        return decryptedValue;
    }

    public  KeyPair genKey() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(512); // 512 is the keysize.
        return (kpg.generateKeyPair());
    }

    public byte[] getiv() {
        return Iv;
    }

    public byte[] wrapedkey() {
        return wrapedKey;
    }

    public Key getPriKey(byte[] wrapedKey, String keyString) throws  Exception {
        Key keywrap = generateKey(keyString);
        Cipher cipher = Cipher.getInstance(ALGO_RSA);
        cipher.init(Cipher.UNWRAP_MODE,keywrap);
        Key key = cipher.unwrap(wrapedKey, get_algo_name(), 2);
        return key;
    }

    public String get_algo_name() {
        return algoName;
    }

    private Key generateKey(String keyValue) throws Exception {
        Key key = new SecretKeySpec(keyValue.getBytes(), "RSA");
        return key;
    }
}
