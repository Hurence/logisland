package com.hurence.logisland.processor.encryption;


import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.*;
import java.security.spec.*;
import scala.io.Source;


public class EncryptorRSA implements Encryptor {

    public final static String ALGO_RSA = "RSA";
    private String mode;
    private String padding;
    private String key;
    private Cipher cipher;

    public static EncryptorRSA getInstance(String mode, String padding, String key)
            throws NoSuchAlgorithmException, NoSuchPaddingException, IllegalArgumentException {
        if (mode == null) {
            return new EncryptorRSA(null, null, key);
        }
        return new EncryptorRSA(mode, padding, key);
    }

    private EncryptorRSA(String mode, String padding, String key) throws NoSuchAlgorithmException, NoSuchPaddingException {
        this.mode = mode;
        this.padding = padding;
        this.key = key;
        if (mode == null) {
            cipher = Cipher.getInstance(ALGO_RSA);
        } else {cipher = Cipher.getInstance(ALGO_RSA +"/"+mode+"/"+padding);}
    }

    public byte[] encrypt (byte[] Data) throws Exception{
        PublicKey pubKey = readPublicKeyFromFile(this.key);
        cipher.init(Cipher.ENCRYPT_MODE, pubKey);
        return  cipher.doFinal(Data);
    }

    public byte[] decrypt (byte[] encryptedData) throws  Exception {
        PrivateKey privateKey = readPrivateKeyFromFile(this.key);
        cipher.init(Cipher.DECRYPT_MODE, privateKey);
        return cipher.doFinal(encryptedData);

    }

    public PublicKey readPublicKeyFromFile (String filename) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        if (filename.endsWith("pem")) {
            return getPublicKey(filename);
        } else {
            byte[] keyBytes = Files.readAllBytes(new File(filename).toPath());
            X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return kf.generatePublic(spec);
        }
    }
    public PrivateKey readPrivateKeyFromFile (String filename) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        if (filename.endsWith("pem")) {
            return getPrivateKey(filename);
        } else {
            byte[] keyBytes = Files.readAllBytes(new File(filename).toPath());
            PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return kf.generatePrivate(spec);
        }
    }

    public PublicKey getPublicKey (String filename) throws NoSuchAlgorithmException, InvalidKeySpecException{
        String publicKeyPEM = Source.fromFile(filename, StandardCharsets.UTF_8.toString()).getLines().mkString("\n");
        return getPublicKeyFROMString(publicKeyPEM);

    }

    public PublicKey getPublicKeyFROMString (String key) throws NoSuchAlgorithmException, InvalidKeySpecException{
        String publicKeyPEM = key;
        publicKeyPEM = publicKeyPEM.replace("-----BEGIN PUBLIC KEY-----\n", "");
        publicKeyPEM = publicKeyPEM.replace("-----END PUBLIC KEY-----", "");
        byte[] encoded = Base64.decodeBase64(publicKeyPEM.trim());
        KeyFactory kf = KeyFactory.getInstance("RSA");
        PublicKey pubKey = kf.generatePublic(new X509EncodedKeySpec(encoded));
        return pubKey;
    }
    public PrivateKey getPrivateKey (String filename) throws NoSuchAlgorithmException, InvalidKeySpecException{
        String privateKeyPEM = Source.fromFile(filename, StandardCharsets.UTF_8.toString()).getLines().mkString("\n");
        return getPrivateKeyFROMString(privateKeyPEM);

    }

    public PrivateKey getPrivateKeyFROMString (String key) throws NoSuchAlgorithmException, InvalidKeySpecException{
        String privateKeyPEM = key;
        privateKeyPEM = privateKeyPEM.replace("-----BEGIN PRIVATE KEY-----\n", "");
        privateKeyPEM = privateKeyPEM.replace("-----END PRIVATE KEY-----", "");
        byte[] encoded = Base64.decodeBase64(privateKeyPEM.trim());
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(encoded);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        PrivateKey privateKey = kf.generatePrivate(spec);
        return privateKey;
    }


}
