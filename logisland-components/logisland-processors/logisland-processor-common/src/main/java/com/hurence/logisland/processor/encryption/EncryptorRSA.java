package com.hurence.logisland.processor.encryption;


import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.*;
import java.security.spec.*;
// for using encryption/decryption with RSA algo we recommend generating both public and private keys with these commands:
//               $ openssl genrsa -out keypair.pem 4096  // you can use 1024 or 2048 also
//               Generating RSA private key, 4096 bit long modulus
//                ............+++
//                ................................+++
//                e is 65537 (0x10001)
//                $ openssl rsa -in keypair.pem -outform DER -pubout -out public.der
//                writing RSA key
//                $ openssl pkcs8 -topk8 -nocrypt -in keypair.pem -outform DER -out private.der
// then provide the path for public.der if encrypting, or the path for private.dem if decrypting.cd e

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
        byte[] keyBytes = Files.readAllBytes(new File(filename).toPath());
        X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePublic(spec);
    }
    public PrivateKey readPrivateKeyFromFile (String filename) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        byte[] keyBytes = Files.readAllBytes(new File(filename).toPath());
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePrivate(spec);
    }
}
