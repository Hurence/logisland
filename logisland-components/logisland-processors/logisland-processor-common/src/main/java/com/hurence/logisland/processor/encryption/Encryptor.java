package com.hurence.logisland.processor.encryption;

public interface Encryptor {

    public byte[] encrypt (byte[] Data) throws Exception;
    public byte[] decrypt (byte[] encryptedData) throws  Exception;
}
