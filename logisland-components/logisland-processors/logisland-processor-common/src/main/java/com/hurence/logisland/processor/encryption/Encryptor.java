package com.hurence.logisland.processor.encryption;

public interface Encryptor {

    public byte[] encrypt (Object Data) throws Exception;
    public Object decrypt (byte[] encryptedData) throws  Exception;
}
