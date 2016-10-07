package com.hurence.logisland.util.string;


import org.apache.commons.codec.binary.Base64;

import java.io.UnsupportedEncodingException;

/**
 * Utility class to encode/decode bytes[]
 */
public class BinaryEncodingUtils {


    public static String encode(byte[] content) throws UnsupportedEncodingException {
        return new String(Base64.encodeBase64(content), "UTF-8");
    }

    public static byte[] decode(String content)    {
        return Base64.decodeBase64(content);
    }
}