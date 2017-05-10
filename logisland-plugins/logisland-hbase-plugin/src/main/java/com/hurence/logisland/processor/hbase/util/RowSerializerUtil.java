
package com.hurence.logisland.processor.hbase.util;


import com.hurence.logisland.processor.hbase.scan.ResultCell;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class RowSerializerUtil {

    /**
     * @param rowId the row id to get the string from
     * @param charset the charset that was used to encode the cell's row
     * @param base64encodeValues whether or not to base64 encode the returned string
     *
     * @return the String representation of the cell's row
     */
    public static String getRowId(final byte[] rowId, final Charset charset, final boolean base64encodeValues) {
        if (base64encodeValues) {
            ByteBuffer cellRowBuffer = ByteBuffer.wrap(rowId);
            ByteBuffer base64Buffer = Base64.getEncoder().encode(cellRowBuffer);
            return new String(base64Buffer.array(), StandardCharsets.UTF_8);
        } else {
            return new String(rowId, charset);
        }
    }

    /**
     * @param cell the cell to get the family from
     * @param charset the charset that was used to encode the cell's family
     * @param base64encodeValues whether or not to base64 encode the returned string
     *
     * @return the String representation of the cell's family
     */
    public static String getCellFamily(final ResultCell cell, final Charset charset, final boolean base64encodeValues) {
        if (base64encodeValues) {
            ByteBuffer cellFamilyBuffer = ByteBuffer.wrap(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            ByteBuffer base64Buffer = Base64.getEncoder().encode(cellFamilyBuffer);
            return new String(base64Buffer.array(), StandardCharsets.UTF_8);
        } else {
            return new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(), charset);
        }
    }

    /**
     * @param cell the cell to get the qualifier from
     * @param charset the charset that was used to encode the cell's qualifier
     * @param base64encodeValues whether or not to base64 encode the returned string
     *
     * @return the String representation of the cell's qualifier
     */
    public static String getCellQualifier(final ResultCell cell, final Charset charset, final boolean base64encodeValues) {
        if (base64encodeValues) {
            ByteBuffer cellQualifierBuffer = ByteBuffer.wrap(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            ByteBuffer base64Buffer = Base64.getEncoder().encode(cellQualifierBuffer);
            return new String(base64Buffer.array(), StandardCharsets.UTF_8);
        } else {
            return new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(), charset);
        }
    }

    /**
     * @param cell the cell to get the value from
     * @param charset the charset that was used to encode the cell's value
     * @param base64encodeValues whether or not to base64 encode the returned string
     *
     * @return the String representation of the cell's value
     */
    public static String getCellValue(final ResultCell cell, final Charset charset, final boolean base64encodeValues) {
        if (base64encodeValues) {
            ByteBuffer cellValueBuffer = ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            ByteBuffer base64Buffer = Base64.getEncoder().encode(cellValueBuffer);
            return new String(base64Buffer.array(), StandardCharsets.UTF_8);
        } else {
            return new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), charset);
        }
    }

}
