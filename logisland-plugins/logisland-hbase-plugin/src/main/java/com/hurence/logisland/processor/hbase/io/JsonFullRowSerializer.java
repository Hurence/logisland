package com.hurence.logisland.processor.hbase.io;

import com.hurence.logisland.processor.hbase.scan.ResultCell;
import com.hurence.logisland.processor.hbase.util.RowSerializerUtil;
import org.apache.commons.lang3.StringEscapeUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

/**
 * Serializes a row from HBase to a JSON document of the form:
 *
 * {
 *    "row" : "row1",
 *    "cells": [
 *      {
 *          "family" : "fam1",
 *          "qualifier" : "qual1"
 *          "value" : "val1"
 *          "timestamp" : 123456789
 *      },
 *      {
 *          "family" : "fam1",
 *          "qualifier" : "qual2"
 *          "value" : "val2"
 *          "timestamp" : 123456789
 *      }
 *    ]
 * }
 *
 * If base64encode is true, the row id, family, qualifier, and value will be represented as base 64 encoded strings.
 */
public class JsonFullRowSerializer implements RowSerializer {

    private final Charset decodeCharset;
    private final Charset encodeCharset;
    private final boolean base64encode;

    public JsonFullRowSerializer(final Charset decodeCharset, final Charset encodeCharset) {
        this(decodeCharset, encodeCharset, false);
    }

    public JsonFullRowSerializer(final Charset decodeCharset, final Charset encodeCharset, final boolean base64encode) {
        this.decodeCharset = decodeCharset;
        this.encodeCharset = encodeCharset;
        this.base64encode = base64encode;
    }

    @Override
    public String serialize(byte[] rowKey, ResultCell[] cells) {
        final String rowId = RowSerializerUtil.getRowId(rowKey, decodeCharset, base64encode);

        final StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{");

        jsonBuilder.append("\"row\":");
        appendString(jsonBuilder, rowId, base64encode);

        jsonBuilder.append(", \"cells\": [");
        int i = 0;
        for (final ResultCell cell : cells) {
            final String cellFamily = RowSerializerUtil.getCellFamily(cell, decodeCharset, base64encode);
            final String cellQualifier = RowSerializerUtil.getCellQualifier(cell, decodeCharset, base64encode);
            final String cellValue = RowSerializerUtil.getCellValue(cell, decodeCharset, base64encode);

            if (i > 0) {
                jsonBuilder.append(", ");
            }

            // start cell
            jsonBuilder.append("{");

            jsonBuilder.append("\"fam\":");
            appendString(jsonBuilder, cellFamily, base64encode);

            jsonBuilder.append(",\"qual\":");
            appendString(jsonBuilder, cellQualifier, base64encode);

            jsonBuilder.append(",\"val\":");
            appendString(jsonBuilder, cellValue, base64encode);

            jsonBuilder.append(",\"ts\":");
            jsonBuilder.append(String.valueOf(cell.getTimestamp()));

            // end cell
            jsonBuilder.append("}");
            i++;
        }

        // end cell array
        jsonBuilder.append("]");

        // end overall document
        jsonBuilder.append("}");
        return jsonBuilder.toString();
    }

    @Override
    public void serialize(final byte[] rowKey, final ResultCell[] cells, final OutputStream out) throws IOException {
        final String json = serialize(rowKey, cells);
        out.write(json.getBytes(encodeCharset));
    }

    private void appendString(final StringBuilder jsonBuilder, final String str, final boolean base64encode) {
        jsonBuilder.append("\"");

        // only escape the value when not doing base64
        if (!base64encode) {
            jsonBuilder.append(StringEscapeUtils.escapeJson(str));
        } else {
            jsonBuilder.append(str);
        }

        jsonBuilder.append("\"");
    }

}
