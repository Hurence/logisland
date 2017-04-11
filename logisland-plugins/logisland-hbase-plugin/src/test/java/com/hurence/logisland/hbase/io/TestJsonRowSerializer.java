package com.hurence.logisland.hbase.io;


import com.hurence.logisland.hbase.scan.ResultCell;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TestJsonRowSerializer {

    private final byte[] rowKey = "row1".getBytes(StandardCharsets.UTF_8);
    private ResultCell[] cells;

    @Before
    public void setup() {
        final byte[] cell1Fam = "colFam1".getBytes(StandardCharsets.UTF_8);
        final byte[] cell1Qual = "colQual1".getBytes(StandardCharsets.UTF_8);
        final byte[] cell1Val = "val1".getBytes(StandardCharsets.UTF_8);

        final byte[] cell2Fam = "colFam2".getBytes(StandardCharsets.UTF_8);
        final byte[] cell2Qual = "colQual2".getBytes(StandardCharsets.UTF_8);
        final byte[] cell2Val = "val2".getBytes(StandardCharsets.UTF_8);

        final ResultCell cell1 = getResultCell(cell1Fam, cell1Qual, cell1Val);
        final ResultCell cell2 = getResultCell(cell2Fam, cell2Qual, cell2Val);

        cells = new ResultCell[] { cell1, cell2 };
    }

    @Test
    public void testSerializeRegular() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final RowSerializer rowSerializer = new JsonRowSerializer(StandardCharsets.UTF_8);
        rowSerializer.serialize(rowKey, cells, out);

        final String json = out.toString(StandardCharsets.UTF_8.name());
        Assert.assertEquals("{\"row\":\"row1\", \"cells\": {\"colFam1:colQual1\":\"val1\", \"colFam2:colQual2\":\"val2\"}}", json);
    }

    private ResultCell getResultCell(byte[] fam, byte[] qual, byte[] val) {
        final ResultCell cell = new ResultCell();

        cell.setFamilyArray(fam);
        cell.setFamilyOffset(0);
        cell.setFamilyLength((byte)fam.length);

        cell.setQualifierArray(qual);
        cell.setQualifierOffset(0);
        cell.setQualifierLength(qual.length);

        cell.setValueArray(val);
        cell.setValueOffset(0);
        cell.setValueLength(val.length);

        return cell;
    }

}
