package com.hurence.logisland.processor.hbase;

import com.hurence.logisland.service.hbase.put.PutColumn;
import com.hurence.logisland.service.hbase.put.PutRecord;

import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;



public class HBaseTestUtil {

    public static void verifyPut(final String row, final String columnFamily, final Map<String,byte[]> columns, final List<PutRecord> puts) {
        boolean foundPut = false;

        for (final PutRecord put : puts) {
            if (!row.equals(new String(put.getRow(), StandardCharsets.UTF_8))) {
                continue;
            }

            if (put.getColumns() == null || put.getColumns().size() != columns.size()) {
                continue;
            }

            // start off assuming we have all the columns
            boolean foundAllColumns = true;

            for (Map.Entry<String, byte[]> entry : columns.entrySet()) {
                // determine if we have the current expected column
                boolean foundColumn = false;
                for (PutColumn putColumn : put.getColumns()) {
                    if (columnFamily.equals(new String(putColumn.getColumnFamily(), StandardCharsets.UTF_8))
                            && entry.getKey().equals(new String(putColumn.getColumnQualifier(), StandardCharsets.UTF_8))
                            && Arrays.equals(entry.getValue(), putColumn.getBuffer())) {
                        foundColumn = true;
                        break;
                    }
                }

                // if we didn't have the current expected column we know we don't have all expected columns
                if (!foundColumn) {
                    foundAllColumns = false;
                    break;
                }
            }

            // if we found all the expected columns this was a match so we can break
            if (foundAllColumns) {
                foundPut = true;
                break;
            }
        }

        assertTrue(foundPut);
    }


}
