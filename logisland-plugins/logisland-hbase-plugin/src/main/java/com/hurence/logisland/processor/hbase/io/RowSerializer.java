
package com.hurence.logisland.processor.hbase.io;



import com.hurence.logisland.processor.hbase.scan.ResultCell;

import java.io.IOException;
import java.io.OutputStream;

public interface RowSerializer {

    /**
     * Serializes the given row and cells to the provided OutputStream
     *
     * @param rowKey the row's key
     * @param cells the cells to serialize
     * @param out the OutputStream to serialize to
     * @throws IOException if unable to serialize the row
     */
    void serialize(byte[] rowKey, ResultCell[] cells, OutputStream out) throws IOException;

    /**
     *
     * @param rowKey the row key of the row being serialized
     * @param cells the cells of the row being serialized
     * @return the serialized string representing the row
     */
    String serialize(byte[] rowKey, ResultCell[] cells);

}
