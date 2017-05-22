package com.hurence.logisland.service.hbase.scan;

/**
 * Handles a single row from an HBase scan.
 */
public interface ResultHandler {

    void handle(byte[] row, ResultCell[] resultCells);

}
