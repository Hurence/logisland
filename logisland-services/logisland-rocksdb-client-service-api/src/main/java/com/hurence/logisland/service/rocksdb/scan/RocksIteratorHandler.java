package com.hurence.logisland.service.rocksdb.scan;

import org.rocksdb.RocksIterator;

/**
 * Created by gregoire on 09/06/17.
 */
public interface RocksIteratorHandler {

    void handle(RocksIterator rocksIterator);
}
