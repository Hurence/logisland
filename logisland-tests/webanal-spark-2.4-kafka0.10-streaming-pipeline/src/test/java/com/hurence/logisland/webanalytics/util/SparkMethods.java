package com.hurence.logisland.webanalytics.util;

import com.hurence.logisland.bean.KeyValue;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class SparkMethods implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(SparkMethods.class);

    public static Row mapRowIntoKeyValueRow(Row row) {
        KeyValue kv = new KeyValue(row.getString(0), row.getLong(1));
        logger.info("processing key {} with count {}", kv.key, kv.count);
        return row;
    }

}
