package com.hurence.logisland.webanalytics.util;

import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SparkMethods implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(SparkMethods.class);

    public static Iterator<Row> mappartitionRowIntoKeyValueRow(Iterator<Row> rows) {
        List<Row> output = new ArrayList<Row>();
        while (rows.hasNext()) {
            Row row = rows.next();
            logger.info("processing row {}", row);
            output.add(row);
        }
        return output.iterator();
    }

    public static String groupBySession(Row row) {
        return row.getAs("key");
    }

    public static Iterator<Row> doForEAchSessionGroup(String session, Iterator<Row> rows) {
        logger.info("processing group with key {}", session);
        return mappartitionRowIntoKeyValueRow(rows);
    }

}
