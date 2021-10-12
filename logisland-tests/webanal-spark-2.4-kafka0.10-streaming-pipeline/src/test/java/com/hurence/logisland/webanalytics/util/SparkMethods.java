/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
