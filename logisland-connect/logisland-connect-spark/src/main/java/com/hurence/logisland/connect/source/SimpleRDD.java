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
package com.hurence.logisland.connect.source;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.InternalRow;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag$;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Simple kafka connect source partitioned RDD.
 *
 * @author amarziali
 */
public class SimpleRDD extends RDD<InternalRow> {

    final Map<Integer, List<InternalRow>> data;

    public SimpleRDD(SparkContext _sc, Map<Integer, List<InternalRow>> data) {
        super(_sc, JavaConversions.collectionAsScalaIterable(Collections.<Dependency<?>>emptyList()).toSeq(),
                ClassTag$.MODULE$.apply(InternalRow.class));
        this.data = data;
    }

    @Override
    public Iterator<InternalRow> compute(Partition split, TaskContext context) {
        return JavaConversions.collectionAsScalaIterable(data.get(((SimplePartition)split).getHash())).iterator();
    }

    @Override
    public Partition[] getPartitions() {
        Partition[] ret = new SimplePartition[data.size()];
        int j = 0;
        for (Integer i : data.keySet()) {
            ret[j] = new SimplePartition(j, i);
            j++;
        }
        return ret;

    }

}
