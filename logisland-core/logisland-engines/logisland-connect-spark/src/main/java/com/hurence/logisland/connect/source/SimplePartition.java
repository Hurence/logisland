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

import org.apache.spark.Partition;

/**
 * Simple partition.
 *
 * @author amarziali
 */
public class SimplePartition implements Partition {

    private final int index;
    private final int hash;

    public SimplePartition(int index, int hash) {
        this.index = index;
        this.hash = hash;
    }

    @Override
    public int index() {
        return index;
    }

    public int getHash() {
        return hash;
    }

    @Override
    public String toString() {
        return "SimplePartition{" +
                "index=" + index +
                ", hash=" + hash +
                '}';
    }
}
