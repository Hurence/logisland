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
package com.hurence.logisland.service.cache.model;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by gregoire on 18/05/17.
 */
public class LRULinkedHashMapTest {

    private static Logger logger = LoggerFactory.getLogger(LRULinkedHashMapTest.class);

    @Test
    public void testAutomaticDeductionOfCapacity() {
        LRULinkedHashMap<String, String> map = new LRULinkedHashMap<>(16, 0.75f, 3);
        map.put("1", "1");
        map.put("2", "2");
        map.put("3", "3");
        Assert.assertEquals(3, map.size());
        Assert.assertEquals(Arrays.asList("1", "2", "3"), new ArrayList<>(map.values()));
        map.put("4", "4");
        Assert.assertEquals(3, map.size());
        Assert.assertEquals(Arrays.asList("2", "3", "4"), new ArrayList<>(map.values()));
        map.put("5", "5");
        map.put("6", "6");
        map.put("7", "7");
        Assert.assertEquals(Arrays.asList("5", "6", "7"), new ArrayList<>(map.values()));
        map.get("5");
        Assert.assertEquals(Arrays.asList("6", "7", "5"), new ArrayList<>(map.values()));
        map.get("6");
        Assert.assertEquals(Arrays.asList("7", "5", "6"), new ArrayList<>(map.values()));
        map.put("4", "4");
        Assert.assertEquals(Arrays.asList("5", "6", "4"), new ArrayList<>(map.values()));

    }
}
