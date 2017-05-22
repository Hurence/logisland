/**
 * Copyright (C) 2017 Hurence
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.service.cache.model;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;

import java.util.LinkedHashMap;


/**
 * Created by gregoire on 18/05/17.
 *
 * <p>This an implementation of an LRU cache (Last Recent Used) using LinkedHashmap
 * It will cache every item automatically with put method. You just have to use get method
 * to retrieve cached object.</p>
 *
 * <p>You specify maximum number of element to cache in the map by specifying maxElement parameter.
 * When using put on the map when the size is >= maxElement then last recently used entry is deleted automatically</p>
 *
 * @see LinkedHashMap
 */
@Tags({"cache", "service", "key", "value", "pair", "LRU"})
@CapabilityDescription("A controller service for caching data by key value pair with LRU (last recently used) strategy. using LinkedHashMap")
public class LRUCache<K, V> implements Cache<K,V> {

    private static final int DEFAULT_CAPACITY = 1024;
    private final LRULinkedHashMap<K, V> map;

    public LRUCache() {
        this(DEFAULT_CAPACITY);
    }

    public LRUCache(int capacityCache) {
        int capacityLinkedHashMap = getClosestPowerOf2Lt(capacityCache);
        float loadFactorLinkedHashMap = 0.3f;
        map = new LRULinkedHashMap<K, V>(capacityLinkedHashMap, loadFactorLinkedHashMap, capacityCache);
    }

    /**
     * @param number
     * @return 0 if number <= 0
     * number if number is 1 or 2
     * else closest power of 2 from number that is inferior than number
     */
    public int getClosestPowerOf2Lt(int number) {
        if (number <= 0) return 0;
        if (number == 1 || number == 2) return number;
        int puissance = 32 - Integer.numberOfLeadingZeros(number - 1);
        return (int) Math.pow(2, puissance - 1);
    }

    @Override
    public V get(K k) {
        return map.get(k);
    }

    @Override
    public void set(K k, V v) {
        map.put(k, v);
    }
}
