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

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;


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
 *
 * This cache is not threadsafe ! We do not care about missing one update for a key.
 *
 * @see LinkedHashMap
 */
@Tags({"cache", "service", "key", "value", "pair", "LRU"})
@CapabilityDescription("A controller service for caching data by key value pair with LRU (last recently used) strategy. using LinkedHashMap")
public class LRUCache<K, V> implements Cache<K,V> {

    private static final int DEFAULT_CAPACITY = 1024;
    private final Map<K, V> synchronized_map;

    public LRUCache() {
        this(DEFAULT_CAPACITY);
    }

    public LRUCache(int capacityCache) {
        this(capacityCache, 0.3f);
    }

    public LRUCache(int capacityCache, float loadFactor) {
        int capacityLinkedHashMap = getClosestPowerOf2Lt(capacityCache);
        LRULinkedHashMap<K, V> map = new LRULinkedHashMap<K, V>(capacityLinkedHashMap, loadFactor, capacityCache);
        synchronized_map = Collections.synchronizedMap(map);
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
        return synchronized_map.get(k);
    }

    @Override
    public K getString(K k) {
         return (K) synchronized_map.get(k);
    }

    @Override
    public void set(K k, V v) {
        synchronized_map.put(k, v);
    }

    @Override
    public void setString(K k, K v) {
    }
}
