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
 * @see LinkedHashMap
 */
public class LRULinkedHashMap<K, V> extends LinkedHashMap<K, V> {

    private static final float DEFAULT_LOAD_FACTOR = 0.75f;
    private static final int DEFAULT_INITIAL_CAPACITY = 1024; // aka 16
    private static final int DEFAULT_MAX_ELEMENTS = 1000;

    private int maxElement;

    /**
     * will cache 1000 element maximum then removing eldest used element for each now input to cache
     */
    public LRULinkedHashMap() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_MAX_ELEMENTS);
    }

    /**
     * Be warned that it is using the defaut capacity which is 1024
     * @See LinkedHashMap
     */
    public LRULinkedHashMap(int maxElement) {
        //capicity should be a power of 2 see Hashmap documentation
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, maxElement);
    }

    /**
     *
     * @param  initialCapacity the initial capacity
     * @param  maxElement      the maximum number of element that will be cached
     * @throws IllegalArgumentException if the initial capacity is negative
     *         or the load factor is nonpositive
     */
    public LRULinkedHashMap(int initialCapacity,
                            int maxElement) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, maxElement);
    }

    /**
     *
     * @param  initialCapacity the initial capacity
     * @param  loadFactor      the load factor
     * @param  maxElement      the maximum number of element that will be cached
     * @throws IllegalArgumentException if the initial capacity is negative
     *         or the load factor is nonpositive
     */
    public LRULinkedHashMap(int initialCapacity,
                            float loadFactor,
                            int maxElement) {
        super(initialCapacity, loadFactor, true);
        this.maxElement = maxElement;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
        return size() > maxElement;
    }


}
