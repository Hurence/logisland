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
package com.hurence.logisland.service.cache;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.validator.StandardValidators;

import java.util.List;

/**
 * Created by gregoire on 19/05/17.
 */

@Tags({"cache", "service", "key", "value", "pair"})
@CapabilityDescription("A controller service for caching data")
public interface CacheService<K,V,S> extends ControllerService {

    PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache.size")
            .description("The maximum number of element in the cache.")
            .required(false)
            .defaultValue("16384")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    /**
     * Get an element from the cache
     * @param k
     * @return
     */
    public V get(K k);

    /**
     * Save an element into the cache
     * @param k
     * @param v
     */
    public void set(K k, V v);


    /**
     * Save an element into the indexed cache
     * @param k
     * @param s
     * @param v
     */
    public void set(K k, S s, V v);


    /**
     * Get list of elements from the cache.
     *
     * @param k : key
     * @param min : min value of the score
     * @param max : max value of the score
     * @param limit : limit of values to return
     * @return
     */
    public List<V> get(K k, S min, S max, S limit);
}
