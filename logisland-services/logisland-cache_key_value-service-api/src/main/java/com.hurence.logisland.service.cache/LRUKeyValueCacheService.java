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
package com.hurence.logisland.service.cache;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.service.cache.model.Cache;
import com.hurence.logisland.service.cache.model.LRUCache;

import java.io.IOException;
import java.util.*;


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
 * This cache is not threadsafe ! We do not care about missing one update for a key.
 *
 * @see LinkedHashMap
 */
@Tags({"cache", "service", "key", "value", "pair", "LRU"})
@CapabilityDescription("A controller service for caching data by key value pair with LRU (last recently used) strategy. using LinkedHashMap")
public class LRUKeyValueCacheService<K,V>  extends AbstractControllerService implements CacheService<K,V> {

    private volatile Cache<K,V> cache;

    @Override
    public V get(K k) {
        return cache.get(k);
    }

    @Override
    public void set(K k, V v) {
        cache.set(k, v);
    }

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException {
        try {
            this.cache = createCache(context);
        }catch (Exception e){
            throw new InitializationException(e);
        }
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(CACHE_SIZE);
        return Collections.unmodifiableList(props);
    }

    protected Cache<K,V> createCache(final ControllerServiceInitializationContext context) throws IOException, InterruptedException {
        final int capacity = context.getPropertyValue(CACHE_SIZE).asInteger();
        return new LRUCache<K,V>(capacity);
    }
}
