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
package com.hurence.logisland.processor.webanalytics;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.service.cache.CacheService;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockCacheService<S, T> extends AbstractControllerService implements CacheService<S, T> {

    private Map<S, T> map = Collections.synchronizedMap(new HashMap<>());
    private int getMethodCallCount = 0;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.emptyList();
    }

    @Override
    public T get(S o) {
        getMethodCallCount++;
        return map.get(o);
    }

    @Override
    public void set(S o, T o2) {
        map.put(o, o2);
    }

    public int getGetMethodCallCount() {
        return this.getMethodCallCount;
    }
}