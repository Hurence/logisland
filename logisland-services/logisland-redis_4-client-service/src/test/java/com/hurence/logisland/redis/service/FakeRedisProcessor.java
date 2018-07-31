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
package com.hurence.logisland.redis.service;


import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Fake processor used for testing RedisConnectionPoolService.
 */
public class FakeRedisProcessor extends AbstractProcessor {

    public static final PropertyDescriptor REDIS_SERVICE = new PropertyDescriptor.Builder()
            .name("redis-service")
            .displayName("Redis Service")
            .identifiesControllerService(RedisKeyValueCacheService.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(REDIS_SERVICE);
    }


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        return null;
    }
}
