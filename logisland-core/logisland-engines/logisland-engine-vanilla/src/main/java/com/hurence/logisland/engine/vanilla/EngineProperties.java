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
package com.hurence.logisland.engine.vanilla;

import com.hurence.logisland.component.PropertyDescriptor;

public interface EngineProperties {

    PropertyDescriptor JVM_HEAP_MEM_MIN = new PropertyDescriptor.Builder()
            .name("jvm.heap.min")
            .required(false)
            .description("Minimum memory the JVM should allocate for its heap")
            .build();

    PropertyDescriptor JVM_HEAP_MEM_MAX = new PropertyDescriptor.Builder()
            .name("jvm.heap.max")
            .required(false)
            .description("Maximum memory the JVM should allocate for its heap")
            .build();
}
