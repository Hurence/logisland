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
package com.hurence.logisland.classloading.serialization;

import com.hurence.logisland.classloading.PluginLoader;
import com.hurence.logisland.classloading.PluginProxy;

import java.io.ByteArrayInputStream;
import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * A serialized version of a CGLIB autoproxied plugin
 *
 * @author amarziali
 */
public class AutoProxiedSerializablePlugin implements Serializable {
    private final String originalClassName;
    private final byte[] content;

    public AutoProxiedSerializablePlugin(String originalClassName, byte[] content) {
        this.originalClassName = originalClassName;
        this.content = content;
    }

    Object readResolve() throws ObjectStreamException {
        try {
            return PluginProxy.create(new ClassLoaderAwareObjectInputStream(new ByteArrayInputStream(content),
                    PluginLoader.getRegistry().getOrDefault(originalClassName, Thread.currentThread().getContextClassLoader())).readObject());
        } catch (Exception e) {
            throw new InvalidObjectException("Unable to resolve plugin proxy class: " + e.getMessage());
        }
    }
}