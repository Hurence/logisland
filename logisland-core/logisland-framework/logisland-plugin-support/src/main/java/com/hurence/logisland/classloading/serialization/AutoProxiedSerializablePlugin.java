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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger logger = LoggerFactory.getLogger(PluginLoader.class);

    private final String originalClassName;
    private final byte[] content;

    public AutoProxiedSerializablePlugin(String originalClassName, byte[] content) {
        this.originalClassName = originalClassName;
        this.content = content;
    }

    Object readResolve() throws ObjectStreamException {
        try {
            return PluginProxy.create(new ClassLoaderAwareObjectInputStream(new ByteArrayInputStream(content),
                    PluginLoader.getRegistry().getOrDefault(originalClassName,
                            Thread.currentThread().getContextClassLoader())).readObject());
        } catch (Exception e) {

            if (e instanceof ClassNotFoundException)
            {
                logger.info("Could not create plugin proxy for " + originalClassName + ": forcing plugin registry load " +
                        "and retrying...");
                /**
                 * In databricks environment, on executors, the static code of PluginLoader is not called (as objects are
                 * sent through closures?) and the current thread context classloader is a repl.ExecutorClassLoader which
                 * is not a URLClassLoader and has not parent. Thus, in order to fill the plugin registry once for all
                 * (which is not yet filled and that is why we get a ClassNotFoundException here) for the executor VM,
                 * we force the scanAndRegisterPlugins call using the classloader of the current class, who will allow to
                 * find the logisland jars. Then we try to redo the plugin proxy creation that failed.
                 */
                PluginLoader.scanAndRegisterPlugins(AutoProxiedSerializablePlugin.class.getClassLoader());

                try {
                    return PluginProxy.create(new ClassLoaderAwareObjectInputStream(new ByteArrayInputStream(content),
                            PluginLoader.getRegistry().getOrDefault(originalClassName,
                                    Thread.currentThread().getContextClassLoader())).readObject());
                } catch (Exception ex) {
                    logger.error("Could create plugin proxy for " + originalClassName + " after having forced plugin " +
                            "registry loaded");
                }
            }
            throw new InvalidObjectException("Unable to resolve plugin proxy class: " + e.getMessage());
        }
    }
}