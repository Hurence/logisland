/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.classloading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.loader.LaunchedURLClassLoader;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A plugin classloader doing parent last resolution.
 *
 * @author amarziali
 */
public class PluginClassLoader extends LaunchedURLClassLoader {
    private static final Logger log = LoggerFactory.getLogger(PluginClassLoader.class);

    static {
        ClassLoader.registerAsParallelCapable();
    }

    private final Set<Pattern> classFilterPatterns = new HashSet<>();

    /**
     * Constructor that accepts a specific classloader as parent.
     *
     * @param urls                the list of urls from which to load classes and resources for this plugin.
     * @param parentFirstPatterns list of class pattern that will be resolved first by the parent.
     * @param parent              the parent classloader to be used for delegation for classes that were
     *                            not found or should not be loaded in isolation by this classloader.
     */
    public PluginClassLoader(URL[] urls, String[] parentFirstPatterns, ClassLoader parent) {
        super(urls, parent);
        //add defaults
        classFilterPatterns.add(Pattern.compile("java.*"));
        classFilterPatterns.add(Pattern.compile("scala.*"));
        for (String parentFirstPattern : parentFirstPatterns) {
            classFilterPatterns.add(Pattern.compile(parentFirstPattern));
        }
    }


    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> klass = null;
            if (classFilterPatterns.stream().noneMatch(p->p.matcher(name).matches())) {
                klass = findLoadedClass(name);

                if (klass == null) {
                    try {
                        klass = findClass(name);

                    } catch (ClassNotFoundException e) {
                        // Not found in loader's path. Search in parents.
                        log.trace("Class '{}' not found. Delegating to parent", name);
                    }
                }
            }
            if (klass == null) {
                klass = super.loadClass(name, false);
            }
            if (resolve) {
                resolveClass(klass);
            }
            return klass;
        }
    }
}