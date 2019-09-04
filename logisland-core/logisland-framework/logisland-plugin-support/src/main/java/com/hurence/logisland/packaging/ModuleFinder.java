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
package com.hurence.logisland.packaging;

import com.hurence.logisland.classloading.PluginClassLoader;
import com.hurence.logisland.classloading.PluginClassloaderBuilder;
import org.springframework.boot.loader.archive.JarFileArchive;

import java.io.File;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

public class ModuleFinder {

    /**
     *
     * @param jarFile jar to look for classes
     * @param locationPrefix location in jar to look for class
     * @param parentClasses
     * @return
     * @throws Exception
     */
    public static List<String> findOfType(URL jarFile, String locationPrefix, Set<String> parentClasses) throws Exception {
        try (final JarFile file = new JarFile(jarFile.getFile())) {
            try (final PluginClassLoader classLoader = PluginClassloaderBuilder.build(new JarFileArchive(new File(jarFile.toURI())))) {
                Set<Class<?>> parents = new HashSet<>();
                for (String s : parentClasses) {
                    parents.add(classLoader.loadClass(s));
                }
                return file.stream().map(JarEntry::getName)
                        .filter(s -> s.startsWith(locationPrefix) && s.endsWith(".class") && !s.contains("$"))
                        .map(s -> s.substring(locationPrefix.length(), s.length() - ".class".length()).replace("/", "."))
                        .map(s -> {
                            try {
                                return Class.forName(s, false, classLoader);
                            } catch (Exception e) {
                                throw new RuntimeException("Unable to resolve classes '" + s + "'", e);
                            }
                        })
                        .filter(clazz -> !Modifier.isAbstract(clazz.getModifiers()))
                        .filter(clazz -> parents.stream().anyMatch(p -> p.isAssignableFrom(clazz)))
                        .map(Class::getCanonicalName)
                        .collect(Collectors.toList());

            }
        }
    }
}
