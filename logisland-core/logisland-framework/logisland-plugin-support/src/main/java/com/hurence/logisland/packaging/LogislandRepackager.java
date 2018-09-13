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

package com.hurence.logisland.packaging;


import com.hurence.logisland.classloading.PluginClassLoader;
import com.hurence.logisland.component.ConfigurableComponent;
import org.apache.commons.io.IOUtils;
import org.springframework.boot.loader.LaunchedURLClassLoader;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

/**
 * An hazardous repackager.
 *
 * @author amarziali
 */
public class LogislandRepackager {


    public static void main(String args[]) throws Exception {
        JarFile file = new JarFile(args[0]);
        String destFilename = args[0] + "-tmp";

        //first build the manifest
        LaunchedURLClassLoader classLoader = new PluginClassLoader(new URL[]{new File(args[0]).toURI().toURL()}, Thread.currentThread().getContextClassLoader());
        Manifest mf = file.getManifest();
        String springBootClasses = mf.getMainAttributes().getValue("Spring-Boot-Classes");
        //first collect classes
        String foundPluginList = file.stream().map(JarEntry::getName)
                .filter(s -> s.startsWith(springBootClasses) && s.endsWith(".class") && !s.contains("$"))
                .map(s -> s.substring(springBootClasses.length(), s.length() - ".class".length()).replace("/", "."))
                .map(s -> {
                    try {
                        return classLoader.loadClass(s);
                    } catch (Exception e) {
                        throw new RuntimeException("Unable to resolve classes", e);
                    }
                })
                .filter(clazz -> !Modifier.isAbstract(clazz.getModifiers()) && ConfigurableComponent.class.isAssignableFrom(clazz))
                .map(Class::getCanonicalName)
                .collect(Collectors.joining(","));
        mf.getMainAttributes().putValue("Logisland-Module-Exports", foundPluginList);
        mf.getMainAttributes().putValue("Logisland-Module-Version", args[2]);

        classLoader.close();
        JarOutputStream jos = new JarOutputStream(new FileOutputStream(destFilename), mf);

        Enumeration<JarEntry> enumeration = file.entries();
        while (enumeration.hasMoreElements()) {
            JarEntry entry = enumeration.nextElement();
            if (entry.getName().equals("META-INF/MANIFEST.MF")) {
                continue;
            }
            if (entry.getName().startsWith(args[1])) {
                continue;
            }
            jos.putNextEntry(entry);
            jos.write(IOUtils.toByteArray(file.getInputStream(entry)));
            jos.closeEntry();
        }
        jos.close();
        file.close();
        new File(args[0]).delete();
        new File(destFilename).renameTo(new File(args[0]));

    }
}
