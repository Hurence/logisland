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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.loader.archive.Archive;
import org.springframework.boot.loader.archive.JarFileArchive;
import org.springframework.boot.loader.jar.JarFile;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.*;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * A plugin loader scanning the classpath.
 * A plugin is assumed to:
 * <p>
 * <li>
 * <ul>Have a 'Logisland-PLugins-Export' manifest entry containing a comma separated classes the archive is exporting.</ul>
 * <ul>Be packaged like a spring boot jar archive (with BOOT-INF/classes and BOOT-INF/lib). See https://docs.spring.io/spring-boot/docs/current/maven-plugin/repackage-mojo.html for more information.</ul>
 * </li>
 *
 * @author amarziali
 */
public class PluginLoader {

    private static final Logger logger = LoggerFactory.getLogger(PluginLoader.class);

    private static final Attributes.Name MANIFEST_ATTRIBUTE_PLUGINS_EXPORT = new Attributes.Name("Logisland-Plugins-Export");
    private static final Attributes.Name MANIFEST_ATTRIBUTE_LOGISLAND_VERSION = new Attributes.Name("Logisland-Version");
    private static final Map<String, ClassLoader> registry = Collections.synchronizedMap(new HashMap<>());

    static {
        JarFile.registerUrlProtocolHandler();
        scanAndRegisterPlugins();
    }

    /**
     * Scan for plugins.
     */
    private static void scanAndRegisterPlugins() {
        Set<URL> urls = new HashSet<>();
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        while (cl != null) {
            if (cl instanceof URLClassLoader) {
                urls.addAll(Arrays.asList(((URLClassLoader) cl).getURLs()));
                cl = cl.getParent();
            }
        }


        for (URL url : urls) {
            try {
                Archive archive = null;
                try {
                    archive = new JarFileArchive(new File(URLDecoder.decode(url.getFile(), Charset.defaultCharset().name())), url);
                } catch (Exception e) {
                    //silently swallowing exception. just skip the archive since not an archive
                }
                if (archive == null) {
                    continue;
                }
                Manifest manifest = archive.getManifest();
                if (manifest != null) {
                    String exportedPlugins = manifest.getMainAttributes().getValue(MANIFEST_ATTRIBUTE_PLUGINS_EXPORT);
                    if (exportedPlugins != null) {
                        String version = StringUtils.defaultIfEmpty(manifest.getMainAttributes().getValue(MANIFEST_ATTRIBUTE_LOGISLAND_VERSION), "UNKNOWN");

                        logger.info("Loading plugins from jar {}", archive.getUrl().toExternalForm());
                        List<URL> urlList = new ArrayList<>();


                        for (Archive a : archive.getNestedArchives(PluginLoader::isNestedArchive)) {
                            urlList.add(a.getUrl());
                        }


                        Arrays.stream(exportedPlugins.split(",")).map(String::trim).forEach(s -> {
                            if (registry.putIfAbsent(s,
                                    new PluginClassLoader(urlList.toArray(new URL[urlList.size()]), Thread.currentThread().getContextClassLoader())) == null) {
                                logger.info("Registered plugin '{}' version '{}'", s, version);
                            }
                        });
                    }
                }

            } catch (Exception e) {
                logger.error("Unable to load plugin from " + url.toExternalForm(), e);
            }
        }
    }


    private static boolean isNestedArchive(Archive.Entry entry) {
        return (entry.isDirectory() && entry.getName().equals("BOOT-INF/classes/")) || entry.getName().startsWith("BOOT-INF/lib/");
    }


    /**
     * Load a plugin by autoproxying between current caller classloader and plugin own classloader.
     *
     * @param className the name of plugin class to load
     * @param <U>       the return type.
     * @return an instance of the requested plugin
     * @throws Exception
     */
    public static <U> U loadPlugin(String className) throws Exception {
        ClassLoader cl = registry.get(className);
        if (cl == null) {
            throw new ClassNotFoundException("Unable to find plugin class " + className +
                    ". Please check your classpath");
        }
        return (U) PluginProxy.create(cl.loadClass(className).newInstance());
    }


    public static Map<String, ClassLoader> getRegistry() {
        return Collections.unmodifiableMap(registry);
    }

}
