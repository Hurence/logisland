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
package com.hurence.logisland.classloading;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.loader.archive.Archive;
import org.springframework.boot.loader.archive.JarFileArchive;
import org.springframework.boot.loader.jar.JarFile;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.*;
import java.util.jar.Manifest;

/**
 * A plugin loader scanning the classpath.
 * A plugin is assumed to:
 * <p>
 * <li>
 * <ul>Have a 'Logisland-Module-Exports' manifest entry containing a comma separated classes the archive is exporting.</ul>
 * <ul>Be packaged like a spring boot jar archive (with BOOT-INF/classes and BOOT-INF/lib). See https://docs.spring.io/spring-boot/docs/current/maven-plugin/repackage-mojo.html for more information.</ul>
 * </li>
 *
 * @author amarziali
 */
public class PluginLoader {

    private static final Logger logger = LoggerFactory.getLogger(PluginLoader.class);

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
                    String exportedPlugins = manifest.getMainAttributes().getValue(ManifestAttributes.MODULE_EXPORTS);
                    if (exportedPlugins != null) {
                        String version = StringUtils.defaultIfEmpty(manifest.getMainAttributes().getValue(ManifestAttributes.MODULE_VERSION), "UNKNOWN");

                        logger.info("Loading components from module {}", archive.getUrl().toExternalForm());


                        final Archive arc = archive;

                        if (StringUtils.isNotBlank(exportedPlugins)) {
                            Arrays.stream(exportedPlugins.split(",")).map(String::trim).forEach(s -> {
                                if (registry.putIfAbsent(s, PluginClassloaderBuilder.build(arc)) == null) {
                                    logger.info("Registered component '{}' version '{}'", s, version);
                                }

                            });
                        }
                    }
                }

            } catch (Exception e) {
                logger.error("Unable to load components from " + url.toExternalForm(), e);
            }
        }
    }


    /**
     * Load a plugin by autoproxying between current caller classloader and plugin own classloader.
     *
     * @param className the name of plugin class to load
     * @param <U>       the return type.
     * @return an instance of the requested plugin
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static <U> U loadPlugin(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        ClassLoader cl = registry.get(className);
        if (cl == null) {
            throw new ClassNotFoundException("Unable to find component with class " + className +
                    ". Please check your classpath");
        }
        ClassLoader thiz = Thread.currentThread().getContextClassLoader();
        Object o;
        try {
            Class<?> cls = cl.loadClass(className);
            Thread.currentThread().setContextClassLoader(cl);
            o = cls.newInstance();
        } finally {
            Thread.currentThread().setContextClassLoader(thiz);
        }
        return (U) PluginProxy.create(o);
    }


    public static Map<String, ClassLoader> getRegistry() {
        return Collections.unmodifiableMap(registry);
    }

}
