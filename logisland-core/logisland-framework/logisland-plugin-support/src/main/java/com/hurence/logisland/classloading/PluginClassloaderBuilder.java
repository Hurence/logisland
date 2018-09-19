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

import org.springframework.boot.loader.archive.Archive;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.Attributes;

public class PluginClassloaderBuilder {


    private static boolean isNestedArchive(Archive.Entry entry) {
        return (entry.isDirectory() && entry.getName().equals("BOOT-INF/classes/")) || entry.getName().startsWith("BOOT-INF/lib/");
    }

    public static PluginClassLoader build(Archive archive) {
        List<URL> urlList = new ArrayList<>();

        try {
            for (Archive a : archive.getNestedArchives(PluginClassloaderBuilder::isNestedArchive)) {
                urlList.add(a.getUrl());
            }
            Attributes attributes = archive.getManifest().getMainAttributes();
            String parentFirstPatterns = attributes.getValue(ManifestAttributes.CLASSLOADER_PARENT_FIRST);
            ModuleInfo moduleInfo = new ModuleInfo(attributes.getValue(ManifestAttributes.MODULE_NAME),
                    attributes.getValue(ManifestAttributes.MODULE_DESCRIPTION),
                    archive.getUrl().toString(),
                    attributes.getValue(ManifestAttributes.MODULE_ARTIFACT),
                    attributes.getValue(ManifestAttributes.MODULE_VERSION));
            return new PluginClassLoader(moduleInfo, urlList.toArray(new URL[urlList.size()]),
                    parentFirstPatterns != null ? parentFirstPatterns.split(",") : new String[0],
                    Thread.currentThread().getContextClassLoader());

        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }
}
