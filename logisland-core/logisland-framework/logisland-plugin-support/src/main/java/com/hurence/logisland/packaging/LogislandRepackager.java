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


import com.hurence.logisland.classloading.ManifestAttributes;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.LinkedHashSet;
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
        execute(args[0], args[2], args[3], args[4], args[5], args[6], args[1].split(","));

    }

    public static void execute(String jarFile,
                               String providedLibPath,
                               String version,
                               String artifact,
                               String name,
                               String description,
                               String... parentClasses) throws Exception {
        JarFile file = new JarFile(jarFile);
        String destFilename = jarFile + "-tmp";


        Manifest mf = file.getManifest();
        String springBootClasses = mf.getMainAttributes().getValue("Spring-Boot-Classes");
        //first collect classes
        String foundPluginList = ModuleFinder.findOfType(new File(jarFile).toURI().toURL(),
                springBootClasses,
                new LinkedHashSet<>(Arrays.asList(parentClasses)))
                .stream().collect(Collectors.joining(","));
        mf.getMainAttributes().putValue(ManifestAttributes.MODULE_EXPORTS.toString(), foundPluginList);
        mf.getMainAttributes().putValue(ManifestAttributes.MODULE_VERSION.toString(), version);
        mf.getMainAttributes().putValue(ManifestAttributes.MODULE_ARTIFACT.toString(), artifact);
        mf.getMainAttributes().putValue(ManifestAttributes.MODULE_NAME.toString(), name);
        mf.getMainAttributes().putValue(ManifestAttributes.MODULE_DESCRIPTION.toString(), description);


        JarOutputStream jos = new JarOutputStream(new FileOutputStream(destFilename), mf);

        Enumeration<JarEntry> enumeration = file.entries();
        while (enumeration.hasMoreElements()) {
            JarEntry entry = enumeration.nextElement();
            if (entry.getName().equals("META-INF/MANIFEST.MF")) {
                continue;
            }
            if (entry.getName().startsWith(providedLibPath)) {
                continue;
            }
            jos.putNextEntry(entry);
            jos.write(IOUtils.toByteArray(file.getInputStream(entry)));
            jos.closeEntry();
        }
        jos.close();
        file.close();
        new File(jarFile).delete();
        new File(destFilename).renameTo(new File(jarFile));


    }

}
