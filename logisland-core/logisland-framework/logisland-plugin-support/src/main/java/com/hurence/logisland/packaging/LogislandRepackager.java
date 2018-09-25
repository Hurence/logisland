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

import java.io.*;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;

/**
 * An hazardous repackager.
 *
 * @author amarziali
 */
public class LogislandRepackager {


    public static void execute(String jarFile,
                               String providedLibPath,
                               String version,
                               String artifact,
                               String name,
                               String description,
                               String[] classLoaderParentFirst,
                               String[] apiArtifacts,
                               String... parentClasses) throws Exception {
        JarInputStream inputStream = new JarInputStream(new FileInputStream(jarFile));
        String destFilename = jarFile + "-tmp";


        Manifest mf = inputStream.getManifest();
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
        if (classLoaderParentFirst != null && classLoaderParentFirst.length > 0) {
            mf.getMainAttributes().putValue(ManifestAttributes.CLASSLOADER_PARENT_FIRST.toString(),
                    Arrays.stream(classLoaderParentFirst)
                            .map(String::trim)
                            .collect(Collectors.joining(",")));

        }


        JarOutputStream jos = new JarOutputStream(new FileOutputStream(destFilename), mf);
        writeToJar(inputStream, jos, apiArtifacts, providedLibPath, false);
        jos.close();

        new File(jarFile).delete();
        new File(destFilename).renameTo(new File(jarFile));
    }

    private static byte[] copyStream(InputStream in, ZipEntry entry) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long size = entry.getSize();
        if (size > -1) {
            byte[] buffer = new byte[1024 * 4];
            int n = 0;
            long count = 0;
            while (-1 != (n = in.read(buffer)) && count < size) {
                baos.write(buffer, 0, n);
                count += n;
            }
        } else {
            while (true) {
                int b = in.read();
                if (b == -1) {
                    break;
                }
                baos.write(b);
            }
        }
        baos.close();
        return baos.toByteArray();
    }


    private static void writeToJar(JarInputStream jis, JarOutputStream jos, String[] apiArtifacts, String providedLibPath,
                                   boolean skipMetaInf) throws Exception {


        try (JarInputStream jarInputStream = jis) {
            ZipEntry jarEntry;
            while ((jarEntry = jarInputStream.getNextEntry()) != null) {
                final ZipEntry entry = jarEntry;

                if ((skipMetaInf && entry.getName().startsWith("META-INF/")) || entry.getName().equals("META-INF/MANIFEST.MF")) {
                    continue;
                } else if (apiArtifacts != null && Arrays.stream(apiArtifacts).anyMatch(s -> entry.getName().endsWith(s))) {

                    writeToJar(new JarInputStream(new ByteArrayInputStream(copyStream(jis, entry))),
                            jos, null, null, true);
                    continue;


                } else if (providedLibPath != null && entry.getName().startsWith(providedLibPath)) {
                    continue;
                } else {
                    jos.putNextEntry(entry);
                    jos.write(copyStream(jis, entry));
                    jos.closeEntry();
                }
            }
        }
    }


}
