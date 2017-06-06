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
package com.hurence.logisland.documentation.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ClassFinder {

    private static Logger logger = LoggerFactory.getLogger(ClassFinder.class);

    /**
     * find classes in the classpath
     * @param visitor
     */
    public static void findClasses(Visitor<String> visitor) {
        String classpath = System.getProperty("java.class.path");

        String[] paths = classpath.split(System.getProperty("path.separator"));

     /*   String javaHome = System.getProperty("java.home");
        File file = new File(javaHome + File.separator + "lib");
        if (file.exists()) {
            findClasses(file, file, true, visitor);
        }*/

        for (String path : paths) {
            File file = new File(path);
            if (file.exists() && file.getName().contains("logisland")) {
                findClasses(file, file, true, visitor);

            }
        }
    }

    private static boolean findClasses(File root, File file, boolean includeJars, Visitor<String> visitor) {
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                if (!findClasses(root, child, includeJars, visitor)) {
                    return false;
                }
            }
        } else {
            if (file.getName().toLowerCase().endsWith(".jar") && includeJars) {

                JarFile jar = null;
                try {
                    jar = new JarFile(file);
                } catch (Exception ex) {

                }
                if (jar != null) {
                    Enumeration<JarEntry> entries = jar.entries();
                    while (entries.hasMoreElements()) {
                        JarEntry entry = entries.nextElement();
                        String name = entry.getName();
                        int extIndex = name.lastIndexOf(".class");
                        if (extIndex > 0) {
                            if (!visitor.visit(name.substring(0, extIndex).replace("/", "."))) {
                                return false;
                            }
                        }
                    }
                }
            }
            else if (file.getName().toLowerCase().endsWith(".class")) {
                if (!visitor.visit(createClassName(root, file))) {
                    return false;
                }
            }
        }

        return true;
    }

    private static String createClassName(File root, File file) {
        StringBuffer sb = new StringBuffer();
        String fileName = file.getName();
        sb.append(fileName.substring(0, fileName.lastIndexOf(".class")));
        file = file.getParentFile();
        while (file != null && !file.equals(root)) {
            sb.insert(0, '.').insert(0, file.getName());
            file = file.getParentFile();
        }
        return sb.toString();
    }
}