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

package com.hurence.logisland.classloading.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolves classes by selecting the right classloader.
 *
 * @author amarziali
 */
public class ClassLoaderAwareObjectInputStream extends ObjectInputStream {
    private static final Map<String, Class<?>> primitiveTypes =
            new HashMap<>();

    static {
        primitiveTypes.put("byte", byte.class);
        primitiveTypes.put("short", short.class);
        primitiveTypes.put("int", int.class);
        primitiveTypes.put("long", long.class);
        primitiveTypes.put("float", float.class);
        primitiveTypes.put("double", double.class);
        primitiveTypes.put("boolean", boolean.class);
        primitiveTypes.put("char", char.class);
        primitiveTypes.put("void", void.class);
    }

    private final ClassLoader scopedClassloader;

    /**
     * Constructor.
     *
     * @param in The <code>InputStream</code>.
     * @param cl the scoped classloader
     * @throws java.io.IOException if an I/O error occurs while reading stream header.
     * @see java.io.ObjectInputStream
     */
    public ClassLoaderAwareObjectInputStream(final InputStream in, ClassLoader cl) throws IOException {
        super(in);
        this.scopedClassloader = cl;
    }

    /**
     * Overriden version that uses the parametrized <code>ClassLoader</code> or the <code>ClassLoader</code>
     * of the current <code>Thread</code> to resolve the class.
     *
     * @param desc An instance of class <code>ObjectStreamClass</code>.
     * @return A <code>Class</code> object corresponding to <code>desc</code>.
     * @throws IOException            Any of the usual Input/Output exceptions.
     * @throws ClassNotFoundException If class of a serialized object cannot be found.
     */
    @Override
    protected Class<?> resolveClass(final ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        final String name = desc.getName();
        try {
            return Class.forName(name, false, scopedClassloader);
        } catch (final ClassNotFoundException ex) {
            final Class<?> cls = primitiveTypes.get(name);
            if (cls != null) {
                return cls;
            }
            throw ex;
        }
    }
}